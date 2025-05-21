import amqp, { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager';
import { ConfirmChannel, ConsumeMessage, Options, Replies, XDeath } from 'amqplib';
import _ from 'lodash';
import { ExchangeType, MessageSource, PublishMessage, RabbitOptions, SubscribeOptions } from './interfaces';
import { Logger } from './logger';
import { ErrorUtils } from './utils';

const $queue = (name: string, prefix?: string) => {
  if (prefix) {
    return `${prefix}.${name}`;
  }
  return name;
};

const $exchange = (topic: string, type: ExchangeType, prefix?: string) => {
  if (prefix) {
    return `${prefix}.${topic}.${type}`;
  }
  return `${topic}.${type}`;
};

const minify = (obj: any) => {
  return JSON.stringify(obj, undefined, 0);
};

export interface RabbitMessage<T = any> extends Omit<ConsumeMessage, 'content'> {
  /**
   * Number of times handle message (starts with 1)
   */
  attempts: number;
  content: T;
}

export class RabbitConnection {
  protected logger: Logger;
  protected connection: AmqpConnectionManager;
  protected channels: Map<number, ChannelWrapper>;
  protected asserted: {
    queues: Set<string>;
    exchanges: Set<string>;
  };
  protected options: Required<RabbitOptions>;
  protected _status: 'connecting' | 'reconnecting' | 'connected';

  constructor(options: RabbitOptions) {
    this.logger = new Logger(RabbitConnection.name);
    this.options = {
      connect: 'amqp://localhost:5672',
      prefix: {
        queue: undefined,
        exchange: undefined,
      },
      serialize: (content: any) => {
        if (typeof content === 'string') {
          return Buffer.from(content);
        }
        return Buffer.from(minify(content));
      },
      deserialize: (buffer: Buffer) => {
        const str = buffer.toString('utf8');
        return JSON.parse(str);
      },
      ...options,
    };
    this.channels = new Map();
    this.asserted = { exchanges: new Set(), queues: new Set() };
    this._status = 'connecting';
    this.connection = amqp.connect(options.connect);
    this.connection.on('connect', () => {
      if (this._status === 'reconnecting') {
        this.logger.info(`Rabbit is reconnected`);
      } else {
        this.logger.info(`Rabbit is connected`);
      }
      this._status = 'connected';
    });
    this.connection.on('disconnect', ({ err }) => {
      this._status = 'reconnecting';
      this.logger.info(`Rabbit is disconnected | Error=%j`, ErrorUtils.json(err, true));
    });
    this.connection.on('connectFailed', ({ err }) => {
      this._status = 'reconnecting';
      this.logger.info(`Rabbit connect failed | Error=%j`, ErrorUtils.json(err, true));
    });
  }

  public get status() {
    return this._status;
  }

  public close() {
    return this.connection.close();
  }

  protected async assertChannel(concurrency = 1) {
    let channel = this.channels.get(concurrency);
    if (!channel) {
      channel = this.connection.createChannel({
        setup: (channel: ConfirmChannel) => {
          return channel.prefetch(concurrency, false);
        },
      });
      this.channels.set(concurrency, channel);
    }
    await channel.waitForConnect();
    return channel;
  }

  public async publish(message: PublishMessage) {
    const { content, topic, type, routing = '', options = {} } = message;
    const exchange = $exchange(topic, type, this.options.prefix.exchange);
    this.logger.debug(
      `Publish to exchange=${exchange} | routing=${routing} | content=${minify(content)} | options=${minify(options)}`
    );
    const channel = await this.assertChannel();
    const buffer = this.options.serialize(content);
    return channel.publish(exchange, routing, buffer, options);
  }

  public async subscribe(
    source: string,
    callback: (channel: ChannelWrapper, message: RabbitMessage) => any,
    options: SubscribeOptions = {}
  ) {
    const channel = await this.assertChannel(options.concurrency);
    const queue = $queue(source, this.options.prefix.queue);
    this.logger.info(`Create consumer on queue=${queue}`);
    return channel.consume(
      queue,
      msg => {
        if (_.isNil(msg)) {
          return;
        }
        try {
          msg.content = this.options.deserialize(msg.content);
          const deaths = _.get(msg, 'properties.headers.x-death', [] as XDeath[]);
          const rejected = deaths.find(death => {
            return death.reason === 'rejected' && death.queue === queue;
          });
          const attempts = rejected ? rejected.count + 1 : 1;
          Object.defineProperty(msg, 'attempts', { value: attempts });
          callback(channel, msg as any);
        } catch {
          channel.nack(msg, false, true);
        }
      },
      options
    );
  }

  public async assertQueue(name: string, options?: Options.AssertQueue) {
    let reply = {
      queue: $queue(name, this.options.prefix.queue),
    } as Replies.AssertQueue;
    if (this.asserted.queues.has(reply.queue)) {
      return reply;
    }
    const channel = await this.assertChannel();
    this.logger.debug(`Assert queue=${reply.queue} | options=${minify(options)}`);
    reply = await channel.assertQueue(reply.queue, options);
    this.asserted.queues.add(reply.queue);
    return reply;
  }

  public async assertRetryableQueue(name: string, delay: number, options?: Options.AssertQueue) {
    let targetQueue = {
      queue: $queue(name, this.options.prefix.queue),
    } as Replies.AssertQueue;
    if (this.asserted.queues.has(targetQueue.queue)) {
      return targetQueue;
    }
    const retryExchange = await this.assertExchange('retry', 'direct', { durable: true, autoDelete: false });
    const delayQueue = await this.assertQueue(`${name}.delay`, {
      durable: true,
      autoDelete: false,
      messageTtl: delay,
      deadLetterExchange: retryExchange.exchange,
      deadLetterRoutingKey: targetQueue.queue,
    });
    targetQueue = await this.assertQueue(name, {
      ...options,
      deadLetterExchange: retryExchange.exchange,
      deadLetterRoutingKey: delayQueue.queue,
    });
    await this.bindQueue(name, { topic: 'retry', type: 'direct', routing: targetQueue.queue });
    await this.bindQueue(`${name}.delay`, { topic: 'retry', type: 'direct', routing: delayQueue.queue });
    return targetQueue;
  }

  public async assertExchange(topic: string, type: ExchangeType, options?: Options.AssertExchange) {
    let reply = {
      exchange: $exchange(topic, type, this.options.prefix.exchange),
    } as Replies.AssertExchange;
    if (this.asserted.exchanges.has(reply.exchange)) {
      return reply;
    }
    const channel = await this.assertChannel();
    this.logger.debug(`Assert exchange=${reply.exchange} | options=${minify(options)}`);
    reply = await channel.assertExchange(reply.exchange, type, options);
    this.asserted.exchanges.add(reply.exchange);
    return reply;
  }

  public async bindQueue(name: string, source: MessageSource) {
    const queue = $queue(name, this.options.prefix.queue);
    const exchange = $exchange(source.topic, source.type, this.options.prefix.exchange);
    this.logger.debug(`Bind queue=${queue} to source=${minify(source)}`);
    const channel = await this.assertChannel();
    if (source.type === 'direct') {
      return channel.bindQueue(queue, exchange, source.routing);
    }
    if (source.type === 'fanout') {
      return channel.bindQueue(queue, exchange, '');
    }
    if (source.type === 'headers') {
      return channel.bindQueue(queue, exchange, '', {
        'x-match': source.match,
        ...source.headers,
      });
    }
    if (source.type === 'topic') {
      return channel.bindQueue(queue, exchange, source.routing);
    }
    // @ts-ignore
    throw new Error(`Unknown message source topic '${source.topic}' and type '${source.type}'`);
  }
}
