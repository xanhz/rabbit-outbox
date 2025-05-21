import { ChannelWrapper } from 'amqp-connection-manager';
import dotenv from 'dotenv';
import { RabbitMessage } from './interfaces';
import { Logger } from './logger';
import { RabbitConnection } from './rabbit';

dotenv.config({});

const logger = new Logger('Consumer');

async function main() {
  const rabbit = new RabbitConnection({
    connect: process.env.RABBITMQ_URI as string,
  });
  await rabbit.assertExchange('ORDER.CREATED', 'fanout', { durable: true, autoDelete: false });
  await rabbit.assertRetryableQueue('order-counter', 5_000, { durable: true, autoDelete: false });

  const orders = new Set<string>();

  const handler = (channel: ChannelWrapper, message: RabbitMessage) => {
    try {
      const metadata = {
        event: message.content.event,
        payload: message.content.payload,
        messageId: message.properties.messageId,
        timestamp: message.properties.timestamp,
      };
      logger.info('Received message=%j', metadata);
      orders.add(message.content.payload._id);
      logger.info('Order count: %d', orders.size);
    } catch (error) {
      logger.error(error);
      channel.nack(message, false, false);
    }
  };

  await rabbit.subscribe('order-counter', handler, {
    noAck: true,
    concurrency: 1,
  });
}

main();
