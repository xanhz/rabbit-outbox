import _ from 'lodash';
import mongoose from 'mongoose';
import { Logger } from './logger';
import { TaskQueue } from './task-queue';
import { TimerUtils } from './utils';

export interface OutboxDocument<TPayload = any> {
  _id: mongoose.Types.ObjectId;
  event: string;
  payload: TPayload;
  sent: boolean;
  createdAt: Date;
  sentAt?: Date;
}

export interface OutboxRunnerConfig {
  Outbox: mongoose.Model<OutboxDocument>;
  publish: (doc: OutboxDocument) => Promise<any>;
}

export abstract class OutboxRunner {
  protected logger: Logger;
  protected Outbox: mongoose.Model<OutboxDocument>;
  protected publish: (doc: OutboxDocument) => Promise<any>;

  constructor(config: OutboxRunnerConfig) {
    this.logger = new Logger(this.constructor.name);
    this.Outbox = config.Outbox;
    this.publish = config.publish;
  }

  public abstract start(): Promise<void>;

  public abstract stop(): Promise<void>;
}

export interface ResumeTokenManager {
  get(): Promise<mongoose.mongo.ResumeToken>;
  set(token: mongoose.mongo.ResumeToken): Promise<any>;
}

export interface PushOutboxRunnerConfig extends OutboxRunnerConfig {
  resumeTokenManager: ResumeTokenManager;
}

export class PushOutboxRunner extends OutboxRunner {
  protected resumeTokenManager: ResumeTokenManager;
  protected queue: TaskQueue;
  protected stream!: mongoose.mongo.ChangeStream;

  constructor(config: PushOutboxRunnerConfig) {
    super(config);
    this.queue = new TaskQueue();
    this.resumeTokenManager = config.resumeTokenManager;
  }

  public override start() {
    if (this.stream) {
      return Promise.resolve(void 0);
    }
    return this.run();
  }

  private async run() {
    const resumeToken = await this.resumeTokenManager.get();
    this.stream = this.Outbox.watch(
      [
        {
          $match: {
            operationType: 'insert',
          },
        },
      ],
      {
        resumeAfter: resumeToken,
        readPreference: 'secondaryPreferred',
      }
    );

    this.stream.on('change', (change: mongoose.mongo.ChangeStreamInsertDocument) => {
      this.queue.push({
        execute: async () => {
          this.logger.info('Processing outbox insert with token=%j', change._id);
          await this.publish(change.fullDocument as any);
          await this.Outbox.updateOne({ _id: change.documentKey._id }, { $set: { sent: true, sentAt: new Date() } });
          this.logger.info('Mark current token=%j', change._id);
          await this.resumeTokenManager.set(change._id);
        },
        onerror: e => {
          this.logger.error(e);
          this.queue.clear();
          this.run();
        },
      });
    });

    this.stream.once('error', err => {
      this.logger.error(err);
      this.stream.close().catch(_.noop);
      this.queue.clear();
      TimerUtils.delay(1_000).then(() => this.run());
    });

    this.stream.once('end', () => {
      this.logger.warn('Change stream ended. Restarting...');
      this.stream.close().catch(_.noop);
      this.queue.clear();
      TimerUtils.delay(1_000).then(() => this.run());
    });
  }

  public override stop() {
    if (this.stream) {
      return this.stream.close();
    }
    return Promise.resolve(void 0);
  }
}

export interface PollOutboxRunnerConfig extends OutboxRunnerConfig {
  interval: number;
}

export class PollOutboxRunner extends OutboxRunner {
  protected timer: NodeJS.Timeout;
  protected running: boolean;

  constructor(config: PollOutboxRunnerConfig) {
    super(config);
    this.running = false;
    this.timer = setInterval(() => this.start(), config.interval);
  }

  public override async start() {
    if (this.running) {
      return Promise.resolve(void 0);
    }
    this.running = true;
    const unsents = await this.Outbox.find({ sent: false }).lean();
    for (const unsent of unsents) {
      try {
        await this.publish(unsent);
        await this.Outbox.updateOne({ _id: unsent._id }, { $set: { sent: true, sentAt: new Date() } });
      } catch (error) {
        this.logger.error(error);
        break;
      }
    }
    this.running = false;
  }

  public override stop() {
    clearInterval(this.timer);
    return Promise.resolve(void 0);
  }
}
