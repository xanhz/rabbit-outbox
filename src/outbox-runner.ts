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

export interface ResumeTokenManager {
  get(): Promise<mongoose.mongo.ResumeToken>;
  set(token: mongoose.mongo.ResumeToken): Promise<any>;
}

export interface OutboxRunnerConfig {
  Outbox: mongoose.Model<OutboxDocument>;
  publish: (doc: OutboxDocument) => Promise<any>;
  resumeTokenManager: ResumeTokenManager;
}

export class OutboxRunner {
  protected logger: Logger;
  protected Outbox: mongoose.Model<OutboxDocument>;
  protected publish: (doc: OutboxDocument) => Promise<any>;
  protected resumeTokenManager: ResumeTokenManager;
  protected queue: TaskQueue;
  protected running: boolean;

  constructor(config: OutboxRunnerConfig) {
    this.logger = new Logger(this.constructor.name);
    this.Outbox = config.Outbox;
    this.publish = config.publish;
    this.resumeTokenManager = config.resumeTokenManager;
    this.queue = new TaskQueue();
    this.running = false;
  }

  public start() {
    if (this.running) {
      return Promise.resolve(void 0);
    }
    this.running = true;
    return this.run();
  }

  private async run() {
    const resumeToken = await this.resumeTokenManager.get();
    const stream = this.Outbox.watch(
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

    stream.on('change', (change: mongoose.mongo.ChangeStreamInsertDocument) => {
      this.queue.push(async () => {
        try {
          this.logger.info('Processing outbox insert with token=%j', change._id);
          await this.publish(change.fullDocument as any);
          await this.Outbox.updateOne({ _id: change.documentKey._id }, { $set: { sent: true, sentAt: new Date() } });
          this.logger.info('Mark current token=%j', change._id);
          await this.resumeTokenManager.set(change._id);
        } catch (error) {
          this.logger.error(error);
          await TimerUtils.delay(1_000);
          this.queue.flush();
          this.run();
        }
      });
    });

    stream.once('error', err => {
      this.logger.error(err);
      stream
        .close()
        .then(() => TimerUtils.delay(1_000))
        .then(() => {
          this.queue.flush();
          this.run();
        })
        .catch(_.noop);
    });

    stream.once('end', () => {
      this.logger.warn('Change stream ended. Restarting...');
      stream
        .close()
        .then(() => TimerUtils.delay(1_000))
        .then(() => {
          this.queue.flush();
          this.run();
        })
        .catch(_.noop);
    });
  }
}
