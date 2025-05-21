import dotenv from 'dotenv';
import { Redis } from 'ioredis';
import _ from 'lodash';
import mongoose from 'mongoose';
import { Logger } from './logger';
import { OutboxRunner } from './outbox-runner';
import { RabbitConnection } from './rabbit';

dotenv.config({});

const logger = new Logger('Publisher');

async function main() {
  const rabbit = new RabbitConnection({
    connect: process.env.RABBITMQ_URI as string,
  });
  const redis = new Redis(process.env.REDIS_URI as string);

  await rabbit.assertExchange('ORDER.CREATED', 'fanout', { durable: true, autoDelete: false });
  await rabbit.assertRetryableQueue('order-counter', 5_000, { durable: true, autoDelete: false });
  await rabbit.bindQueue('order-counter', { topic: 'ORDER.CREATED', type: 'fanout' });

  const connection = await mongoose.createConnection(process.env.MONGODB_URI as string).asPromise();

  const { OrderSchema } = await import('./schemas/order');
  const { OutboxSchema } = await import('./schemas/outbox');

  const Order = connection.model('Order', OrderSchema);
  const Outbox = connection.model('Outbox', OutboxSchema);

  const runner = new OutboxRunner({
    Outbox: Outbox as mongoose.Model<any>,
    publish: doc => {
      return rabbit.publish({
        topic: doc.event,
        type: 'fanout',
        content: {
          event: doc.event,
          payload: doc.payload,
        },
        options: {
          persistent: true,
          messageId: `${doc._id}`,
          timestamp: doc.createdAt.getTime(),
        },
      });
    },
    resumeTokenManager: {
      async get() {
        const token = await redis.get('change-streams:outbox:resume-token');
        if (_.isEmpty(token)) {
          return undefined;
        }
        return JSON.parse(token as any);
      },
      set(token) {
        return redis.set('change-streams:outbox:resume-token', JSON.stringify(token, undefined, 0));
      },
    },
  });
  runner.start();

  setInterval(() => {
    connection
      .transaction(async session => {
        const now = Date.now();
        const orders = [];
        for (let i = 0; i < 10; i++) {
          const order = new Order({
            code: `RX-${now + i}`,
          });
          orders.push(order);
        }
        await Order.create(orders, { session, ordered: true });
        const outboxes = orders.map(order => {
          return new Outbox({
            event: 'ORDER.CREATED',
            payload: order.toJSON(),
          });
        });
        await Outbox.create(outboxes, { session, ordered: true });
      })
      .catch(error => logger.error(error));
  }, 5000);
}

main();
