import dotenv from 'dotenv';
import mongoose from 'mongoose';
import { Logger } from './logger';
import { PollOutboxRunner, PushOutboxRunner } from './outbox-runner';
import { RabbitConnection } from './rabbit';
import { FileResumeTokenManager } from './resume-token-manager';

dotenv.config({});

const logger = new Logger('Publisher');

async function main() {
  const rabbit = new RabbitConnection({
    connect: process.env.RABBITMQ_URI as string,
  });

  await rabbit.assertExchange('ORDER.CREATED', 'fanout', { durable: true, autoDelete: false });
  await rabbit.assertRetryableQueue('order-counter', 5_000, { durable: true, autoDelete: false });
  await rabbit.bindQueue('order-counter', { topic: 'ORDER.CREATED', type: 'fanout' });

  const connection = await mongoose.createConnection(process.env.MONGODB_URI as string).asPromise();

  const { OrderSchema } = await import('./schemas/order');
  const { OutboxSchema } = await import('./schemas/outbox');

  const Order = connection.model('Order', OrderSchema);
  const Outbox = connection.model('Outbox', OutboxSchema);

  const runner = new PushOutboxRunner({
    resumeTokenManager: new FileResumeTokenManager('.temp/token.json'),
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
  });

  // const runner = new PollOutboxRunner({
  //   interval: 5 * 1000,
  //   Outbox: Outbox as mongoose.Model<any>,
  //   publish: doc => {
  //     return rabbit.publish({
  //       topic: doc.event,
  //       type: 'fanout',
  //       content: {
  //         event: doc.event,
  //         payload: doc.payload,
  //       },
  //       options: {
  //         persistent: true,
  //         messageId: `${doc._id}`,
  //         timestamp: doc.createdAt.getTime(),
  //       },
  //     });
  //   },
  // });

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
