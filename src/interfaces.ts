import { ConsumeMessage, Options } from 'amqplib';

export type ExchangeType = 'direct' | 'fanout' | 'topic' | 'headers';

export interface RabbitMessage<T = any> extends Omit<ConsumeMessage, 'content'> {
  /**
   * Number of times handle message (starts with 1)
   */
  attempts: number;
  content: T;
}

export interface RabbitOptions {
  connect?: string | Options.Connect;
  prefix?: {
    queue?: string;
    exchange?: string;
  };
  serialize?: (content: unknown) => Buffer;
  deserialize?: (buffer: Buffer) => any;
}

export interface SendMessage<T = any> {
  queue: string;
  content: T;
  options?: Options.Publish;
}

export interface PublishMessage<T = any> {
  topic: string;
  type: ExchangeType;
  content: T;
  routing?: string;
  options?: Options.Publish;
}

export interface SubscribeOptions extends Options.Consume {
  concurrency?: number;
}

export type DirectMessageSource = {
  topic: string;
  type: 'direct';
  routing: string;
};

export type FanoutMessageSource = {
  topic: string;
  type: 'fanout';
};

export type TopicMessageSource = {
  topic: string;
  type: 'topic';
  routing: string;
};

export type HeadersMessageSource = {
  topic: string;
  type: 'headers';
  match: 'any' | 'all' | 'any-with-x' | 'all-with-x';
  headers: Record<string, string>;
};

export type MessageSource = DirectMessageSource | FanoutMessageSource | TopicMessageSource | HeadersMessageSource;
