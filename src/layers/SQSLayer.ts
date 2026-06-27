import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { Context, Effect, Layer } from 'effect';
import { ConfigService } from '../config/index.js';
import { SQSDeleteError, SQSReceiveError } from '../errors/index.js';

export interface SQSMessage {
  body: string;
  receiptHandle: string;
  messageId: string;
}

export interface SQSServiceShape {
  receiveMessage(): Effect.Effect<SQSMessage | null, SQSReceiveError>;
  deleteMessage(receiptHandle: string): Effect.Effect<void, SQSDeleteError>;
  extendVisibility(
    receiptHandle: string,
    timeoutSec: number,
  ): Effect.Effect<void, SQSReceiveError>;
}

export class SQSService extends Context.Tag('SQSService')<
  SQSService,
  SQSServiceShape
>() {}

export const SQSServiceLive = Layer.effect(
  SQSService,
  Effect.gen(function* () {
    const config = yield* ConfigService;
    const client = new SQSClient({ region: config.AWS_REGION });

    return {
      receiveMessage: () =>
        Effect.tryPromise({
          try: () =>
            client.send(
              new ReceiveMessageCommand({
                QueueUrl: config.SQS_QUEUE_URL,
                MaxNumberOfMessages: 1,
                WaitTimeSeconds: 20,
              }),
            ),
          catch: (e) => new SQSReceiveError({ cause: e }),
        }).pipe(
          Effect.map((res) => {
            const m = res.Messages?.[0];
            if (!m?.Body || !m.ReceiptHandle || !m.MessageId) return null;
            return {
              body: m.Body,
              receiptHandle: m.ReceiptHandle,
              messageId: m.MessageId,
            };
          }),
        ),

      deleteMessage: (receiptHandle) =>
        Effect.tryPromise({
          try: () =>
            client.send(
              new DeleteMessageCommand({
                QueueUrl: config.SQS_QUEUE_URL,
                ReceiptHandle: receiptHandle,
              }),
            ),
          catch: (e) => new SQSDeleteError({ cause: e }),
        }).pipe(Effect.asVoid),

      extendVisibility: (receiptHandle, timeoutSec) =>
        Effect.tryPromise({
          try: () =>
            client.send(
              new ChangeMessageVisibilityCommand({
                QueueUrl: config.SQS_QUEUE_URL,
                ReceiptHandle: receiptHandle,
                VisibilityTimeout: timeoutSec,
              }),
            ),
          catch: (e) => new SQSReceiveError({ cause: e }),
        }).pipe(Effect.asVoid),
    };
  }),
);

export interface StubOpts {
  messages: SQSMessage[];
}

export function SQSServiceStub({
  messages,
}: StubOpts): Layer.Layer<SQSService> {
  const queue = [...messages];
  return Layer.succeed(SQSService, {
    receiveMessage: () => Effect.succeed(queue.shift() ?? null),
    deleteMessage: () => Effect.succeed(undefined),
    extendVisibility: () => Effect.succeed(undefined),
  });
}
