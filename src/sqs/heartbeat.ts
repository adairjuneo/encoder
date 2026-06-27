import { Duration, Effect, type Fiber, Schedule } from 'effect';
import { HeartbeatError } from '../errors/index.js';
import { SQSService } from '../layers/SQSLayer.js';

// Forks a daemon fiber that extends SQS message visibility on a repeating
// schedule. The fiber runs indefinitely until interrupted by the caller.
// The caller MUST interrupt this fiber (even on success) to avoid leaking it.
export function forkSQSHeartbeat(
  receiptHandle: string,
  visibilityTimeout: number,
  intervalMs: number,
): Effect.Effect<Fiber.RuntimeFiber<void, HeartbeatError>, never, SQSService> {
  const heartbeat = Effect.gen(function* () {
    const sqs = yield* SQSService;
    yield* sqs
      .extendVisibility(receiptHandle, visibilityTimeout)
      .pipe(Effect.mapError((e) => new HeartbeatError({ cause: e })));
  }).pipe(
    Effect.repeat(Schedule.spaced(Duration.millis(intervalMs))),
    Effect.asVoid,
  );

  return Effect.forkDaemon(heartbeat);
}
