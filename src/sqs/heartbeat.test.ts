import { Duration, Effect, Fiber, Layer } from 'effect';
import { describe, expect, it } from 'vitest';
import { SQSService } from '../layers/SQSLayer.js';
import { forkSQSHeartbeat } from './heartbeat.js';

describe('forkSQSHeartbeat', () => {
  it('calls extendVisibility after the interval elapses', async () => {
    const calls: Array<{ handle: string; timeout: number }> = [];

    const mockSQSImpl = {
      receiveMessage: () => Effect.succeed(null),
      deleteMessage: () => Effect.succeed(undefined),
      extendVisibility: (handle: string, timeout: number) =>
        Effect.sync(() => {
          calls.push({ handle, timeout });
        }),
    };

    const mockSQSLayer = Layer.succeed(SQSService, mockSQSImpl);

    // Fork the heartbeat with a short 100ms interval and let it run for 250ms
    const fiber = await Effect.runPromise(
      Effect.gen(function* () {
        const f = yield* forkSQSHeartbeat('rh-test', 300, 100);
        // TestContext.TestContext (Effect v3 test clock layer) would allow TestClock.adjust here — use it if migrated
        yield* Effect.sleep(Duration.millis(250));
        return f;
      }).pipe(Effect.provide(mockSQSLayer)),
    );

    // Interrupt the fiber to clean it up
    await Effect.runPromise(Fiber.interrupt(fiber));

    expect(calls.length).toBeGreaterThanOrEqual(1);
    expect(calls[0]).toEqual({ handle: 'rh-test', timeout: 300 });
  });
});
