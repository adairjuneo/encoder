// Stub — full implementation in Issue #5.
// This file exists only so TypeScript can resolve the import in resolution.pipeline.ts.
import { Context, Effect, Layer } from "effect"
import { SNSPublishError } from "../errors/index.js"
import type { SNSEvent } from "../types/index.js"

export interface SNSServiceShape {
  publish(event: SNSEvent): Effect.Effect<void, SNSPublishError>
}

export class SNSService extends Context.Tag("SNSService")<
  SNSService,
  SNSServiceShape
>() {}

export const SNSServiceLive = Layer.succeed(SNSService, {
  publish: (_event: SNSEvent): Effect.Effect<void, SNSPublishError> =>
    Effect.fail(
      new SNSPublishError({
        cause: new Error("SNSServiceLive not yet implemented"),
      }),
    ),
})
