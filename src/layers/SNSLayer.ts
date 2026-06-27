import { Context, Effect, Layer } from "effect"
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns"
import { ConfigService } from "../config/index.js"
import { SNSPublishError } from "../errors/index.js"
import type { SNSEvent } from "../types/index.js"

export interface SNSServiceShape {
  publish(event: SNSEvent): Effect.Effect<void, SNSPublishError>
}

export class SNSService extends Context.Tag("SNSService")<
  SNSService,
  SNSServiceShape
>() {}

export const SNSServiceLive = Layer.effect(
  SNSService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new SNSClient({ region: config.AWS_REGION })

    return {
      publish: (event: SNSEvent) =>
        Effect.tryPromise({
          try: () =>
            client.send(new PublishCommand({
              TopicArn: config.SNS_TOPIC_ARN,
              Message:  JSON.stringify(event),
              MessageAttributes: {
                eventType: { DataType: "String", StringValue: event.type },
                jobId:     { DataType: "String", StringValue: event.jobId },
              },
            })),
          catch: (e) => new SNSPublishError({ cause: e }),
        }).pipe(Effect.asVoid),
    }
  })
)

// Logs events to stdout — useful for manual.ts local dev.
export const SNSServiceConsoleLive: Layer.Layer<SNSService> = Layer.succeed(
  SNSService,
  {
    publish: (event: SNSEvent) =>
      Effect.sync(() => {
        console.log(`[SNS] ${event.type}`, JSON.stringify(event, null, 2))
      }),
  }
)

// Collects events into a provided array — for tests.
export function SNSServiceCollecting(collected: SNSEvent[]): Layer.Layer<SNSService> {
  return Layer.succeed(SNSService, {
    publish: (event: SNSEvent) =>
      Effect.sync(() => { collected.push(event) }),
  })
}
