import { describe, it, expect } from "vitest"
import { Effect } from "effect"
import { SNSService, SNSServiceCollecting } from "./SNSLayer.js"
import type { SNSEvent } from "../types/index.js"

describe("SNSService Collecting Stub", () => {
  it("collects published events in order", async () => {
    const events: SNSEvent[] = []
    const stub = SNSServiceCollecting(events)

    const event: SNSEvent = {
      type:         "job.started",
      jobId:        "job-1",
      instanceId:   "i-test",
      inputUrl:     "https://example.com/video.mkv",
      timestamp:    new Date().toISOString(),
      callbackData: null,
    }

    await Effect.runPromise(
      Effect.gen(function* () {
        const sns = yield* SNSService
        yield* sns.publish(event)
      }).pipe(Effect.provide(stub))
    )

    expect(events).toHaveLength(1)
    expect(events[0]).toMatchObject({ type: "job.started", jobId: "job-1" })
  })
})
