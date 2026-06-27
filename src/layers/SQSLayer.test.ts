import { describe, it, expect } from "vitest"
import { Effect } from "effect"
import { SQSService, SQSServiceStub } from "./SQSLayer.js"

describe("SQSService Stub", () => {
  it("returns null when no messages", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const sqs = yield* SQSService
        return yield* sqs.receiveMessage()
      }).pipe(Effect.provide(SQSServiceStub({ messages: [] })))
    )
    expect(result).toBeNull()
  })

  it("returns and dequeues a message", async () => {
    const msg = { body: '{"id":"test"}', receiptHandle: "rh-1", messageId: "mid-1" }
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const sqs = yield* SQSService
        const first  = yield* sqs.receiveMessage()
        const second = yield* sqs.receiveMessage()
        return { first, second }
      }).pipe(Effect.provide(SQSServiceStub({ messages: [msg] })))
    )
    expect(result.first).toEqual(msg)
    expect(result.second).toBeNull()
  })

  it("extendVisibility succeeds without error", async () => {
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const sqs = yield* SQSService
          yield* sqs.extendVisibility("rh-1", 300)
        }).pipe(Effect.provide(SQSServiceStub({ messages: [] })))
      )
    ).resolves.toBeUndefined()
  })
})
