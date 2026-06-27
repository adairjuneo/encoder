import { describe, it, expect } from "vitest"
import { Effect } from "effect"
import { R2Service, R2ServiceStub } from "./R2Layer.js"

describe("R2Service Stub", () => {
  it("records uploaded buffers", async () => {
    const uploads: Map<string, Buffer> = new Map()
    const stub = R2ServiceStub(uploads)

    await Effect.runPromise(
      Effect.gen(function* () {
        const r2 = yield* R2Service
        yield* r2.uploadBuffer(
          "transcoded/job1/480p/seg_0000.ts",
          Buffer.from("data"),
        )
      }).pipe(Effect.provide(stub)),
    )

    expect(uploads.has("transcoded/job1/480p/seg_0000.ts")).toBe(true)
    expect(uploads.get("transcoded/job1/480p/seg_0000.ts")).toEqual(
      Buffer.from("data"),
    )
  })

  it("returns a fake presigned URL", async () => {
    const stub = R2ServiceStub(new Map())
    const url = await Effect.runPromise(
      Effect.gen(function* () {
        const r2 = yield* R2Service
        return yield* r2.getPresignedUrl("transcoded/job1/master.m3u8")
      }).pipe(Effect.provide(stub)),
    )
    expect(url).toContain("transcoded/job1/master.m3u8")
  })
})
