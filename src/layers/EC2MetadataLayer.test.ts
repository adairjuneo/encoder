import { describe, it, expect } from "vitest"
import { Effect } from "effect"
import { EC2MetadataService, EC2MetadataServiceStub } from "./EC2MetadataLayer.js"

describe("EC2MetadataService Stub", () => {
  it("returns stub instance ID", async () => {
    const id = await Effect.runPromise(
      Effect.gen(function* () {
        const ec2 = yield* EC2MetadataService
        return yield* ec2.getInstanceId()
      }).pipe(Effect.provide(EC2MetadataServiceStub))
    )
    expect(id).toBe("i-test-instance")
  })

  it("returns stub instance type", async () => {
    const type = await Effect.runPromise(
      Effect.gen(function* () {
        const ec2 = yield* EC2MetadataService
        return yield* ec2.getInstanceType()
      }).pipe(Effect.provide(EC2MetadataServiceStub))
    )
    expect(type).toBe("t3.medium")
  })
})
