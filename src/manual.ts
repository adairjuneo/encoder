import { Effect, Schema } from "effect"
import { transcodeJob } from "./pipelines/master.pipeline.js"
import { JobPayloadSchema } from "./types/index.js"
import { ManualAppLayer } from "./bootstrap.js"

const TEST_PAYLOAD = Schema.decodeUnknownSync(JobPayloadSchema)({
  id:           "manual-test-01",
  inputUrl:     "https://pub-d161d9f2b7ce41e4a08dfd8c7f742dc7.r2.dev/raw/tests/PB.S01E01.150s.1080p.mkv",
  outputPrefix: "manual-test-01",
  callbackData: { source: "manual-run" },
})

console.info(`[manual] Worker started at ${new Date().toISOString()}`)

Effect.runPromise(
  transcodeJob(TEST_PAYLOAD, "manual-receipt-handle").pipe(
    Effect.provide(ManualAppLayer)
  )
)
  .then((manifest) => {
    console.log("[manual] Completed:", JSON.stringify(manifest, null, 2))
    process.exit(0)
  })
  .catch((e) => {
    console.error("[manual] Failed:", e)
    process.exit(1)
  })
