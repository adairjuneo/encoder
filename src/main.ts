import { Effect, Schema } from "effect"
import pino from "pino"
import { transcodeJob } from "./pipelines/master.pipeline.js"
import { SQSService } from "./layers/SQSLayer.js"
import { JobPayloadSchema } from "./types/index.js"
import { AppLayer } from "./bootstrap.js"
import { JobPayloadParseError } from "./errors/index.js"

const logger = pino({ level: process.env.LOG_LEVEL ?? "info" })

const main = Effect.gen(function* () {
  const sqs = yield* SQSService

  const message = yield* sqs.receiveMessage()
  if (!message) {
    logger.info("No message received — exiting.")
    process.exit(0)
  }

  const payload = yield* Effect.try({
    try: () => Schema.decodeUnknownSync(JobPayloadSchema)(JSON.parse(message.body)),
    catch: (e) => new JobPayloadParseError({ raw: message.body, cause: e }),
  })

  yield* transcodeJob(payload, message.receiptHandle)
})

Effect.runPromise(main.pipe(Effect.provide(AppLayer)))
  .then(() => {
    logger.info("Job completed successfully.")
    process.exit(0)
  })
  .catch((e) => {
    logger.error({ err: e }, "Job failed")
    process.exit(1)
  })
