import { Effect, Array as Arr, Fiber } from "effect"
import path from "node:path"
import fs from "node:fs/promises"
import { SQSService } from "../layers/SQSLayer.js"
import { SNSService } from "../layers/SNSLayer.js"
import { R2Service } from "../layers/R2Layer.js"
import { EC2MetadataService } from "../layers/EC2MetadataLayer.js"
import { CostService } from "../layers/CostLayer.js"
import { FFmpegService } from "../layers/FFmpegLayer.js"
import { ConfigService } from "../config/index.js"
import { forkSQSHeartbeat } from "../sqs/heartbeat.js"
import { HeartbeatError } from "../errors/index.js"
import { transcodeResolution } from "./resolution.pipeline.js"
import { extractThumbnail } from "./thumbnail.pipeline.js"
import { probeVideoDuration } from "../utils/ffmpeg-progress.js"
import type {
  JobPayload,
  OutputManifest,
  JobState,
  Resolution,
  ResolutionOutput,
  ResolutionProgress,
} from "../types/index.js"
import { RESOLUTIONS, RESOLUTION_CONFIGS } from "../types/index.js"

type AppDeps =
  | SQSService
  | SNSService
  | R2Service
  | EC2MetadataService
  | CostService
  | FFmpegService
  | ConfigService

const initialProgress = (): ResolutionProgress => ({
  status:           "pending",
  transcodePct:     0,
  segmentsUploaded: 0,
  segmentsTotal:    0,
})

export function transcodeJob(
  payload:       JobPayload,
  receiptHandle: string,
): Effect.Effect<OutputManifest, never, AppDeps> {
  // Mutable capture so catchAllCause can interrupt the heartbeat fiber on failure
  let heartbeatFiber: Fiber.RuntimeFiber<void, HeartbeatError> | null = null

  return Effect.gen(function* () {
    const config  = yield* ConfigService
    const sqs     = yield* SQSService
    const sns     = yield* SNSService
    const ec2     = yield* EC2MetadataService
    const cost    = yield* CostService

    const startedAt    = new Date()
    const instanceId   = yield* ec2.getInstanceId()
    const instanceType = yield* ec2.getInstanceType()
    const workDir      = path.join(config.TEMP_DIR, payload.id)

    yield* Effect.promise(() => fs.mkdir(workDir, { recursive: true }))

    const durationSec = yield* probeVideoDuration(payload.inputUrl)

    const jobState: JobState = {
      jobId:            payload.id,
      startedAt,
      instanceId,
      instanceType,
      receiptHandle,
      videoDurationSec: durationSec,
      totalOutputBytes: 0,
      progress: {
        "480p":  initialProgress(),
        "720p":  initialProgress(),
        "1080p": initialProgress(),
      },
    }

    yield* sns.publish({
      type:         "job.started",
      jobId:        payload.id,
      instanceId,
      inputUrl:     payload.inputUrl,
      timestamp:    startedAt.toISOString(),
      callbackData: payload.callbackData ?? null,
    })

    // Fork heartbeat fiber for the duration of the job.
    const fiber = yield* forkSQSHeartbeat(
      receiptHandle,
      config.SQS_VISIBILITY_TIMEOUT_SEC,
      config.SQS_HEARTBEAT_INTERVAL_MS,
    )
    heartbeatFiber = fiber  // capture for failure handler

    // Process resolutions in batches of RESOLUTION_BATCH_SIZE.
    const batches: Resolution[][] = Arr.chunksOf(RESOLUTIONS, config.RESOLUTION_BATCH_SIZE)

    const resolutionOutputs: Record<string, ResolutionOutput> = {}

    for (const batch of batches) {
      const results = yield* Effect.forEach(
        batch,
        (resolution) =>
          transcodeResolution(resolution, payload.inputUrl, workDir, jobState).pipe(
            Effect.map((progress) => ({ resolution, progress })),
          ),
        { concurrency: config.RESOLUTION_BATCH_SIZE },
      )

      for (const { resolution, progress } of results) {
        jobState.progress[resolution] = progress
        resolutionOutputs[resolution] = {
          playlistUrl: `transcoded/${payload.id}/${resolution}/index.m3u8`,
          bandwidth:   RESOLUTION_CONFIGS[resolution].bandwidth,
          codecs:      RESOLUTION_CONFIGS[resolution].codecs,
          resolution,
        }
      }
    }

    // Build master.m3u8 content.
    const masterLines = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      ...RESOLUTIONS.flatMap((res) => [
        `#EXT-X-STREAM-INF:BANDWIDTH=${RESOLUTION_CONFIGS[res].bandwidth},RESOLUTION=${RESOLUTION_CONFIGS[res].scale.replace(":", "x")},CODECS="${RESOLUTION_CONFIGS[res].codecs}"`,
        `${res}/index.m3u8`,
      ]),
    ].join("\n")

    // Extract thumbnail and upload master playlist in parallel.
    const masterKey = `transcoded/${payload.id}/master.m3u8`
    const [thumbnailKey] = yield* Effect.all(
      [
        extractThumbnail(payload.inputUrl, workDir, payload.id, durationSec),
        (yield* R2Service).uploadBuffer(masterKey, Buffer.from(masterLines, "utf-8")),
      ],
      { concurrency: 2 },
    )

    // Calculate cost.
    const endedAt    = new Date()
    const costReport = yield* cost.calculateJobCost(instanceType, startedAt, endedAt)

    // Delete SQS message (success path only).
    yield* sqs.deleteMessage(receiptHandle)

    const manifest: OutputManifest = {
      masterPlaylistUrl: `${config.R2_PUBLIC_BASE_URL}/${masterKey}`,
      thumbnailUrl:      `${config.R2_PUBLIC_BASE_URL}/${thumbnailKey}`,
      resolutions:       resolutionOutputs as OutputManifest["resolutions"],
      durationSec,
      processingMs:      endedAt.getTime() - startedAt.getTime(),
      outputSizeBytes:   jobState.totalOutputBytes,
      cost:              costReport,
    }

    yield* sns.publish({
      type:         "job.completed",
      jobId:        payload.id,
      instanceId,
      output:       manifest,
      timestamp:    endedAt.toISOString(),
      callbackData: payload.callbackData ?? null,
    })

    // Interrupt heartbeat and clean up temp files.
    yield* Fiber.interrupt(fiber).pipe(Effect.asVoid)
    yield* Effect.promise(() => fs.rm(workDir, { recursive: true, force: true }))

    return manifest
  }).pipe(
    Effect.catchAllCause((cause) =>
      Effect.gen(function* () {
        // Interrupt heartbeat fiber if it was started before the failure
        if (heartbeatFiber !== null) {
          yield* Fiber.interrupt(heartbeatFiber).pipe(Effect.asVoid, Effect.orElseSucceed(() => undefined))
        }

        const config     = yield* ConfigService
        const sns        = yield* SNSService
        const ec2        = yield* EC2MetadataService
        const workDir    = path.join(config.TEMP_DIR, payload.id)
        const instanceId = yield* ec2.getInstanceId().pipe(
          Effect.orElseSucceed(() => "unknown"),
        )

        yield* sns.publish({
          type:         "job.failed",
          jobId:        payload.id,
          instanceId,
          error:        { code: "TRANSCODE_ERROR", message: String(cause) },
          timestamp:    new Date().toISOString(),
          callbackData: payload.callbackData ?? null,
        }).pipe(Effect.orElseSucceed(() => undefined))

        yield* Effect.promise(() => fs.rm(workDir, { recursive: true, force: true }))

        return yield* Effect.failCause(cause)
      }),
    ),
    Effect.orDie,
  )
}
