# PRD — Video Transcoder Worker v3

## Problem Statement

The encoder service has a spec but no implementation. There is no mechanism to take a video file URL, transcode it into adaptive-streaming HLS format (480p, 720p, 1080p), upload the segments to cloud storage, or notify downstream services of job progress and completion. The existing codebase is a four-file skeleton: environment validation, a placeholder entry point, and a byte formatter. All service layers, pipelines, error handling, and process lifecycle logic are missing.

## Solution

Implement the full transcoding worker described in `video-transcoder-spec-v3.md`. The worker runs as a single-job process on an EC2 spot instance:

1. Polls SQS for a job (long-poll, MaxMessages=1)
2. Extends SQS visibility via a background heartbeat fiber (every 4 minutes)
3. Publishes `job.started` to SNS
4. Transcodes input video via FFmpeg to HLS segments for 480p, 720p, and 1080p in batches of 2 (480p + 720p concurrently, then 1080p)
5. Uploads each HLS segment to Cloudflare R2 in real-time as FFmpeg writes it (chokidar-driven)
6. Publishes per-resolution `job.progress` events to SNS
7. Generates and uploads `master.m3u8` and `thumbnail.jpg`
8. Queries AWS EC2 Spot Price History to compute job cost
9. Publishes `job.completed` or `job.failed` to SNS (with OutputManifest or error details)
10. Deletes SQS message on success (leaving it for retry on failure)
11. Exits with code 0 (success) or 1 (failure)

## User Stories

### Job Dispatcher (produces SQS messages)

1. As a job dispatcher, I want to enqueue a transcoding job by publishing a JSON message to SQS, so that the worker can pick it up without any direct coupling.
2. As a job dispatcher, I want the SQS message to contain `id`, `inputUrl`, `outputPrefix`, and optional `callbackData`, so that the worker has everything it needs to execute the job.
3. As a job dispatcher, I want the worker to use long-polling so that jobs are picked up within seconds of being enqueued.
4. As a job dispatcher, I want unprocessed jobs to be retried automatically if the worker crashes, so that I don't need to re-enqueue them manually.
5. As a job dispatcher, I want a failed job to leave the SQS message in-flight (not deleted), so that the SQS visibility timeout mechanism handles retry.

### Transcoding Worker (the system itself)

6. As the transcoding worker, I want to validate the job payload schema before starting work, so that malformed messages fail fast with a clear error.
7. As the transcoding worker, I want to fetch the EC2 instance ID from IMDS v2 at startup, so that I can include it in all SNS events for traceability.
8. As the transcoding worker, I want to probe the input video's duration with `ffprobe` before transcoding, so that I can publish accurate progress percentages.
9. As the transcoding worker, I want to extend SQS message visibility every 4 minutes in a background fiber, so that long jobs don't expire their lock while processing.
10. As the transcoding worker, I want to transcode 480p and 720p concurrently in Batch 1, then 1080p sequentially in Batch 2, so that CPU and network I/O are balanced.
11. As the transcoding worker, I want to upload each `.ts` segment to R2 immediately after FFmpeg writes it (via chokidar), so that upload time overlaps with transcode time.
12. As the transcoding worker, I want to build a per-resolution `index.m3u8` from the uploaded segments and upload it to R2 when transcoding completes.
13. As the transcoding worker, I want to build and upload a `master.m3u8` referencing all three resolutions after all pipelines complete.
14. As the transcoding worker, I want to extract a thumbnail JPEG from the video midpoint and upload it to R2 in parallel with master.m3u8 generation.
15. As the transcoding worker, I want to query AWS EC2 Spot Price History to calculate the actual cost of the instance for the job duration.
16. As the transcoding worker, I want to delete the SQS message only after successfully completing all work, so that partial failures trigger retry.
17. As the transcoding worker, I want to clean up the `/tmp/transcoder/<jobId>` scratch directory on both success and failure, so that disk space is not leaked.
18. As the transcoding worker, I want to exit with code 0 on success and 1 on failure, so that the EC2 lifecycle Lambda can determine the termination reason.
19. As the transcoding worker, I want all logs to be structured JSON via pino, so that log aggregation tools can query them.
20. As the transcoding worker, I want to run in local development using stub layers (no real AWS/R2/EC2), so that the pipeline can be tested without cloud credentials.

### Downstream Consumer (subscribes to SNS)

21. As a downstream consumer, I want to receive a `job.started` event with `jobId`, `instanceId`, `inputUrl`, and `timestamp`, so that I can track job initiation.
22. As a downstream consumer, I want to receive `job.progress` events with `resolution`, `transcodePct`, `segmentsUploaded`, and `overallPct`, so that I can display real-time progress to end users.
23. As a downstream consumer, I want to receive a `job.completed` event containing the full `OutputManifest` (URLs, cost, duration, size), so that I can update my database and surface the video.
24. As a downstream consumer, I want to receive a `job.failed` event with an error `code` and `message`, so that I can surface a useful error to the user and decide whether to retry.
25. As a downstream consumer, I want all SNS events to include `eventType` and `jobId` as `MessageAttributes`, so that I can use SNS filter policies to subscribe selectively.
26. As a downstream consumer, I want `callbackData` from the original job payload to be echoed back in completion and failure events, so that I can correlate the job to my internal records without a lookup.

### DevOps / Platform Operator

27. As a platform operator, I want the worker to run in a Docker container built from `node:20-alpine` with a separately installed FFmpeg 6.x, so that the image is minimal and reproducible.
28. As a platform operator, I want all environment variables to be validated at startup with clear error messages, so that misconfigured deployments fail fast before any SQS message is consumed.
29. As a platform operator, I want the EC2 instance ID to appear in every SNS event, so that I can correlate CloudWatch logs to job events.
30. As a platform operator, I want the worker's IAM policy to require only the minimum permissions (SQS ReceiveMessage/DeleteMessage/ChangeMessageVisibility, SNS Publish, EC2 DescribeSpotPriceHistory), so that the blast radius of a compromised instance is minimised.

## Implementation Decisions

- **Module system**: ESM (`"type": "module"` in package.json, `"module": "nodenext"` in tsconfig). Required by `execa@^8`.
- **Runtime**: Node.js 20 LTS, TypeScript 5.x (strict, no `any`), Effect v3.
- **Schema validation**: `Schema` from `effect` (built into `effect@3.x` — no separate `@effect/schema` package). Used for env config, job payload, and SNS event types.
- **FFmpeg subprocess**: `execa@^8` for full control over stderr streaming and exit codes. `fluent-ffmpeg` removed.
- **File watching**: `chokidar@^4` wrapped as `Effect.Stream<string>` to stream newly-written `.ts` segment paths.
- **Resolution batching**: `Array.chunksOf(RESOLUTION_BATCH_SIZE)` over `["480p", "720p", "1080p"]` processed with `Effect.forEach(..., { concurrency: BATCH_SIZE })`.
- **SQS heartbeat**: forked as an Effect `Fiber` using `Effect.forkDaemon`, interrupted on job completion or failure.
- **Logging**: `pino` (JSON structured) — one logger instance injected via the Config/bootstrap layer.
- **R2 storage**: `@aws-sdk/client-s3` + `@aws-sdk/lib-storage` pointed at Cloudflare R2 endpoint. Presigned URLs via `@aws-sdk/s3-request-presigner`.
- **Error taxonomy**: All errors are `Data.TaggedError` subclasses. Error type determines whether the SQS message is deleted or left for retry.
- **Stub layers**: Every layer with external I/O has a Stub variant (`SQSServiceStub`, `SNSServiceConsoleLive`, `EC2MetadataServiceStub`, `CostServiceStub`). Stubs are composed into a `ManualAppLayer` used by `manual.ts`.
- **Output layout**:
  ```
  <bucket>/transcoded/<jobId>/
  ├── master.m3u8
  ├── thumbnail.jpg
  ├── 480p/index.m3u8 + seg_NNNN.ts
  ├── 720p/index.m3u8 + seg_NNNN.ts
  └── 1080p/index.m3u8 + seg_NNNN.ts
  ```
- **`manual.ts`**: Uses a hardcoded `TEST_PAYLOAD` with a real public `.mkv` URL. All AWS/EC2 layers are stubs; R2 layer is optional (can write locally). Runnable with `npm run manual`.
- **ULID**: `ulid` package used to generate segment IDs if needed; job ID comes from the SQS message payload.

## Testing Decisions

**Good tests** test the service contract (inputs → outputs, error conditions), not internal implementation details. Tests should not assert that a specific SDK method was called — they should assert that the service behaved correctly given the inputs.

**Seam 1 — Layer contract tests** (`src/layers/*.test.ts`, `src/sqs/heartbeat.test.ts`)

Each service layer is tested with an Effect `TestLayer` or `Layer.succeed` stub for its dependencies. Tests cover:
- Happy path: returns expected value
- Failure path: AWS SDK throws → tagged error returned
- Heartbeat: `TestClock.adjust` to simulate time passing without waiting real time

Pattern:
```typescript
import { describe, it, expect } from "vitest"
import { Effect, Layer } from "effect"

it("returns null when queue is empty", async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const sqs = yield* SQSService
      return yield* sqs.receiveMessage()
    }).pipe(
      Effect.provide(SQSServiceStub({ messages: [] }))
    )
  )
  expect(result).toBeNull()
})
```

**Seam 2 — Pipeline integration test** (`src/pipelines/master.pipeline.test.ts`)

The highest seam: `transcodeJob(payload, state)` with all stub layers injected and the real FFmpeg binary. Uses a short fixture video (`test-fixtures/short.mkv`, ≤10s, created in Task 13). Verifies:
- Returns `OutputManifest` with correct shape and all three resolution keys
- SNS events published in correct order: `job.started` → `job.progress` × N → `job.completed`
- Temp directory removed after completion
- `job.failed` event published and temp dir cleaned on simulated FFmpeg failure

## Out of Scope

- HTTP API or any synchronous request/response interface
- Redis, BullMQ, or any other queueing mechanism (SQS only)
- Processing more than one job per process lifecycle
- Resuming interrupted jobs (SQS retry handles re-delivery)
- Video format support beyond what FFmpeg's HLS muxer handles for H.264/AAC
- Unit tests for `parseProgressLine` and `chokidar-stream` (covered implicitly by pipeline integration tests)
- CI/CD pipeline configuration
- Terraform / CDK infrastructure for SQS, SNS, EC2, R2

## Further Notes

- The spec specifies `@effect/schema` as a separate package. In `effect@3.x` (already installed), `Schema` is exported directly from `effect` — no additional package needed. All schema code uses `import { Schema } from "effect"`.
- The spec's R2 output path uses `<outputPrefix>` from the job payload as the base path under `transcoded/`. The actual key pattern is `transcoded/<outputPrefix>/<resolution>/seg_NNNN.ts`.
- The `manual.ts` entry point references a public test file (`PB.S01E01.150s.1080p.mkv` at 150 seconds). For the pipeline integration test, a shorter fixture (≤10s) should be generated with FFmpeg to keep test time under 60 seconds.
- EC2 IMDS v2 requires two requests: first `PUT /latest/api/token` with `X-aws-ec2-metadata-token-ttl-seconds: 21600`, then `GET /latest/meta-data/instance-id` with `X-aws-ec2-metadata-token: <token>`. The Stub returns `"i-test-instance"` and `"t3.medium"` without network calls.
