# Video Transcoder Worker v3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a TypeScript Effect-based video transcoding worker that polls SQS, transcodes to HLS via FFmpeg, uploads to Cloudflare R2 in real time, and publishes job lifecycle events to SNS.

**Architecture:** Foundation-first — types and errors establish contracts, service layers implement each external dependency (SQS/SNS/R2/EC2/Cost), pipelines compose the layers, and entry points wire everything together. All layers have a Live variant (real AWS) and a Stub variant (for local dev/tests).

**Tech Stack:** Node.js 20 LTS · TypeScript strict · Effect v3 · execa@8 · chokidar@4 · pino · @aws-sdk/{sqs,sns,s3,ec2} · Cloudflare R2 (S3-compatible) · vitest

## Global Constraints

- ESM throughout — all relative imports use `.js` extension, even when the source file is `.ts`
- No `any` types — TypeScript strict mode is enforced
- All errors are `Data.TaggedError` subclasses — no `throw`, no raw `Error` in service code
- `Schema` is imported from `"effect"`, not a separate `@effect/schema` package
- Effect layer pattern: `Context.Tag` → `Layer.effect` (Live) + `Layer.succeed` (Stub)
- Pino logger used everywhere — no `console.log` in production paths
- All tests: `Effect.runPromise(program.pipe(Effect.provide(stubLayer)))` pattern
- Vitest globals enabled — no need to import `describe`/`it`/`expect` in test files

---

## File Map

```
src/
├── types/index.ts              # All domain types (Resolution, JobPayload, JobState, etc.)
├── errors/index.ts             # All Data.TaggedError subclasses
├── config/
│   ├── index.ts                # ConfigService tag + AppConfig interface
│   └── config.test.ts          # (Task 2)
├── utils/
│   ├── ffmpeg-progress.ts      # parseProgressLine, probeVideoDuration
│   └── chokidar-stream.ts      # watchDirectory → Stream<string>
├── layers/
│   ├── SQSLayer.ts             # SQSService tag, Live, Stub
│   ├── SQSLayer.test.ts
│   ├── SNSLayer.ts             # SNSService tag, Live, ConsoleLive
│   ├── SNSLayer.test.ts
│   ├── R2Layer.ts              # R2Service tag, Live, Stub
│   ├── R2Layer.test.ts
│   ├── EC2MetadataLayer.ts     # EC2MetadataService tag, Live, Stub
│   ├── EC2MetadataLayer.test.ts
│   ├── CostLayer.ts            # CostService tag, Live, Stub
│   ├── CostLayer.test.ts
│   └── FFmpegLayer.ts          # FFmpegService tag, Live (resolves binary path)
├── sqs/
│   ├── heartbeat.ts            # forkSQSHeartbeat
│   └── heartbeat.test.ts
├── pipelines/
│   ├── resolution.pipeline.ts  # transcodeResolution
│   ├── thumbnail.pipeline.ts   # extractThumbnail
│   ├── master.pipeline.ts      # transcodeJob
│   └── master.pipeline.test.ts # Integration test (highest seam)
├── bootstrap.ts                # AppLayer (all Live layers composed)
├── main.ts                     # Production entry point
├── manual.ts                   # Local dev entry point (stubs)
└── utils.ts                    # (existing) formatBytes — leave as-is
```

Modified at root:
- `package.json` — ESM, new deps, remove fluent-ffmpeg/zod, add vitest scripts
- `tsconfig.json` — nodenext module resolution
- `vitest.config.ts` — new file

---

## Task 0: Project Setup

**Files:**
- Modify: `package.json`
- Modify: `tsconfig.json`
- Create: `vitest.config.ts`
- Delete: `src/env.ts` (replaced by `src/config/index.ts` in Task 2)

**Interfaces:**
- Produces: ESM build environment, all required packages installed, vitest test runner configured

- [ ] **Step 1: Update package.json to ESM + swap dependencies**

Replace the `package.json` content:

```json
{
  "name": "encoder",
  "version": "2.0.0",
  "type": "module",
  "main": "build/main.js",
  "scripts": {
    "build": "tsc",
    "start": "node build/main.js",
    "manual": "tsx --env-file=.env src/manual.ts",
    "dev": "tsx --watch --env-file=.env src/manual.ts",
    "test": "vitest run",
    "test:watch": "vitest",
    "typecheck": "tsc --noEmit",
    "lint": "npx @biomejs/biome lint --write",
    "lint:check": "npx @biomejs/biome check --write",
    "lint:format": "npx @biomejs/biome format --write"
  },
  "dependencies": {
    "effect": "3.21.4",
    "@effect/platform": "0.96.2",
    "@effect/platform-node": "0.107.0",
    "@aws-sdk/client-s3": "3.1075.0",
    "@aws-sdk/client-sqs": "^3.1075.0",
    "@aws-sdk/client-sns": "^3.1075.0",
    "@aws-sdk/client-ec2": "^3.1075.0",
    "@aws-sdk/lib-storage": "3.1075.0",
    "@aws-sdk/s3-request-presigner": "^3.1075.0",
    "chokidar": "^4.0.0",
    "execa": "^9.0.0",
    "pino": "^9.0.0",
    "ulid": "^2.3.0"
  },
  "devDependencies": {
    "@biomejs/biome": "2.5.1",
    "@types/node": "26.0.1",
    "tsup": "8.5.1",
    "tsx": "4.22.4",
    "typescript": "6.0.3",
    "vitest": "^2.0.0"
  }
}
```

- [ ] **Step 2: Update tsconfig.json for nodenext ESM**

```json
{
  "compilerOptions": {
    "lib": ["es2022"],
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "target": "es2022",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "types": ["node"],
    "rootDir": "./src",
    "outDir": "build",
    "declaration": true,
    "sourceMap": true,
    "paths": {
      "@/*": ["./src/*"]
    }
  },
  "exclude": ["node_modules", "build", "dist", "_tmp", "_upload", "**/*.test.ts"]
}
```

- [ ] **Step 3: Create vitest.config.ts**

```typescript
import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    globals: true,
    pool: "forks",
    testTimeout: 120_000,
  },
})
```

- [ ] **Step 4: Install dependencies and remove old ones**

```bash
npm install
npm uninstall fluent-ffmpeg zod
npm install
```

Expected: no errors, `node_modules` contains `execa`, `chokidar`, `pino`, `ulid`, all four `@aws-sdk/client-*` packages.

- [ ] **Step 5: Verify TypeScript compiles**

```bash
npx tsc --noEmit
```

Expected: exits 0. (Only `src/index.ts` and `src/utils.ts` are compiled; `src/env.ts` will be deleted now.)

- [ ] **Step 6: Delete src/env.ts and src/index.ts**

```bash
rm src/env.ts src/index.ts
```

- [ ] **Step 7: Verify vitest can find test files (no tests yet — just config check)**

```bash
npx vitest run --reporter=verbose 2>&1 | head -5
```

Expected: `No test files found` (not an error, just no tests yet).

- [ ] **Step 8: Commit**

```bash
git add package.json tsconfig.json vitest.config.ts src/
git commit -m "chore: migrate to ESM, swap deps (execa/pino/chokidar), add vitest"
```

---

## Task 1: Types and Errors

**Files:**
- Create: `src/types/index.ts`
- Create: `src/errors/index.ts`

**Interfaces:**
- Produces: All domain types and error classes referenced by every subsequent task

- [ ] **Step 1: Create src/types/index.ts**

```typescript
import { Schema } from "effect"

// ─── Resolution ───────────────────────────────────────────────────────────────

export type Resolution = "480p" | "720p" | "1080p"

export const RESOLUTIONS: Resolution[] = ["480p", "720p", "1080p"]

export interface ResolutionConfig {
  scale: string          // e.g. "854:480"
  crf: number            // Constant Rate Factor
  audioBitrate: string   // e.g. "96k"
  bandwidth: number      // bits/s for HLS manifest
  codecs: string         // e.g. "avc1.42c01e,mp4a.40.2"
}

export const RESOLUTION_CONFIGS: Record<Resolution, ResolutionConfig> = {
  "480p":  { scale: "854:480",   crf: 28, audioBitrate: "96k",  bandwidth: 800_000,  codecs: "avc1.42c01e,mp4a.40.2" },
  "720p":  { scale: "1280:720",  crf: 23, audioBitrate: "128k", bandwidth: 2_800_000, codecs: "avc1.4d401f,mp4a.40.2" },
  "1080p": { scale: "1920:1080", crf: 20, audioBitrate: "192k", bandwidth: 5_000_000, codecs: "avc1.640028,mp4a.40.2" },
}

// ─── Job Payload (from SQS message body) ─────────────────────────────────────

export const JobPayloadSchema = Schema.Struct({
  id:           Schema.String,
  inputUrl:     Schema.String,
  outputPrefix: Schema.String,
  callbackData: Schema.optional(Schema.Unknown),
})

export type JobPayload = typeof JobPayloadSchema.Type

// ─── Resolution Progress ──────────────────────────────────────────────────────

export type ProgressStatus = "pending" | "transcoding" | "uploading" | "done" | "failed"

export interface ResolutionProgress {
  status:           ProgressStatus
  transcodePct:     number   // 0–100
  segmentsUploaded: number
  segmentsTotal:    number
}

// ─── Job State (in-memory, mutated throughout job lifecycle) ─────────────────

export interface JobState {
  jobId:           string
  startedAt:       Date
  instanceId:      string
  instanceType:    string
  receiptHandle:   string
  videoDurationSec: number
  totalOutputBytes: number
  progress:        Record<Resolution, ResolutionProgress>
}

// ─── Job Cost Report ──────────────────────────────────────────────────────────

export interface JobCostReport {
  costCents:           number
  spotPricePerHourUsd: number
  durationHours:       number
  instanceType:        string
  currency:            "USD"
}

// ─── Output Manifest ─────────────────────────────────────────────────────────

export interface ResolutionOutput {
  playlistUrl: string
  bandwidth:   number
  codecs:      string
  resolution:  string
}

export interface OutputManifest {
  masterPlaylistUrl: string
  thumbnailUrl:      string
  resolutions:       Record<Resolution, ResolutionOutput>
  durationSec:       number
  processingMs:      number
  outputSizeBytes:   number
  cost:              JobCostReport
}

// ─── SNS Events ───────────────────────────────────────────────────────────────

export type SNSEventType = "job.started" | "job.progress" | "job.completed" | "job.failed"

export interface SNSEventBase {
  jobId:     string
  timestamp: string
}

export interface JobStartedEvent extends SNSEventBase {
  type:         "job.started"
  instanceId:   string
  inputUrl:     string
  callbackData: unknown
}

export interface JobProgressEvent extends SNSEventBase {
  type:             "job.progress"
  resolution:       Resolution
  transcodePct:     number
  segmentsUploaded: number
  overallPct:       number
}

export interface JobCompletedEvent extends SNSEventBase {
  type:         "job.completed"
  instanceId:   string
  output:       OutputManifest
  callbackData: unknown
}

export interface JobFailedEvent extends SNSEventBase {
  type:         "job.failed"
  instanceId:   string
  error:        { code: string; message: string }
  callbackData: unknown
}

export type SNSEvent =
  | JobStartedEvent
  | JobProgressEvent
  | JobCompletedEvent
  | JobFailedEvent
```

- [ ] **Step 2: Create src/errors/index.ts**

```typescript
import { Data } from "effect"

export class ConfigError extends Data.TaggedError("ConfigError")<{
  message: string
}> {}

export class SQSReceiveError extends Data.TaggedError("SQSReceiveError")<{
  cause: unknown
}> {}

export class SQSDeleteError extends Data.TaggedError("SQSDeleteError")<{
  cause: unknown
}> {}

export class HeartbeatError extends Data.TaggedError("HeartbeatError")<{
  cause: unknown
}> {}

export class SNSPublishError extends Data.TaggedError("SNSPublishError")<{
  cause: unknown
}> {}

export class R2UploadError extends Data.TaggedError("R2UploadError")<{
  key:   string
  cause: unknown
}> {}

export class R2PresignError extends Data.TaggedError("R2PresignError")<{
  key:   string
  cause: unknown
}> {}

export class FFmpegError extends Data.TaggedError("FFmpegError")<{
  resolution: string
  exitCode:   number | null
  stderr:     string
}> {}

export class ProbeError extends Data.TaggedError("ProbeError")<{
  inputUrl: string
  cause:    unknown
}> {}

export class EC2MetadataError extends Data.TaggedError("EC2MetadataError")<{
  field: string
  cause: unknown
}> {}

export class CostCalculationError extends Data.TaggedError("CostCalculationError")<{
  cause: unknown
}> {}

export class JobPayloadParseError extends Data.TaggedError("JobPayloadParseError")<{
  raw:   string
  cause: unknown
}> {}
```

- [ ] **Step 3: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add src/types/index.ts src/errors/index.ts
git commit -m "feat: add domain types and tagged error classes"
```

---

## Task 2: Config Layer

**Files:**
- Create: `src/config/index.ts`
- Create: `src/config/config.test.ts`

**Interfaces:**
- Consumes: `ConfigError` from `src/errors/index.ts`
- Produces: `ConfigService` tag, `AppConfig` interface

- [ ] **Step 1: Write the failing test**

```typescript
// src/config/config.test.ts
import { describe, it, expect } from "vitest"
import { Effect } from "effect"
import { ConfigService, ConfigServiceLive } from "./index.js"

describe("ConfigService", () => {
  it("parses valid environment variables", async () => {
    process.env.AWS_REGION = "us-east-1"
    process.env.SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123/test"
    process.env.SQS_VISIBILITY_TIMEOUT_SEC = "300"
    process.env.SQS_HEARTBEAT_INTERVAL_MS = "240000"
    process.env.SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123:test"
    process.env.R2_ACCOUNT_ID = "acct"
    process.env.R2_ACCESS_KEY_ID = "key"
    process.env.R2_SECRET_ACCESS_KEY = "secret"
    process.env.R2_BUCKET_NAME = "my-bucket"
    process.env.R2_PUBLIC_BASE_URL = "https://r2.example.com"
    process.env.TEMP_DIR = "/tmp/transcoder"
    process.env.FFMPEG_PRESET = "veryfast"
    process.env.RESOLUTION_BATCH_SIZE = "2"
    process.env.LOG_LEVEL = "info"
    process.env.NODE_ENV = "test"

    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* ConfigService
      }).pipe(Effect.provide(ConfigServiceLive))
    )

    expect(config.AWS_REGION).toBe("us-east-1")
    expect(config.SQS_VISIBILITY_TIMEOUT_SEC).toBe(300)
    expect(config.RESOLUTION_BATCH_SIZE).toBe(2)
  })

  it("throws ConfigError when required variable is missing", async () => {
    const saved = process.env.AWS_REGION
    delete process.env.AWS_REGION

    await expect(
      Effect.runPromise(
        Effect.provide(ConfigService, ConfigServiceLive)
      )
    ).rejects.toThrow()

    process.env.AWS_REGION = saved
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/config/config.test.ts
```

Expected: FAIL — `Cannot find module './index.js'`

- [ ] **Step 3: Create src/config/index.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import { ConfigError } from "../errors/index.js"

export interface AppConfig {
  NODE_ENV:                   "development" | "test" | "production"
  AWS_REGION:                 string
  SQS_QUEUE_URL:              string
  SQS_VISIBILITY_TIMEOUT_SEC: number
  SQS_HEARTBEAT_INTERVAL_MS:  number
  SNS_TOPIC_ARN:              string
  R2_ACCOUNT_ID:              string
  R2_ACCESS_KEY_ID:           string
  R2_SECRET_ACCESS_KEY:       string
  R2_BUCKET_NAME:             string
  R2_PUBLIC_BASE_URL:         string
  TEMP_DIR:                   string
  FFMPEG_PRESET:              "ultrafast" | "veryfast" | "medium"
  RESOLUTION_BATCH_SIZE:      number
  LOG_LEVEL:                  string
}

export class ConfigService extends Context.Tag("ConfigService")<
  ConfigService,
  AppConfig
>() {}

function required(key: string): Effect.Effect<string, ConfigError> {
  const val = process.env[key]
  if (!val) return Effect.fail(new ConfigError({ message: `Missing required env var: ${key}` }))
  return Effect.succeed(val)
}

function optionalInt(key: string, fallback: number): number {
  const val = process.env[key]
  if (!val) return fallback
  const n = parseInt(val, 10)
  return isNaN(n) ? fallback : n
}

function optionalStr(key: string, fallback: string): string {
  return process.env[key] ?? fallback
}

export const ConfigServiceLive = Layer.effect(
  ConfigService,
  Effect.gen(function* () {
    const AWS_REGION   = yield* required("AWS_REGION")
    const SQS_QUEUE_URL = yield* required("SQS_QUEUE_URL")
    const SNS_TOPIC_ARN = yield* required("SNS_TOPIC_ARN")
    const R2_ACCOUNT_ID = yield* required("R2_ACCOUNT_ID")
    const R2_ACCESS_KEY_ID = yield* required("R2_ACCESS_KEY_ID")
    const R2_SECRET_ACCESS_KEY = yield* required("R2_SECRET_ACCESS_KEY")
    const R2_BUCKET_NAME = yield* required("R2_BUCKET_NAME")
    const R2_PUBLIC_BASE_URL = yield* required("R2_PUBLIC_BASE_URL")

    const preset = optionalStr("FFMPEG_PRESET", "veryfast")
    if (!["ultrafast", "veryfast", "medium"].includes(preset)) {
      yield* Effect.fail(new ConfigError({ message: `Invalid FFMPEG_PRESET: ${preset}` }))
    }

    return {
      NODE_ENV: (process.env.NODE_ENV ?? "development") as AppConfig["NODE_ENV"],
      AWS_REGION,
      SQS_QUEUE_URL,
      SQS_VISIBILITY_TIMEOUT_SEC: optionalInt("SQS_VISIBILITY_TIMEOUT_SEC", 300),
      SQS_HEARTBEAT_INTERVAL_MS:  optionalInt("SQS_HEARTBEAT_INTERVAL_MS", 240_000),
      SNS_TOPIC_ARN,
      R2_ACCOUNT_ID,
      R2_ACCESS_KEY_ID,
      R2_SECRET_ACCESS_KEY,
      R2_BUCKET_NAME,
      R2_PUBLIC_BASE_URL,
      TEMP_DIR:               optionalStr("TEMP_DIR", "/tmp/transcoder"),
      FFMPEG_PRESET:          preset as AppConfig["FFMPEG_PRESET"],
      RESOLUTION_BATCH_SIZE:  optionalInt("RESOLUTION_BATCH_SIZE", 2),
      LOG_LEVEL:              optionalStr("LOG_LEVEL", "info"),
    } satisfies AppConfig
  })
)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/config/config.test.ts
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/config/
git commit -m "feat: add ConfigService layer with env var validation"
```

---

## Task 3: FFmpeg Utilities

**Files:**
- Create: `src/utils/ffmpeg-progress.ts`

**Interfaces:**
- Consumes: `ProbeError` from `src/errors/index.ts`
- Produces:
  - `parseProgressLine(line: string): number | null` — returns 0–100 or null if line is not a time= progress line
  - `probeVideoDuration(inputPath: string): Effect.Effect<number, ProbeError>` — returns duration in seconds

- [ ] **Step 1: Create src/utils/ffmpeg-progress.ts**

```typescript
import { Effect } from "effect"
import { execa } from "execa"
import { ProbeError } from "../errors/index.js"

// Parses FFmpeg stderr lines that contain "time=HH:MM:SS.ss" progress.
// Returns elapsed seconds as a fraction of totalDuration (0–100), or null
// if the line does not contain time= progress data.
export function parseProgressLine(line: string, totalDurationSec: number): number | null {
  const match = line.match(/time=(\d{2}):(\d{2}):(\d{2}\.\d+)/)
  if (!match) return null

  const hours   = parseInt(match[1], 10)
  const minutes = parseInt(match[2], 10)
  const seconds = parseFloat(match[3])
  const elapsed = hours * 3600 + minutes * 60 + seconds

  if (totalDurationSec === 0) return 0
  return Math.min(100, Math.round((elapsed / totalDurationSec) * 100))
}

// Runs ffprobe to get video duration in seconds.
export function probeVideoDuration(inputPath: string): Effect.Effect<number, ProbeError> {
  return Effect.tryPromise({
    try: async () => {
      const { stdout } = await execa("ffprobe", [
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        inputPath,
      ])
      const duration = parseFloat(stdout.trim())
      if (isNaN(duration)) throw new Error(`ffprobe returned non-numeric duration: ${stdout}`)
      return duration
    },
    catch: (e) => new ProbeError({ inputUrl: inputPath, cause: e }),
  })
}
```

- [ ] **Step 2: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/utils/ffmpeg-progress.ts
git commit -m "feat: add FFmpeg progress parser and ffprobe duration utility"
```

---

## Task 4: Chokidar Stream Utility

**Files:**
- Create: `src/utils/chokidar-stream.ts`

**Interfaces:**
- Produces: `watchDirectory(dir: string): Stream.Stream<string>` — emits full paths of newly added `.ts` files

- [ ] **Step 1: Create src/utils/chokidar-stream.ts**

```typescript
import { Stream } from "effect"
import chokidar from "chokidar"
import path from "node:path"

// Returns a Stream that emits the absolute path of each .ts segment file
// added to the watched directory. The stream does not error — chokidar
// watcher errors are ignored (uploads that fail are caught at the R2 layer).
// Callers are responsible for interrupting the stream when transcoding ends.
export function watchDirectory(dir: string): Stream.Stream<string> {
  return Stream.async<string>((emit) => {
    const watcher = chokidar.watch(dir, {
      persistent:  true,
      ignoreInitial: true,
      awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
    })

    watcher.on("add", (filePath: string) => {
      if (filePath.endsWith(".ts")) {
        emit.single(filePath)
      }
    })

    return () => { watcher.close() }
  })
}
```

- [ ] **Step 2: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/utils/chokidar-stream.ts
git commit -m "feat: add chokidar directory watcher as Effect Stream"
```

---

## Task 5: SQS Layer

**Files:**
- Create: `src/layers/SQSLayer.ts`
- Create: `src/layers/SQSLayer.test.ts`

**Interfaces:**
- Consumes: `ConfigService`, `SQSReceiveError`, `SQSDeleteError`
- Produces:
  - `SQSMessage` interface
  - `SQSService` tag with `receiveMessage`, `deleteMessage`, `extendVisibility`
  - `SQSServiceLive` layer
  - `SQSServiceStub(opts)` factory

- [ ] **Step 1: Write the failing test**

```typescript
// src/layers/SQSLayer.test.ts
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/layers/SQSLayer.test.ts
```

Expected: FAIL — `Cannot find module './SQSLayer.js'`

- [ ] **Step 3: Create src/layers/SQSLayer.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} from "@aws-sdk/client-sqs"
import { ConfigService } from "../config/index.js"
import { SQSReceiveError, SQSDeleteError } from "../errors/index.js"

export interface SQSMessage {
  body:          string
  receiptHandle: string
  messageId:     string
}

export interface SQSServiceShape {
  receiveMessage(): Effect.Effect<SQSMessage | null, SQSReceiveError>
  deleteMessage(receiptHandle: string): Effect.Effect<void, SQSDeleteError>
  extendVisibility(receiptHandle: string, timeoutSec: number): Effect.Effect<void, SQSReceiveError>
}

export class SQSService extends Context.Tag("SQSService")<
  SQSService,
  SQSServiceShape
>() {}

export const SQSServiceLive = Layer.effect(
  SQSService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new SQSClient({ region: config.AWS_REGION })

    return {
      receiveMessage: () =>
        Effect.tryPromise({
          try: () =>
            client.send(new ReceiveMessageCommand({
              QueueUrl:            config.SQS_QUEUE_URL,
              MaxNumberOfMessages: 1,
              WaitTimeSeconds:     20,
            })),
          catch: (e) => new SQSReceiveError({ cause: e }),
        }).pipe(
          Effect.map((res) => {
            const m = res.Messages?.[0]
            if (!m?.Body || !m.ReceiptHandle || !m.MessageId) return null
            return { body: m.Body, receiptHandle: m.ReceiptHandle, messageId: m.MessageId }
          })
        ),

      deleteMessage: (receiptHandle) =>
        Effect.tryPromise({
          try: () =>
            client.send(new DeleteMessageCommand({
              QueueUrl:      config.SQS_QUEUE_URL,
              ReceiptHandle: receiptHandle,
            })),
          catch: (e) => new SQSDeleteError({ cause: e }),
        }).pipe(Effect.asVoid),

      extendVisibility: (receiptHandle, timeoutSec) =>
        Effect.tryPromise({
          try: () =>
            client.send(new ChangeMessageVisibilityCommand({
              QueueUrl:          config.SQS_QUEUE_URL,
              ReceiptHandle:     receiptHandle,
              VisibilityTimeout: timeoutSec,
            })),
          catch: (e) => new SQSReceiveError({ cause: e }),
        }).pipe(Effect.asVoid),
    }
  })
)

interface StubOpts {
  messages: SQSMessage[]
}

export function SQSServiceStub({ messages }: StubOpts): Layer.Layer<SQSService> {
  const queue = [...messages]
  return Layer.succeed(SQSService, {
    receiveMessage:   () => Effect.succeed(queue.shift() ?? null),
    deleteMessage:    () => Effect.succeed(undefined),
    extendVisibility: () => Effect.succeed(undefined),
  })
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/layers/SQSLayer.test.ts
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/layers/SQSLayer.ts src/layers/SQSLayer.test.ts
git commit -m "feat: add SQSService layer with Live and Stub variants"
```

---

## Task 6: SNS Layer

**Files:**
- Create: `src/layers/SNSLayer.ts`
- Create: `src/layers/SNSLayer.test.ts`

**Interfaces:**
- Consumes: `ConfigService`, `SNSPublishError`, `SNSEvent`
- Produces:
  - `SNSService` tag with `publish(event: SNSEvent): Effect<void, SNSPublishError>`
  - `SNSServiceLive` layer
  - `SNSServiceConsoleLive` stub (logs to stdout, no real SNS)
  - `SNSServiceCollecting` stub for tests (collects published events)

- [ ] **Step 1: Write the failing test**

```typescript
// src/layers/SNSLayer.test.ts
import { describe, it, expect } from "vitest"
import { Effect, Ref } from "effect"
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/layers/SNSLayer.test.ts
```

Expected: FAIL — `Cannot find module './SNSLayer.js'`

- [ ] **Step 3: Create src/layers/SNSLayer.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns"
import { ConfigService } from "../config/index.js"
import { SNSPublishError } from "../errors/index.js"
import type { SNSEvent, SNSEventType } from "../types/index.js"

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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/layers/SNSLayer.test.ts
```

Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add src/layers/SNSLayer.ts src/layers/SNSLayer.test.ts
git commit -m "feat: add SNSService layer with Live, ConsoleLive, and Collecting stubs"
```

---

## Task 7: R2 Layer

**Files:**
- Create: `src/layers/R2Layer.ts`
- Create: `src/layers/R2Layer.test.ts`

**Interfaces:**
- Consumes: `ConfigService`, `R2UploadError`, `R2PresignError`
- Produces:
  - `R2Service` tag with `uploadFile`, `uploadBuffer`, `getPresignedUrl`
  - `R2ServiceLive` layer
  - `R2ServiceStub` layer (captures uploads in memory)

- [ ] **Step 1: Write the failing test**

```typescript
// src/layers/R2Layer.test.ts
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
        yield* r2.uploadBuffer("transcoded/job1/480p/seg_0000.ts", Buffer.from("data"))
      }).pipe(Effect.provide(stub))
    )

    expect(uploads.has("transcoded/job1/480p/seg_0000.ts")).toBe(true)
    expect(uploads.get("transcoded/job1/480p/seg_0000.ts")).toEqual(Buffer.from("data"))
  })

  it("returns a fake presigned URL", async () => {
    const stub = R2ServiceStub(new Map())
    const url = await Effect.runPromise(
      Effect.gen(function* () {
        const r2 = yield* R2Service
        return yield* r2.getPresignedUrl("transcoded/job1/master.m3u8")
      }).pipe(Effect.provide(stub))
    )
    expect(url).toContain("transcoded/job1/master.m3u8")
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/layers/R2Layer.test.ts
```

Expected: FAIL

- [ ] **Step 3: Create src/layers/R2Layer.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3"
import { Upload } from "@aws-sdk/lib-storage"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import fs from "node:fs"
import { ConfigService } from "../config/index.js"
import { R2UploadError, R2PresignError } from "../errors/index.js"

export interface R2ServiceShape {
  uploadFile(key: string, localPath: string): Effect.Effect<void, R2UploadError>
  uploadBuffer(key: string, data: Buffer): Effect.Effect<void, R2UploadError>
  getPresignedUrl(key: string): Effect.Effect<string, R2PresignError>
}

export class R2Service extends Context.Tag("R2Service")<
  R2Service,
  R2ServiceShape
>() {}

export const R2ServiceLive = Layer.effect(
  R2Service,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new S3Client({
      region: "auto",
      endpoint: `https://${config.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId:     config.R2_ACCESS_KEY_ID,
        secretAccessKey: config.R2_SECRET_ACCESS_KEY,
      },
    })

    return {
      uploadFile: (key, localPath) =>
        Effect.tryPromise({
          try: async () => {
            const stream = fs.createReadStream(localPath)
            const upload = new Upload({
              client,
              params: { Bucket: config.R2_BUCKET_NAME, Key: key, Body: stream },
            })
            await upload.done()
          },
          catch: (e) => new R2UploadError({ key, cause: e }),
        }),

      uploadBuffer: (key, data) =>
        Effect.tryPromise({
          try: () =>
            client.send(new PutObjectCommand({
              Bucket: config.R2_BUCKET_NAME,
              Key:    key,
              Body:   data,
            })),
          catch: (e) => new R2UploadError({ key, cause: e }),
        }).pipe(Effect.asVoid),

      getPresignedUrl: (key) =>
        Effect.tryPromise({
          try: () =>
            getSignedUrl(
              client,
              new PutObjectCommand({ Bucket: config.R2_BUCKET_NAME, Key: key }),
              { expiresIn: 3600 }
            ),
          catch: (e) => new R2PresignError({ key, cause: e }),
        }).pipe(
          Effect.map(() => `${config.R2_PUBLIC_BASE_URL}/${key}`)
        ),
    }
  })
)

export function R2ServiceStub(uploads: Map<string, Buffer>): Layer.Layer<R2Service> {
  return Layer.succeed(R2Service, {
    uploadFile:     (key, localPath) =>
      Effect.tryPromise({
        try:   () => fs.promises.readFile(localPath).then(buf => { uploads.set(key, buf) }),
        catch: (e) => new R2UploadError({ key, cause: e }),
      }),
    uploadBuffer:   (key, data) => Effect.sync(() => { uploads.set(key, data) }),
    getPresignedUrl: (key)       => Effect.succeed(`https://r2-stub.local/${key}`),
  })
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/layers/R2Layer.test.ts
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/layers/R2Layer.ts src/layers/R2Layer.test.ts
git commit -m "feat: add R2Service layer with Live and Stub variants"
```

---

## Task 8: EC2 Metadata Layer

**Files:**
- Create: `src/layers/EC2MetadataLayer.ts`
- Create: `src/layers/EC2MetadataLayer.test.ts`

**Interfaces:**
- Consumes: `EC2MetadataError`
- Produces:
  - `EC2MetadataService` tag with `getInstanceId`, `getInstanceType`
  - `EC2MetadataServiceLive` (IMDSv2 via fetch)
  - `EC2MetadataServiceStub` (returns hardcoded values)

- [ ] **Step 1: Write the failing test**

```typescript
// src/layers/EC2MetadataLayer.test.ts
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/layers/EC2MetadataLayer.test.ts
```

Expected: FAIL

- [ ] **Step 3: Create src/layers/EC2MetadataLayer.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import { EC2MetadataError } from "../errors/index.js"

const IMDS_BASE = "http://169.254.169.254"
const TOKEN_TTL = "21600"

export interface EC2MetadataServiceShape {
  getInstanceId():   Effect.Effect<string, EC2MetadataError>
  getInstanceType(): Effect.Effect<string, EC2MetadataError>
}

export class EC2MetadataService extends Context.Tag("EC2MetadataService")<
  EC2MetadataService,
  EC2MetadataServiceShape
>() {}

function fetchImds(path: string): Effect.Effect<string, EC2MetadataError> {
  return Effect.tryPromise({
    try: async () => {
      const tokenRes = await fetch(`${IMDS_BASE}/latest/api/token`, {
        method:  "PUT",
        headers: { "X-aws-ec2-metadata-token-ttl-seconds": TOKEN_TTL },
      })
      const token = await tokenRes.text()
      const res = await fetch(`${IMDS_BASE}${path}`, {
        headers: { "X-aws-ec2-metadata-token": token },
      })
      return res.text()
    },
    catch: (e) => new EC2MetadataError({ field: path, cause: e }),
  })
}

export const EC2MetadataServiceLive: Layer.Layer<EC2MetadataService, EC2MetadataError> =
  Layer.succeed(EC2MetadataService, {
    getInstanceId:   () => fetchImds("/latest/meta-data/instance-id"),
    getInstanceType: () => fetchImds("/latest/meta-data/instance-type"),
  })

export const EC2MetadataServiceStub: Layer.Layer<EC2MetadataService> =
  Layer.succeed(EC2MetadataService, {
    getInstanceId:   () => Effect.succeed("i-test-instance"),
    getInstanceType: () => Effect.succeed("t3.medium"),
  })
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/layers/EC2MetadataLayer.test.ts
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/layers/EC2MetadataLayer.ts src/layers/EC2MetadataLayer.test.ts
git commit -m "feat: add EC2MetadataService with IMDSv2 Live and Stub"
```

---

## Task 9: Cost Layer

**Files:**
- Create: `src/layers/CostLayer.ts`
- Create: `src/layers/CostLayer.test.ts`

**Interfaces:**
- Consumes: `ConfigService`, `CostCalculationError`, `JobCostReport`
- Produces:
  - `CostService` tag with `calculateJobCost(instanceType, startedAt, endedAt): Effect<JobCostReport, CostCalculationError>`
  - `CostServiceLive` (queries EC2 DescribeSpotPriceHistory)
  - `CostServiceStub` (returns fixed cost)

- [ ] **Step 1: Write the failing test**

```typescript
// src/layers/CostLayer.test.ts
import { describe, it, expect } from "vitest"
import { Effect } from "effect"
import { CostService, CostServiceStub } from "./CostLayer.js"

describe("CostService Stub", () => {
  it("returns a deterministic cost report", async () => {
    const start = new Date("2024-01-01T00:00:00Z")
    const end   = new Date("2024-01-01T00:30:00Z") // 30 min

    const report = await Effect.runPromise(
      Effect.gen(function* () {
        const cost = yield* CostService
        return yield* cost.calculateJobCost("t3.medium", start, end)
      }).pipe(Effect.provide(CostServiceStub))
    )

    expect(report.currency).toBe("USD")
    expect(report.durationHours).toBeCloseTo(0.5, 2)
    expect(report.instanceType).toBe("t3.medium")
    expect(typeof report.costCents).toBe("number")
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/layers/CostLayer.test.ts
```

Expected: FAIL

- [ ] **Step 3: Create src/layers/CostLayer.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import { EC2Client, DescribeSpotPriceHistoryCommand } from "@aws-sdk/client-ec2"
import { ConfigService } from "../config/index.js"
import { CostCalculationError } from "../errors/index.js"
import type { JobCostReport } from "../types/index.js"

export interface CostServiceShape {
  calculateJobCost(
    instanceType: string,
    startedAt:    Date,
    endedAt:      Date
  ): Effect.Effect<JobCostReport, CostCalculationError>
}

export class CostService extends Context.Tag("CostService")<
  CostService,
  CostServiceShape
>() {}

export const CostServiceLive = Layer.effect(
  CostService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new EC2Client({ region: config.AWS_REGION })

    return {
      calculateJobCost: (instanceType, startedAt, endedAt) =>
        Effect.tryPromise({
          try: async () => {
            const res = await client.send(new DescribeSpotPriceHistoryCommand({
              InstanceTypes:       [instanceType as any],
              ProductDescriptions: ["Linux/UNIX"],
              StartTime:           startedAt,
              EndTime:             endedAt,
              MaxResults:          1,
            }))
            const price = parseFloat(res.SpotPriceHistory?.[0]?.SpotPrice ?? "0")
            const durationHours = (endedAt.getTime() - startedAt.getTime()) / 3_600_000
            return {
              costCents:           Math.round(price * durationHours * 100),
              spotPricePerHourUsd: price,
              durationHours,
              instanceType,
              currency: "USD" as const,
            }
          },
          catch: (e) => new CostCalculationError({ cause: e }),
        }),
    }
  })
)

const STUB_SPOT_PRICE_USD = 0.0416 // t3.medium on-demand fallback

export const CostServiceStub: Layer.Layer<CostService> = Layer.succeed(CostService, {
  calculateJobCost: (instanceType, startedAt, endedAt) =>
    Effect.sync(() => {
      const durationHours = (endedAt.getTime() - startedAt.getTime()) / 3_600_000
      return {
        costCents:           Math.round(STUB_SPOT_PRICE_USD * durationHours * 100),
        spotPricePerHourUsd: STUB_SPOT_PRICE_USD,
        durationHours,
        instanceType,
        currency: "USD" as const,
      }
    }),
})
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/layers/CostLayer.test.ts
```

Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add src/layers/CostLayer.ts src/layers/CostLayer.test.ts
git commit -m "feat: add CostService with DescribeSpotPriceHistory Live and Stub"
```

---

## Task 10: FFmpeg Layer

**Files:**
- Create: `src/layers/FFmpegLayer.ts`

**Interfaces:**
- Produces: `FFmpegService` tag with `getBinaryPath(): Effect<string>`, `getPreset(): Effect<string>`
- Consumes: `ConfigService`

- [ ] **Step 1: Create src/layers/FFmpegLayer.ts**

```typescript
import { Context, Effect, Layer } from "effect"
import { execaCommand } from "execa"
import { ConfigService } from "../config/index.js"
import { FFmpegError } from "../errors/index.js"

export interface FFmpegServiceShape {
  getBinaryPath(): Effect.Effect<string, FFmpegError>
  getPreset():     Effect.Effect<string>
}

export class FFmpegService extends Context.Tag("FFmpegService")<
  FFmpegService,
  FFmpegServiceShape
>() {}

export const FFmpegServiceLive = Layer.effect(
  FFmpegService,
  Effect.gen(function* () {
    const config = yield* ConfigService

    // Verify ffmpeg is accessible at startup (fail fast).
    const binaryPath = yield* Effect.tryPromise({
      try: async () => {
        const { stdout } = await execaCommand("which ffmpeg")
        return stdout.trim()
      },
      catch: (e) => new FFmpegError({ resolution: "n/a", exitCode: null, stderr: String(e) }),
    })

    return {
      getBinaryPath: () => Effect.succeed(binaryPath),
      getPreset:     () => Effect.succeed(config.FFMPEG_PRESET),
    }
  })
)
```

- [ ] **Step 2: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/layers/FFmpegLayer.ts
git commit -m "feat: add FFmpegService layer that resolves binary path at startup"
```

---

## Task 11: SQS Heartbeat

**Files:**
- Create: `src/sqs/heartbeat.ts`
- Create: `src/sqs/heartbeat.test.ts`

**Interfaces:**
- Consumes: `SQSService`, `HeartbeatError`
- Produces: `forkSQSHeartbeat(receiptHandle, visibilityTimeoutSec, intervalMs): Effect<Fiber<void, HeartbeatError>, never, SQSService>`

- [ ] **Step 1: Write the failing test**

```typescript
// src/sqs/heartbeat.test.ts
import { describe, it, expect } from "vitest"
import { Effect, Fiber, TestClock, Duration } from "effect"
import { forkSQSHeartbeat } from "./heartbeat.js"
import { SQSService } from "../layers/SQSLayer.js"

describe("forkSQSHeartbeat", () => {
  it("calls extendVisibility after the interval elapses", async () => {
    const calls: Array<{ handle: string; timeout: number }> = []

    const mockSQS = Effect.provideService(SQSService, {
      receiveMessage:   () => Effect.succeed(null),
      deleteMessage:    () => Effect.succeed(undefined),
      extendVisibility: (handle, timeout) =>
        Effect.sync(() => { calls.push({ handle, timeout }) }),
    })

    await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* forkSQSHeartbeat("rh-test", 300, 240_000)
        yield* TestClock.adjust(Duration.millis(240_001))
        yield* Fiber.interrupt(fiber)
      }).pipe(
        mockSQS,
        Effect.provide(TestClock.layer)
      )
    )

    expect(calls.length).toBeGreaterThanOrEqual(1)
    expect(calls[0]).toEqual({ handle: "rh-test", timeout: 300 })
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npx vitest run src/sqs/heartbeat.test.ts
```

Expected: FAIL

- [ ] **Step 3: Create src/sqs/heartbeat.ts**

```typescript
import { Effect, Fiber, Schedule, Duration } from "effect"
import { SQSService } from "../layers/SQSLayer.js"
import { HeartbeatError } from "../errors/index.js"

// Forks a daemon fiber that extends SQS message visibility on a repeating
// schedule. The fiber runs indefinitely until interrupted by the caller.
// The caller MUST interrupt this fiber (even on success) to avoid leaking it.
export function forkSQSHeartbeat(
  receiptHandle:      string,
  visibilityTimeout:  number,
  intervalMs:         number
): Effect.Effect<Fiber.RuntimeFiber<void, HeartbeatError>, never, SQSService> {
  const heartbeat = Effect.gen(function* () {
    const sqs = yield* SQSService
    yield* sqs.extendVisibility(receiptHandle, visibilityTimeout).pipe(
      Effect.mapError((e) => new HeartbeatError({ cause: e }))
    )
  }).pipe(
    Effect.repeat(Schedule.spaced(Duration.millis(intervalMs)))
  )

  return Effect.forkDaemon(heartbeat)
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
npx vitest run src/sqs/heartbeat.test.ts
```

Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add src/sqs/heartbeat.ts src/sqs/heartbeat.test.ts
git commit -m "feat: add SQS heartbeat fiber that extends visibility on schedule"
```

---

## Task 12: Resolution Pipeline

**Files:**
- Create: `src/pipelines/resolution.pipeline.ts`

**Interfaces:**
- Consumes: `SQSService`, `SNSService`, `R2Service`, `FFmpegService`, `FFmpegError`, `R2UploadError`, `SNSPublishError`
- Consumes types: `Resolution`, `ResolutionConfig`, `RESOLUTION_CONFIGS`, `ResolutionProgress`, `JobState`
- Produces: `transcodeResolution(resolution, inputPath, outputDir, jobState): Effect<ResolutionProgress, FFmpegError | R2UploadError | SNSPublishError, SQSService | SNSService | R2Service | FFmpegService>`

- [ ] **Step 1: Create src/pipelines/resolution.pipeline.ts**

```typescript
import { Effect, Stream, Fiber } from "effect"
import path from "node:path"
import fs from "node:fs/promises"
import { execa } from "execa"
import { SQSService } from "../layers/SQSLayer.js"
import { SNSService } from "../layers/SNSLayer.js"
import { R2Service } from "../layers/R2Layer.js"
import { FFmpegService } from "../layers/FFmpegLayer.js"
import { FFmpegError, R2UploadError } from "../errors/index.js"
import { watchDirectory } from "../utils/chokidar-stream.js"
import { parseProgressLine } from "../utils/ffmpeg-progress.js"
import type { Resolution, ResolutionProgress, JobState } from "../types/index.js"
import { RESOLUTION_CONFIGS } from "../types/index.js"

export function transcodeResolution(
  resolution:  Resolution,
  inputPath:   string,
  outputDir:   string,
  jobState:    JobState,
): Effect.Effect<
  ResolutionProgress,
  FFmpegError | R2UploadError,
  SNSService | R2Service | FFmpegService
> {
  return Effect.gen(function* () {
    const ffmpeg  = yield* FFmpegService
    const sns     = yield* SNSService
    const r2      = yield* R2Service
    const binary  = yield* ffmpeg.getBinaryPath()
    const preset  = yield* ffmpeg.getPreset()
    const cfg     = RESOLUTION_CONFIGS[resolution]
    const segDir  = path.join(outputDir, resolution)
    const m3u8    = path.join(segDir, "index.m3u8")

    yield* Effect.promise(() => fs.mkdir(segDir, { recursive: true }))

    let segmentsUploaded = 0
    let segmentsTotal    = 0
    let transcodePct     = 0

    // Fork chokidar watcher → upload each new .ts segment to R2 as it appears.
    const watchFiber = yield* Effect.forkDaemon(
      watchDirectory(segDir).pipe(
        Stream.mapEffect((segPath) =>
          Effect.gen(function* () {
            const key = `transcoded/${jobState.jobId}/${resolution}/${path.basename(segPath)}`
            yield* r2.uploadFile(key, segPath)
            segmentsUploaded++
            segmentsTotal = Math.max(segmentsTotal, segmentsUploaded)
          })
        ),
        Stream.runDrain
      )
    )

    // Run FFmpeg transcode process.
    yield* Effect.tryPromise({
      try: () =>
        execa(binary, [
          "-i",    inputPath,
          "-vf",   `scale=${cfg.scale}`,
          "-c:v",  "libx264",
          "-preset", preset,
          "-crf",  String(cfg.crf),
          "-c:a",  "aac",
          "-b:a",  cfg.audioBitrate,
          "-f",    "hls",
          "-hls_time",        "6",
          "-hls_list_size",   "0",
          "-hls_segment_filename", path.join(segDir, "seg_%04d.ts"),
          m3u8,
        ], {
          all: true,
          reject: false,
        }).then((result) => {
          if (result.exitCode !== 0) {
            throw Object.assign(new Error("ffmpeg failed"), {
              exitCode: result.exitCode,
              stderr:   result.stderr,
            })
          }
          // Parse final progress from stderr output.
          const lines = (result.stderr ?? "").split("\n")
          for (const line of lines.reverse()) {
            const pct = parseProgressLine(line, jobState.videoDurationSec)
            if (pct !== null) { transcodePct = pct; break }
          }
        }),
      catch: (e: any) =>
        new FFmpegError({
          resolution,
          exitCode: e.exitCode ?? null,
          stderr:   e.stderr ?? String(e),
        }),
    })

    // Wait for all uploads to drain, then interrupt watcher.
    yield* Fiber.interrupt(watchFiber)

    // Upload the per-resolution index.m3u8.
    const m3u8Key = `transcoded/${jobState.jobId}/${resolution}/index.m3u8`
    yield* r2.uploadBuffer(
      m3u8Key,
      yield* Effect.promise(() => fs.readFile(m3u8))
    )

    // Publish progress event.
    const overallPct = Math.round(
      Object.values(jobState.progress).reduce((sum, p) => sum + p.transcodePct, 0) / 3
    )
    yield* sns.publish({
      type:             "job.progress",
      jobId:            jobState.jobId,
      resolution,
      transcodePct:     100,
      segmentsUploaded,
      overallPct,
      timestamp:        new Date().toISOString(),
    })

    return {
      status:           "done",
      transcodePct:     100,
      segmentsUploaded,
      segmentsTotal,
    } satisfies ResolutionProgress
  })
}
```

- [ ] **Step 2: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/pipelines/resolution.pipeline.ts
git commit -m "feat: add resolution pipeline (FFmpeg HLS transcode + real-time R2 upload)"
```

---

## Task 13: Thumbnail Pipeline

**Files:**
- Create: `src/pipelines/thumbnail.pipeline.ts`

**Interfaces:**
- Consumes: `R2Service`, `FFmpegService`, `FFmpegError`, `R2UploadError`
- Produces: `extractThumbnail(inputPath, outputDir, jobId, durationSec): Effect<string, FFmpegError | R2UploadError, R2Service | FFmpegService>` — returns public R2 URL of thumbnail

- [ ] **Step 1: Create src/pipelines/thumbnail.pipeline.ts**

```typescript
import { Effect } from "effect"
import path from "node:path"
import fs from "node:fs/promises"
import { execa } from "execa"
import { R2Service } from "../layers/R2Layer.js"
import { FFmpegService } from "../layers/FFmpegLayer.js"
import { FFmpegError, R2UploadError } from "../errors/index.js"

export function extractThumbnail(
  inputPath:   string,
  outputDir:   string,
  jobId:       string,
  durationSec: number,
): Effect.Effect<string, FFmpegError | R2UploadError, R2Service | FFmpegService> {
  return Effect.gen(function* () {
    const ffmpeg  = yield* FFmpegService
    const r2      = yield* R2Service
    const binary  = yield* ffmpeg.getBinaryPath()
    const thumbPath = path.join(outputDir, "thumbnail.jpg")
    const midpoint  = Math.floor(durationSec / 2)

    yield* Effect.tryPromise({
      try: () =>
        execa(binary, [
          "-ss", String(midpoint),
          "-i",  inputPath,
          "-vframes", "1",
          "-q:v",     "2",
          thumbPath,
        ]).then((result) => {
          if (result.exitCode !== 0) throw Object.assign(new Error("thumbnail failed"), result)
        }),
      catch: (e: any) =>
        new FFmpegError({ resolution: "thumbnail", exitCode: e.exitCode ?? null, stderr: e.stderr ?? String(e) }),
    })

    const key = `transcoded/${jobId}/thumbnail.jpg`
    yield* r2.uploadBuffer(key, yield* Effect.promise(() => fs.readFile(thumbPath)))

    const r2Service = yield* R2Service
    return yield* r2Service.getPresignedUrl(key).pipe(
      Effect.map(() => `transcoded/${jobId}/thumbnail.jpg`)
    )
  })
}
```

- [ ] **Step 2: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/pipelines/thumbnail.pipeline.ts
git commit -m "feat: add thumbnail extraction pipeline"
```

---

## Task 14: Master Pipeline + Integration Tests

**Files:**
- Create: `src/pipelines/master.pipeline.ts`
- Create: `src/pipelines/master.pipeline.test.ts`
- Create: `test-fixtures/` directory with a short test video (generated in this task)

**Interfaces:**
- Consumes: all service layers, all pipelines, `JobPayload`, `JobState`, `OutputManifest`, `JobCostReport`
- Produces: `transcodeJob(payload: JobPayload, receiptHandle: string): Effect<OutputManifest, AppError, AppDeps>`

- [ ] **Step 1: Generate a short test fixture video**

```bash
mkdir -p test-fixtures
ffmpeg -f lavfi -i testsrc=duration=8:size=1920x1080:rate=30 \
  -f lavfi -i sine=frequency=440:duration=8 \
  -c:v libx264 -crf 28 -c:a aac -b:a 96k \
  test-fixtures/short.mkv
```

Expected: `test-fixtures/short.mkv` created (≈500KB, 8 seconds).

- [ ] **Step 2: Write the failing integration test**

```typescript
// src/pipelines/master.pipeline.test.ts
import { describe, it, expect } from "vitest"
import { Effect, Layer } from "effect"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { transcodeJob } from "./master.pipeline.js"
import { SQSServiceStub } from "../layers/SQSLayer.js"
import { SNSServiceCollecting } from "../layers/SNSLayer.js"
import { R2ServiceStub } from "../layers/R2Layer.js"
import { EC2MetadataServiceStub } from "../layers/EC2MetadataLayer.js"
import { CostServiceStub } from "../layers/CostLayer.js"
import { FFmpegServiceLive } from "../layers/FFmpegLayer.js"
import { ConfigService } from "../config/index.js"
import type { SNSEvent } from "../types/index.js"

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const FIXTURE_VIDEO = path.resolve(__dirname, "../../test-fixtures/short.mkv")

const testConfig = {
  NODE_ENV:                   "test" as const,
  AWS_REGION:                 "us-east-1",
  SQS_QUEUE_URL:              "https://sqs.test",
  SQS_VISIBILITY_TIMEOUT_SEC: 300,
  SQS_HEARTBEAT_INTERVAL_MS:  240_000,
  SNS_TOPIC_ARN:              "arn:aws:sns:test",
  R2_ACCOUNT_ID:              "test",
  R2_ACCESS_KEY_ID:           "test",
  R2_SECRET_ACCESS_KEY:       "test",
  R2_BUCKET_NAME:             "test-bucket",
  R2_PUBLIC_BASE_URL:         "https://r2-test.local",
  TEMP_DIR:                   "/tmp/transcoder-test",
  FFMPEG_PRESET:              "ultrafast" as const,
  RESOLUTION_BATCH_SIZE:      2,
  LOG_LEVEL:                  "silent",
}

describe("transcodeJob", () => {
  it("produces OutputManifest with all three resolutions", async () => {
    const publishedEvents: SNSEvent[] = []
    const uploads = new Map<string, Buffer>()

    const testLayers = Layer.mergeAll(
      SQSServiceStub({ messages: [] }),
      SNSServiceCollecting(publishedEvents),
      R2ServiceStub(uploads),
      EC2MetadataServiceStub,
      CostServiceStub,
      FFmpegServiceLive,
      Layer.succeed(ConfigService, testConfig)
    )

    const payload = {
      id:           "test-job-01",
      inputUrl:     FIXTURE_VIDEO,
      outputPrefix: "test-job-01",
      callbackData: null,
    }

    const result = await Effect.runPromise(
      transcodeJob(payload, "receipt-handle-test").pipe(
        Effect.provide(testLayers)
      )
    )

    // OutputManifest shape
    expect(result.masterPlaylistUrl).toMatch(/master\.m3u8/)
    expect(result.thumbnailUrl).toMatch(/thumbnail\.jpg/)
    expect(result.resolutions).toHaveProperty("480p")
    expect(result.resolutions).toHaveProperty("720p")
    expect(result.resolutions).toHaveProperty("1080p")
    expect(result.durationSec).toBeGreaterThan(0)
    expect(result.processingMs).toBeGreaterThan(0)
    expect(result.cost.currency).toBe("USD")

    // SNS events published in correct order
    const eventTypes = publishedEvents.map((e) => e.type)
    expect(eventTypes[0]).toBe("job.started")
    expect(eventTypes[eventTypes.length - 1]).toBe("job.completed")
    expect(eventTypes).toContain("job.progress")
  }, 120_000)
})
```

- [ ] **Step 3: Run test to verify it fails**

```bash
npx vitest run src/pipelines/master.pipeline.test.ts
```

Expected: FAIL — `Cannot find module './master.pipeline.js'`

- [ ] **Step 4: Create src/pipelines/master.pipeline.ts**

```typescript
import { Effect, Array as Arr } from "effect"
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
import { transcodeResolution } from "./resolution.pipeline.js"
import { extractThumbnail } from "./thumbnail.pipeline.js"
import { probeVideoDuration } from "../utils/ffmpeg-progress.js"
import type {
  JobPayload, OutputManifest, JobState, Resolution,
  ResolutionOutput, ResolutionProgress,
} from "../types/index.js"
import { RESOLUTIONS, RESOLUTION_CONFIGS } from "../types/index.js"
import { JobPayloadParseError } from "../errors/index.js"

const initialProgress = (): ResolutionProgress => ({
  status: "pending", transcodePct: 0, segmentsUploaded: 0, segmentsTotal: 0,
})

export function transcodeJob(
  payload:       JobPayload,
  receiptHandle: string,
): Effect.Effect<OutputManifest, never, SQSService | SNSService | R2Service | EC2MetadataService | CostService | FFmpegService | ConfigService> {
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
    const heartbeatFiber = yield* forkSQSHeartbeat(
      receiptHandle,
      config.SQS_VISIBILITY_TIMEOUT_SEC,
      config.SQS_HEARTBEAT_INTERVAL_MS
    )

    // Process resolutions in batches of RESOLUTION_BATCH_SIZE.
    const batches: Resolution[][] = Arr.chunksOf(RESOLUTIONS, config.RESOLUTION_BATCH_SIZE)

    const resolutionOutputs: Record<string, ResolutionOutput> = {}

    for (const batch of batches) {
      const results = yield* Effect.forEach(
        batch,
        (resolution) =>
          transcodeResolution(resolution, payload.inputUrl, workDir, jobState).pipe(
            Effect.map((progress) => ({ resolution, progress }))
          ),
        { concurrency: config.RESOLUTION_BATCH_SIZE }
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

    // Upload master.m3u8 and thumbnail in parallel.
    const masterKey = `transcoded/${payload.id}/master.m3u8`
    const [thumbnailKey] = yield* Effect.all([
      extractThumbnail(payload.inputUrl, workDir, payload.id, durationSec),
    ], { concurrency: 2 })

    const r2 = yield* R2Service
    yield* r2.uploadBuffer(masterKey, Buffer.from(masterLines, "utf-8"))

    // Calculate cost.
    const endedAt     = new Date()
    const costReport  = yield* cost.calculateJobCost(instanceType, startedAt, endedAt)

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
    yield* Effect.Fiber.interrupt(heartbeatFiber)
    yield* Effect.promise(() => fs.rm(workDir, { recursive: true, force: true }))

    return manifest
  }).pipe(
    Effect.catchAllCause((cause) =>
      Effect.gen(function* () {
        const config     = yield* ConfigService
        const sns        = yield* SNSService
        const ec2        = yield* EC2MetadataService
        const workDir    = path.join(config.TEMP_DIR, payload.id)
        const instanceId = yield* ec2.getInstanceId().pipe(Effect.orElseSucceed(() => "unknown"))

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
      })
    ),
    Effect.orDie
  )
}
```

- [ ] **Step 5: Run the integration test**

```bash
npx vitest run src/pipelines/master.pipeline.test.ts
```

Expected: PASS (1 test, may take 60–90 seconds).

- [ ] **Step 6: Commit**

```bash
git add src/pipelines/ test-fixtures/.gitkeep
git commit -m "feat: add master pipeline and integration test with real FFmpeg"
```

---

## Task 15: Bootstrap + Entry Points

**Files:**
- Create: `src/bootstrap.ts`
- Create: `src/main.ts`
- Modify: `src/manual.ts`

**Interfaces:**
- Consumes: all layers
- Produces: production entry point + local dev entry point

- [ ] **Step 1: Create src/bootstrap.ts**

```typescript
import { Layer } from "effect"
import { ConfigServiceLive } from "./config/index.js"
import { SQSServiceLive } from "./layers/SQSLayer.js"
import { SNSServiceLive } from "./layers/SNSLayer.js"
import { R2ServiceLive } from "./layers/R2Layer.js"
import { EC2MetadataServiceLive } from "./layers/EC2MetadataLayer.js"
import { CostServiceLive } from "./layers/CostLayer.js"
import { FFmpegServiceLive } from "./layers/FFmpegLayer.js"
import { SQSServiceStub } from "./layers/SQSLayer.js"
import { SNSServiceConsoleLive } from "./layers/SNSLayer.js"
import { R2ServiceStub } from "./layers/R2Layer.js"
import { EC2MetadataServiceStub } from "./layers/EC2MetadataLayer.js"
import { CostServiceStub } from "./layers/CostLayer.js"

// All live layers, requiring full env vars and real AWS/R2/EC2 access.
export const AppLayer = Layer.mergeAll(
  ConfigServiceLive,
  SQSServiceLive,
  SNSServiceLive,
  R2ServiceLive,
  EC2MetadataServiceLive,
  CostServiceLive,
  FFmpegServiceLive,
)

// Stub layers for manual/local development — no cloud credentials required.
export const ManualAppLayer = Layer.mergeAll(
  ConfigServiceLive,
  SQSServiceStub({ messages: [] }),
  SNSServiceConsoleLive,
  R2ServiceStub(new Map()),
  EC2MetadataServiceStub,
  CostServiceStub,
  FFmpegServiceLive,
)
```

- [ ] **Step 2: Create src/main.ts**

```typescript
import { Effect, Schema } from "effect"
import { transcodeJob } from "./pipelines/master.pipeline.js"
import { SQSService } from "./layers/SQSLayer.js"
import { JobPayloadSchema } from "./types/index.js"
import { AppLayer } from "./bootstrap.js"
import { JobPayloadParseError } from "./errors/index.js"

const main = Effect.gen(function* () {
  const sqs = yield* SQSService

  // Long-poll SQS for a single job.
  const message = yield* sqs.receiveMessage()
  if (!message) {
    console.log("No message received — exiting.")
    process.exit(0)
  }

  // Parse and validate job payload.
  const payload = yield* Effect.try({
    try: () => Schema.decodeUnknownSync(JobPayloadSchema)(JSON.parse(message.body)),
    catch: (e) => new JobPayloadParseError({ raw: message.body, cause: e }),
  })

  yield* transcodeJob(payload, message.receiptHandle)
})

Effect.runPromise(main.pipe(Effect.provide(AppLayer)))
  .then(() => {
    console.log("Job completed successfully.")
    process.exit(0)
  })
  .catch((e) => {
    console.error("Job failed:", e)
    process.exit(1)
  })
```

- [ ] **Step 3: Update src/manual.ts**

```typescript
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
```

- [ ] **Step 4: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: exits 0.

- [ ] **Step 5: Run all tests**

```bash
npx vitest run
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/bootstrap.ts src/main.ts src/manual.ts
git commit -m "feat: add bootstrap, main entry point, and updated manual dev entry"
```

---

## Task 16: Dockerfile

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Produces: minimal production Docker image with Node.js 20 + FFmpeg 6.x

- [ ] **Step 1: Create Dockerfile**

```dockerfile
# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

# ── Stage 2: Runner ───────────────────────────────────────────────────────────
FROM node:20-alpine AS runner
WORKDIR /app

# Install FFmpeg 6.x from Alpine apk.
RUN apk add --no-cache ffmpeg

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/build ./build

ENV NODE_ENV=production

CMD ["node", "build/main.js"]
```

- [ ] **Step 2: Create .dockerignore**

```
node_modules
build
.git
.env
.env.*
src
test-fixtures
*.test.ts
.docs
```

- [ ] **Step 3: Verify Docker build (optional — requires Docker)**

```bash
docker build -t encoder:dev .
```

Expected: image builds successfully, `docker run --rm encoder:dev node --version` prints `v20.x.x`.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat: add multi-stage Dockerfile with Node 20 Alpine + FFmpeg"
```

---

## Verification

After all tasks are complete:

1. **All tests pass:**
   ```bash
   npx vitest run --reporter=verbose
   ```
   Expected: green across config, SQS, SNS, R2, EC2 metadata, cost, heartbeat, and master pipeline.

2. **TypeScript compiles cleanly:**
   ```bash
   npx tsc --noEmit
   ```
   Expected: exits 0 with no errors.

3. **Manual run succeeds locally** (requires FFmpeg and `.env` with real or test credentials):
   ```bash
   npm run manual
   ```
   Expected: logs `[manual] Completed:` with an OutputManifest containing `masterPlaylistUrl`, `thumbnailUrl`, and three resolutions.

4. **Docker image builds:**
   ```bash
   docker build -t encoder:latest .
   ```
   Expected: exits 0.
