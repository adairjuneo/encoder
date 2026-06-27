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
