# Video Transcoding Service — Engineering Specification v3

> **Revisão v3** — incorpora aprendizados de um projeto similar já validado em produção:
> (1) cálculo de custo real por job via AWS Spot Price History,
> (2) concorrência de resoluções em lotes de 2 (em vez de todas simultâneas),
> reduzindo contenção de CPU durante encoding paralelo a uploads.
> Sem API HTTP. Sem BullMQ. Sem Redis.  
> Um serviço TypeScript puro, orientado a tarefa única, movido por SQS + SNS + Effect.

---

## 1. Visão Geral

O serviço é um **worker de processamento de vídeo de propósito único**. Ele não expõe
nenhuma interface HTTP. Sua única responsabilidade é:

1. Ler um job da fila SQS.
2. Transcodificar o vídeo para HLS nas resoluções `480p`, `720p` e `1080p`.
3. Fazer upload dos segmentos `.ts` para o Cloudflare R2 em tempo real conforme são
   gerados pelo FFmpeg.
4. Publicar eventos de progresso e conclusão no SNS.
5. Encerrar o processo limpo.

O ciclo de vida é **1:1 com a instância EC2** — uma instância processa exatamente um
job e, após a publicação do evento `job.completed` ou `job.failed`, uma Lambda externa
(subscrita ao tópico SNS) termina a instância.

---

## 2. Service Lifecycle

```
[EC2 Boots] ──► [Docker start] ──► [Fetch instanceId from IMDS]
    │
    ▼
[SQS: ReceiveMessage (long-poll, MaxMessages=1)]
    │
    ├─► [Fork SQS Heartbeat Fiber]  ← estende visibility a cada 4min
    │
    ├─► [SNS: job.started]
    │
    ├─► [ffprobe: resolve video duration]
    │
    ├─► [Lote 1] Effect.all concurrency:2
    │       ├── 480p: FFmpeg ──► chokidar ──► R2 upload (real-time)
    │       └── 720p: FFmpeg ──► chokidar ──► R2 upload (real-time)
    │               └── SNS: job.progress por resolução a cada % calculado
    │
    ├─► [Lote 2] Effect.all concurrency:1
    │       └── 1080p: FFmpeg ──► chokidar ──► R2 upload (real-time)
    │               └── SNS: job.progress
    │
    ├─► [Lotes executam em SÉRIE — dentro do lote, resoluções em PARALELO]
    │   (rationale: durante upload de um lote — bound em bandwidth — o CPU
    │    livre já é aproveitado pela encodificação da próxima resolução)
    │
    ├─► [EC2Metadata: getInstanceType]
    ├─► [CostService: DescribeSpotPriceHistory + (endedAt - startedAt)]
    │
    ├─► Effect.all concurrency:2
    │       ├── Build + upload master.m3u8
    │       └── Extract + upload thumbnail.jpg
    │
    ├─► [SQS: DeleteMessage]         ← só em caso de sucesso
    ├─► [Interrupt Heartbeat Fiber]
    ├─► [SNS: job.completed { instanceId }]
    └─► [Process exit 0]

[Em qualquer erro]:
    ├─► [NÃO deletar SQS message]    ← SQS re-entrega após visibility timeout
    ├─► [Interrupt Heartbeat Fiber]
    ├─► [Cleanup /tmp/<jobId>/]
    ├─► [SNS: job.failed { instanceId }]
    └─► [Process exit 1]

[Lambda subscrita ao SNS filtra job.completed | job.failed]
    └─► ec2:TerminateInstances(instanceId)
```

---

## 3. Tech Stack

| Camada               | Tecnologia                                                           |
|----------------------|----------------------------------------------------------------------|
| Runtime              | Node.js 20 LTS                                                       |
| Linguagem            | TypeScript 5.x — `strict: true`, sem `any`                          |
| Effect runtime       | `effect@^3.x`, `@effect/schema`                                      |
| AWS SDK              | `@aws-sdk/client-sqs`, `@aws-sdk/client-sns`, `@aws-sdk/client-ec2`  |
| Storage              | `@aws-sdk/client-s3` + `@aws-sdk/lib-storage` (Cloudflare R2)        |
| Transcoding          | FFmpeg 6.x via `execa@^8`                                            |
| FS watching          | `chokidar@^4` (encapsulado como `Effect.Stream`)                     |
| IDs                  | `ulid`                                                               |
| Logging              | `pino` (JSON estruturado)                                            |
| Testes               | `vitest` + `Effect TestLayer`                                        |
| Container            | Docker multi-stage (`node:20-alpine` + `ffmpeg`)                     |

---

## 4. Estrutura de Diretórios

Projeto **single-package** — sem monorepo, sem API layer.

```
video-transcoder/
├── src/
│   ├── main.ts                    # Entry point produção: SQS → job → exit
│   ├── manual.ts                  # Entry point local: payload hardcoded → job
│   ├── bootstrap.ts               # Constrói AppLayer, ManagedRuntime
│   │
│   ├── types/
│   │   └── index.ts               # JobPayload, JobState, Resolution, SNSEvent, etc.
│   │
│   ├── errors/
│   │   └── index.ts               # Todos os Data.TaggedError do domínio
│   │
│   ├── config/
│   │   └── index.ts               # Lê e valida process.env com @effect/schema
│   │
│   ├── layers/
│   │   ├── R2Layer.ts             # Cloudflare R2 via S3 SDK
│   │   ├── SQSLayer.ts            # AWS SQS (poll, delete, heartbeat)
│   │   ├── SNSLayer.ts            # AWS SNS (publish + MessageAttributes; inclui SNSLayerConsole para dev local)
│   │   ├── FFmpegLayer.ts         # Config do FFmpeg (preset, thread count)
│   │   ├── EC2MetadataLayer.ts    # IMDS v2: busca instanceId + instanceType (Live + Stub)
│   │   └── CostLayer.ts           # Spot Price History → custo real do job (Live + Stub)
│   │
│   ├── utils/
│   │   ├── chokidar-stream.ts     # chokidar "add" → Effect.Stream<string>
│   │   └── ffmpeg-progress.ts     # Parser de stderr do FFmpeg → number (0-100)
│   │
│   ├── pipelines/
│   │   ├── resolution.pipeline.ts # Transcode de 1 resolução + upload real-time
│   │   ├── thumbnail.pipeline.ts  # Extrai e faz upload do thumbnail
│   │   └── master.pipeline.ts     # Orquestra as 3 resoluções + monta master.m3u8
│   │
│   └── sqs/
│       └── heartbeat.ts           # Fiber que estende visibility timeout a cada 4min
│
├── Dockerfile
├── docker-compose.dev.yml
├── .env.example
├── tsconfig.json
└── package.json
```

---

## 5. Environment Variables

```dotenv
# ─── AWS ─────────────────────────────────────────────────────
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=                # Apenas para dev local. Em EC2, usar IAM Role.
AWS_SECRET_ACCESS_KEY=

SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/transcode-jobs
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789:transcode-events

# ─── Cloudflare R2 ───────────────────────────────────────────
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=videos
R2_PUBLIC_BASE_URL=https://pub-xxxxxx.r2.dev

# ─── Worker ──────────────────────────────────────────────────
RESOLUTION_BATCH_SIZE=2           # Quantas resoluções processar simultaneamente por lote.
                                   # Lotes executam em SÉRIE; dentro do lote, em PARALELO.
                                   # Ex: com 3 resoluções e batch=2 → [480p,720p] depois [1080p].
                                   # Rationale: durante upload (bound em bandwidth), o CPU livre
                                   # já é aproveitado pela encodificação da próxima resolução.
FFMPEG_PRESET=veryfast            # ultrafast | veryfast | medium
TEMP_DIR=/tmp/transcoder
SQS_VISIBILITY_TIMEOUT_SEC=300    # 5 min por heartbeat cycle
SQS_HEARTBEAT_INTERVAL_MS=240000  # 4 min — estende antes de expirar

# ─── Logging ─────────────────────────────────────────────────
LOG_LEVEL=info
NODE_ENV=production
```

> **Em EC2**: `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` **não são usados** —
> o SDK resolve credenciais automaticamente via IAM Instance Role.

---

## 6. Types & Schemas

```typescript
// src/types/index.ts
import { Schema as S } from "@effect/schema"

// ─── Resolutions ─────────────────────────────────────────────────────────────

export type Resolution = "480p" | "720p" | "1080p"
export const RESOLUTIONS: Resolution[] = ["480p", "720p", "1080p"]

export interface ResolutionConfig {
  readonly scale:        string   // "1280:720"
  readonly crf:          number
  readonly audioBitrate: string   // "128k"
  readonly bandwidth:    number   // bits/sec para master.m3u8
  readonly codecs:       string   // "avc1.4d001f,mp4a.40.2"
}

export const RESOLUTION_CONFIGS: Record<Resolution, ResolutionConfig> = {
  "480p":  { scale: "854:480",   crf: 23, audioBitrate: "128k", bandwidth: 800_000,   codecs: "avc1.4d001f,mp4a.40.2" },
  "720p":  { scale: "1280:720",  crf: 21, audioBitrate: "128k", bandwidth: 2_800_000, codecs: "avc1.4d001f,mp4a.40.2" },
  "1080p": { scale: "1920:1080", crf: 20, audioBitrate: "192k", bandwidth: 5_000_000, codecs: "avc1.640028,mp4a.40.2" },
}

// ─── Job Payload (recebido do SQS) ───────────────────────────────────────────

export const JobPayloadSchema = S.Struct({
  id:           S.String,                         // ULID gerado pelo publicador
  inputUrl:     S.String.pipe(S.nonEmpty()),       // Presigned URL ou URL pública do vídeo
  outputPrefix: S.String.pipe(S.nonEmpty()),       // Ex: "transcoded/job_01J.../"
  callbackData: S.optional(S.Record(S.String, S.Unknown)),  // Echoed em todos os eventos SNS
})

export type JobPayload = S.Schema.Type<typeof JobPayloadSchema>

// ─── Job State (gerenciado via Ref<JobState> dentro do Effect) ────────────────

export type ResolutionStatus = "pending" | "transcoding" | "uploading" | "completed" | "failed"

export interface ResolutionProgress {
  status:           ResolutionStatus
  transcodePct:     number          // 0-100, parseado do stderr do FFmpeg
  segmentsUploaded: number
  segmentsTotal:    number | null   // null até FFmpeg terminar de escrever
}

export interface JobState {
  jobId:         string
  startedAt:     Date
  instanceId:    string             // EC2 instance ID (ou "local-dev" em manual.ts)
  instanceType:  string             // EC2 instance type, ex: "c8g.16xlarge" (ou "local-dev" em manual.ts)
  receiptHandle: string             // SQS receipt handle — necessário para DeleteMessage
  videoDurationSec: number | null   // resolvido via ffprobe
  totalOutputBytes: number          // soma de todos os bytes enviados ao R2 (segmentos + playlists + thumb)
  progress: Record<Resolution, ResolutionProgress>
}

export const makeInitialJobState = (
  jobId: string,
  instanceId: string,
  instanceType: string,
  receiptHandle: string,
): JobState => ({
  jobId,
  startedAt: new Date(),
  instanceId,
  instanceType,
  receiptHandle,
  videoDurationSec: null,
  totalOutputBytes: 0,
  progress: {
    "480p":  { status: "pending", transcodePct: 0, segmentsUploaded: 0, segmentsTotal: null },
    "720p":  { status: "pending", transcodePct: 0, segmentsUploaded: 0, segmentsTotal: null },
    "1080p": { status: "pending", transcodePct: 0, segmentsUploaded: 0, segmentsTotal: null },
  },
})

// ─── Cost Report (resolvido via AWS Spot Price History) ──────────────────────

export interface JobCostReport {
  costCents:           number   // custo estimado da execução, em centavos de USD
  spotPricePerHourUsd: number   // preço spot vigente no momento da consulta
  durationHours:       number   // duração total do job em horas (decimal)
  instanceType:        string
  currency:            "USD"
}

// ─── Output Manifest (publicado no SNS job.completed) ────────────────────────

export interface OutputManifest {
  masterPlaylistUrl: string
  thumbnailUrl:      string
  resolutions: Record<Resolution, {
    playlistUrl:   string
    segmentCount:  number
    bandwidth:     number
  }>
  durationSec:    number
  processingMs:   number
  outputSizeBytes: number      // soma de todos os bytes gerados (segmentos + playlists + thumb)
  cost:           JobCostReport
}
```

---

## 7. Error Domain

```typescript
// src/errors/index.ts
import { Data } from "effect"

export class FFmpegError extends Data.TaggedError("FFmpegError")<{
  readonly resolution: string
  readonly exitCode:   number | null
  readonly signal:     string | null
  readonly stderr:     string
}> {}

export class FFprobeError extends Data.TaggedError("FFprobeError")<{
  readonly inputUrl: string
  readonly cause:    unknown
}> {}

export class R2UploadError extends Data.TaggedError("R2UploadError")<{
  readonly key:   string
  readonly cause: unknown
}> {}

export class R2PresignError extends Data.TaggedError("R2PresignError")<{
  readonly key:   string
  readonly cause: unknown
}> {}

export class SQSReceiveError extends Data.TaggedError("SQSReceiveError")<{
  readonly cause: unknown
}> {}

export class SQSDeleteError extends Data.TaggedError("SQSDeleteError")<{
  readonly receiptHandle: string
  readonly cause:         unknown
}> {}

export class SQSHeartbeatError extends Data.TaggedError("SQSHeartbeatError")<{
  readonly receiptHandle: string
  readonly cause:         unknown
}> {}

export class SNSPublishError extends Data.TaggedError("SNSPublishError")<{
  readonly eventType: string
  readonly cause:     unknown
}> {}

export class EC2MetadataError extends Data.TaggedError("EC2MetadataError")<{
  readonly cause: unknown
}> {}

export class CostCalculationError extends Data.TaggedError("CostCalculationError")<{
  readonly instanceType: string
  readonly cause:        unknown
}> {}

export class JobPayloadParseError extends Data.TaggedError("JobPayloadParseError")<{
  readonly raw:   string
  readonly cause: unknown
}> {}
```

---

## 8. Layers

### 8.1 Config Layer

```typescript
// src/config/index.ts
import { Schema as S, decodeUnknownSync } from "@effect/schema"
import { Context, Layer, Effect } from "effect"

const ConfigSchema = S.Struct({
  awsRegion:               S.String,
  sqsQueueUrl:             S.String,
  snsTopicArn:             S.String,
  r2AccountId:             S.String,
  r2AccessKeyId:           S.String,
  r2SecretAccessKey:       S.String,
  r2BucketName:            S.String,
  r2PublicBaseUrl:         S.String,
  resolutionBatchSize:     S.NumberFromString.pipe(S.between(1, 3)),
  ffmpegPreset:            S.Literal("ultrafast", "veryfast", "medium"),
  tempDir:                 S.String,
  sqsVisibilityTimeoutSec: S.NumberFromString,
  sqsHeartbeatIntervalMs:  S.NumberFromString,
})

export type Config = S.Schema.Type<typeof ConfigSchema>

export class ConfigService extends Context.Tag("ConfigService")<
  ConfigService, Config
>() {}

export const ConfigLayer = Layer.effect(
  ConfigService,
  Effect.try({
    try: () => decodeUnknownSync(ConfigSchema)({
      awsRegion:               process.env.AWS_REGION,
      sqsQueueUrl:             process.env.SQS_QUEUE_URL,
      snsTopicArn:             process.env.SNS_TOPIC_ARN,
      r2AccountId:             process.env.R2_ACCOUNT_ID,
      r2AccessKeyId:           process.env.R2_ACCESS_KEY_ID,
      r2SecretAccessKey:       process.env.R2_SECRET_ACCESS_KEY,
      r2BucketName:            process.env.R2_BUCKET_NAME,
      r2PublicBaseUrl:         process.env.R2_PUBLIC_BASE_URL,
      resolutionBatchSize:     process.env.RESOLUTION_BATCH_SIZE ?? "2",
      ffmpegPreset:            process.env.FFMPEG_PRESET ?? "veryfast",
      tempDir:                 process.env.TEMP_DIR ?? "/tmp/transcoder",
      sqsVisibilityTimeoutSec: process.env.SQS_VISIBILITY_TIMEOUT_SEC ?? "300",
      sqsHeartbeatIntervalMs:  process.env.SQS_HEARTBEAT_INTERVAL_MS ?? "240000",
    }),
    catch: (e) => new Error(`Invalid config: ${e}`),
  })
)
```

---

### 8.2 SQS Layer

```typescript
// src/layers/SQSLayer.ts
import { Context, Effect, Layer } from "effect"
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} from "@aws-sdk/client-sqs"
import { ConfigService } from "../config/index.js"
import { SQSReceiveError, SQSDeleteError, SQSHeartbeatError } from "../errors/index.js"

export interface ReceivedMessage {
  body:          string
  receiptHandle: string
}

export interface SQSServiceShape {
  /**
   * Long-poll SQS por até 20s. Retorna null se a fila estiver vazia.
   * MaxNumberOfMessages: 1 — garante que pegamos exatamente 1 job.
   */
  receiveMessage: () => Effect.Effect<ReceivedMessage | null, SQSReceiveError>

  /**
   * Deleta a mensagem da fila. Deve ser chamado SOMENTE após processamento
   * bem-sucedido. Em caso de falha, NÃO chamar — SQS re-entrega automaticamente.
   */
  deleteMessage: (receiptHandle: string) => Effect.Effect<void, SQSDeleteError>

  /**
   * Estende o visibility timeout da mensagem. Usado pelo heartbeat fiber.
   */
  extendVisibility: (
    receiptHandle:  string,
    timeoutSeconds: number,
  ) => Effect.Effect<void, SQSHeartbeatError>
}

export class SQSService extends Context.Tag("SQSService")<
  SQSService, SQSServiceShape
>() {}

export const SQSLayer = Layer.effect(
  SQSService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new SQSClient({ region: config.awsRegion })

    return {
      receiveMessage: () =>
        Effect.tryPromise({
          try: async () => {
            const res = await client.send(new ReceiveMessageCommand({
              QueueUrl:            config.sqsQueueUrl,
              MaxNumberOfMessages: 1,
              WaitTimeSeconds:     20,   // long-polling
              VisibilityTimeout:   config.sqsVisibilityTimeoutSec,
            }))
            const msg = res.Messages?.[0]
            if (!msg?.Body || !msg.ReceiptHandle) return null
            return { body: msg.Body, receiptHandle: msg.ReceiptHandle }
          },
          catch: (e) => new SQSReceiveError({ cause: e }),
        }),

      deleteMessage: (receiptHandle) =>
        Effect.tryPromise({
          try: () => client.send(new DeleteMessageCommand({
            QueueUrl:      config.sqsQueueUrl,
            ReceiptHandle: receiptHandle,
          })).then(() => undefined),
          catch: (e) => new SQSDeleteError({ receiptHandle, cause: e }),
        }),

      extendVisibility: (receiptHandle, timeoutSeconds) =>
        Effect.tryPromise({
          try: () => client.send(new ChangeMessageVisibilityCommand({
            QueueUrl:          config.sqsQueueUrl,
            ReceiptHandle:     receiptHandle,
            VisibilityTimeout: timeoutSeconds,
          })).then(() => undefined),
          catch: (e) => new SQSHeartbeatError({ receiptHandle, cause: e }),
        }),
    }
  })
)
```

---

### 8.3 SQS Heartbeat Fiber

```typescript
// src/sqs/heartbeat.ts
import { Effect, Fiber, Schedule } from "effect"
import { SQSService } from "../layers/SQSLayer.js"
import { ConfigService } from "../config/index.js"

/**
 * Faz o fork de um Fiber que estende o visibility timeout da mensagem SQS
 * a cada `heartbeatIntervalMs`. Retorna o Fiber para que o caller possa
 * interrompê-lo ao final do processamento.
 *
 * Uso:
 *   const hbFiber = yield* forkSQSHeartbeat(receiptHandle)
 *   yield* doWork(...)
 *   yield* Fiber.interrupt(hbFiber)
 */
export const forkSQSHeartbeat = (receiptHandle: string) =>
  Effect.gen(function* () {
    const sqs    = yield* SQSService
    const config = yield* ConfigService

    return yield* Effect.fork(
      sqs.extendVisibility(receiptHandle, config.sqsVisibilityTimeoutSec).pipe(
        // Log de warning em caso de falha, mas NÃO propaga o erro —
        // uma falha de heartbeat não deve matar o job inteiro.
        Effect.tapError((e) =>
          Effect.logWarning("SQS heartbeat failed", { error: e })
        ),
        Effect.ignore,
        Effect.repeat(
          Schedule.fixed(config.sqsHeartbeatIntervalMs)
        ),
      )
    )
  })
```

---

### 8.4 SNS Layer

```typescript
// src/layers/SNSLayer.ts
import { Context, Effect, Layer } from "effect"
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns"
import { ConfigService } from "../config/index.js"
import { SNSPublishError } from "../errors/index.js"
import type { SNSEvent } from "../types/index.js"

export interface SNSServiceShape {
  publish: (event: SNSEvent) => Effect.Effect<void, SNSPublishError>
}

export class SNSService extends Context.Tag("SNSService")<
  SNSService, SNSServiceShape
>() {}

export const SNSLayerLive = Layer.effect(
  SNSService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new SNSClient({ region: config.awsRegion })

    return {
      publish: (event) =>
        Effect.tryPromise({
          try: () => client.send(new PublishCommand({
            TopicArn: config.snsTopicArn,
            Message:  JSON.stringify(event),
            // MessageAttributes para filtragem no lado do subscriber
            MessageAttributes: {
              eventType: {
                DataType:    "String",
                StringValue: event.eventType,
              },
              jobId: {
                DataType:    "String",
                StringValue: event.jobId,
              },
            },
          })).then(() => undefined),
          catch: (e) => new SNSPublishError({ eventType: event.eventType, cause: e }),
        }).pipe(
          // Falha de SNS é logada mas NÃO propaga — não deve derrubar o job.
          Effect.tapError((e) =>
            Effect.logError("SNS publish failed", { event: event.eventType, error: e })
          ),
          Effect.ignore,
        ),
    }
  })
)

// Mock para uso em manual.ts — imprime os eventos no stdout sem chamar AWS.
export const SNSLayerConsole = Layer.succeed(SNSService, {
  publish: (event) =>
    Effect.sync(() => {
      console.log(`\n[SNS EVENT] ${event.eventType}`, JSON.stringify(event, null, 2))
    }),
})
```

---

### 8.5 SNS Event Schema

Todos os eventos compartilham um envelope base. O campo `instanceId` em
`job.completed` e `job.failed` é o que a Lambda usa para chamar
`ec2:TerminateInstances`.

```typescript
// (adicionar em src/types/index.ts)

export type SNSEvent =
  | {
      eventType:    "job.started"
      jobId:        string
      instanceId:   string
      inputUrl:     string
      timestamp:    string
      callbackData: Record<string, unknown> | undefined
    }
  | {
      eventType:        "job.progress"
      jobId:            string
      resolution:       Resolution
      transcodePct:     number    // 0-100 para aquela resolução
      segmentsUploaded: number
      overallPct:       number    // média das 3 resoluções
      timestamp:        string
    }
  | {
      eventType:   "job.completed"
      jobId:       string
      instanceId:  string         // EC2 instance ID → Lambda usa para terminate
      output:      OutputManifest // inclui cost: JobCostReport e outputSizeBytes (ver §6)
      timestamp:   string
      callbackData: Record<string, unknown> | undefined
    }
  | {
      eventType:   "job.failed"
      jobId:       string
      instanceId:  string         // EC2 instance ID → Lambda usa para terminate
      error: {
        code:    string
        message: string
      }
      timestamp:   string
      callbackData: Record<string, unknown> | undefined
    }
```

**SNS MessageAttributes** (para filter policies nos subscribers):

| Atributo    | Tipo     | Valores possíveis                                        |
|-------------|----------|----------------------------------------------------------|
| `eventType` | `String` | `job.started`, `job.progress`, `job.completed`, `job.failed` |
| `jobId`     | `String` | qualquer string                                          |

A Lambda de terminate deve ter filter policy:
```json
{ "eventType": ["job.completed", "job.failed"] }
```

---

### 8.6 R2 Layer

```typescript
// src/layers/R2Layer.ts
import { Context, Effect, Layer } from "effect"
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { GetObjectCommand } from "@aws-sdk/client-s3"
import { Upload } from "@aws-sdk/lib-storage"
import { createReadStream } from "node:fs"
import { ConfigService } from "../config/index.js"
import { R2UploadError, R2PresignError } from "../errors/index.js"

export interface R2ServiceShape {
  uploadFile:      (key: string, localPath: string) => Effect.Effect<void, R2UploadError>
  uploadBuffer:    (key: string, body: Buffer | string, contentType: string) => Effect.Effect<void, R2UploadError>
  getPresignedUrl: (key: string, ttlSeconds?: number) => Effect.Effect<string, R2PresignError>
}

export class R2Service extends Context.Tag("R2Service")<
  R2Service, R2ServiceShape
>() {}

export const R2Layer = Layer.effect(
  R2Service,
  Effect.gen(function* () {
    const config = yield* ConfigService

    const client = new S3Client({
      region:   "auto",
      endpoint: `https://${config.r2AccountId}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId:     config.r2AccessKeyId,
        secretAccessKey: config.r2SecretAccessKey,
      },
    })
    const Bucket = config.r2BucketName

    return {
      uploadFile: (key, localPath) =>
        Effect.tryPromise({
          try: async () => {
            const upload = new Upload({
              client,
              params: { Bucket, Key: key, Body: createReadStream(localPath) },
            })
            await upload.done()
          },
          catch: (e) => new R2UploadError({ key, cause: e }),
        }),

      uploadBuffer: (key, body, contentType) =>
        Effect.tryPromise({
          try: () =>
            client.send(new PutObjectCommand({
              Bucket, Key: key, Body: body, ContentType: contentType,
            })).then(() => undefined),
          catch: (e) => new R2UploadError({ key, cause: e }),
        }),

      getPresignedUrl: (key, ttlSeconds = 3600) =>
        Effect.tryPromise({
          try: () =>
            getSignedUrl(client, new GetObjectCommand({ Bucket, Key: key }), {
              expiresIn: ttlSeconds,
            }),
          catch: (e) => new R2PresignError({ key, cause: e }),
        }),
    }
  })
)
```

---

### 8.7 EC2 Metadata Layer (IMDSv2)

```typescript
// src/layers/EC2MetadataLayer.ts
import { Context, Effect, Layer } from "effect"
import { EC2MetadataError } from "../errors/index.js"

export interface EC2MetadataServiceShape {
  /**
   * Retorna o EC2 instance ID via IMDSv2.
   * Em ambiente local (manual.ts), use EC2MetadataLayerStub.
   */
  getInstanceId: () => Effect.Effect<string, EC2MetadataError>

  /**
   * Retorna o EC2 instance type (ex: "c8g.16xlarge") via IMDSv2.
   * Usado pelo CostService para consultar o Spot Price History.
   */
  getInstanceType: () => Effect.Effect<string, EC2MetadataError>
}

export class EC2MetadataService extends Context.Tag("EC2MetadataService")<
  EC2MetadataService, EC2MetadataServiceShape
>() {}

/**
 * Helper interno: busca o token IMDSv2 uma vez e reutiliza para qualquer
 * chamada subsequente de metadata dentro do mesmo Effect.
 */
const fetchImdsToken = (): Effect.Effect<string, EC2MetadataError> =>
  Effect.tryPromise({
    try: async () => {
      const res = await fetch("http://169.254.169.254/latest/api/token", {
        method:  "PUT",
        headers: { "X-aws-ec2-metadata-token-ttl-seconds": "21600" },
      })
      return res.text()
    },
    catch: (e) => new EC2MetadataError({ cause: e }),
  })

const fetchImdsPath = (path: string): Effect.Effect<string, EC2MetadataError> =>
  Effect.gen(function* () {
    const token = yield* fetchImdsToken()
    return yield* Effect.tryPromise({
      try: async () => {
        const res = await fetch(`http://169.254.169.254/latest/meta-data/${path}`, {
          headers: { "X-aws-ec2-metadata-token": token },
        })
        return res.text()
      },
      catch: (e) => new EC2MetadataError({ cause: e }),
    })
  })

export const EC2MetadataLayerLive = Layer.succeed(EC2MetadataService, {
  getInstanceId:   () => fetchImdsPath("instance-id"),
  getInstanceType: () => fetchImdsPath("instance-type"),
})

// Stub para dev local — sem IMDS disponível
export const EC2MetadataLayerStub = Layer.succeed(EC2MetadataService, {
  getInstanceId:   () => Effect.succeed("local-dev"),
  getInstanceType: () => Effect.succeed("local-dev"),
})
```

---

### 8.8 Cost Layer

Calcula o custo real da execução consultando o histórico de preços Spot da AWS
no momento em que o job termina, cruzado com a duração total do processamento.
O resultado é incluído no `OutputManifest` e, portanto, no payload de
`job.completed`.

> **Nota de precisão**: `DescribeSpotPriceHistory` retorna o preço vigente
> *naquele instante* — não o preço médio durante toda a execução do job. Para
> jobs longos (vídeos de 1h+), o preço spot pode variar ao longo da execução.
> Esta é uma estimativa, não um valor exato de billing. Se for necessário
> precisão de centavo, isso exigiria amostragem periódica do preço durante a
> execução — fora do escopo deste MVP.

```typescript
// src/layers/CostLayer.ts
import { Context, Effect, Layer } from "effect"
import { EC2Client, DescribeSpotPriceHistoryCommand } from "@aws-sdk/client-ec2"
import { ConfigService } from "../config/index.js"
import { CostCalculationError } from "../errors/index.js"
import type { JobCostReport } from "../types/index.js"

export interface CalculateCostParams {
  instanceType: string
  startedAt:    Date
  endedAt:      Date
}

export interface CostServiceShape {
  calculateJobCost: (params: CalculateCostParams) => Effect.Effect<JobCostReport, CostCalculationError>
}

export class CostService extends Context.Tag("CostService")<
  CostService, CostServiceShape
>() {}

export const CostLayerLive = Layer.effect(
  CostService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new EC2Client({ region: config.awsRegion })

    return {
      calculateJobCost: ({ instanceType, startedAt, endedAt }) =>
        Effect.tryPromise({
          try: async () => {
            const durationHours = (endedAt.getTime() - startedAt.getTime()) / 1000 / 3600

            const res = await client.send(new DescribeSpotPriceHistoryCommand({
              InstanceTypes:      [instanceType],
              ProductDescriptions: ["Linux/UNIX"],
              StartTime:          endedAt,
              MaxResults:         1,
            }))

            const priceEntry = res.SpotPriceHistory?.[0]
            if (!priceEntry?.SpotPrice) {
              throw new Error(`No spot price data found for ${instanceType}`)
            }

            const spotPricePerHourUsd = parseFloat(priceEntry.SpotPrice)
            const costCents = Math.round(spotPricePerHourUsd * durationHours * 100)

            return {
              costCents,
              spotPricePerHourUsd,
              durationHours,
              instanceType,
              currency: "USD" as const,
            }
          },
          catch: (e) => new CostCalculationError({ instanceType, cause: e }),
        }),
    }
  })
)

// Stub para dev local — sem instância EC2/Spot real, custo sempre 0.
export const CostLayerStub = Layer.succeed(CostService, {
  calculateJobCost: ({ instanceType, startedAt, endedAt }) =>
    Effect.succeed({
      costCents:           0,
      spotPricePerHourUsd: 0,
      durationHours:       (endedAt.getTime() - startedAt.getTime()) / 1000 / 3600,
      instanceType,
      currency:            "USD" as const,
    }),
})
```

---

## 9. Utilities

### 9.1 Chokidar → Effect.Stream

```typescript
// src/utils/chokidar-stream.ts
import { Stream, Effect, Deferred, Scope } from "effect"
import chokidar from "chokidar"

/**
 * Observa um diretório e emite o path absoluto de cada novo arquivo
 * detectado (evento "add"). O stream encerra quando `done` é completado.
 *
 * Importante:
 * - awaitWriteFinish: aguarda o arquivo estar completamente escrito
 *   antes de emitir, evitando uploads de segmentos parciais.
 * - O Scope garante que o watcher é fechado em qualquer cenário
 *   (sucesso, erro ou interrupção do Fiber).
 */
export const watchDirectory = (
  dir:  string,
  done: Deferred.Deferred<void, never>,
): Stream.Stream<string, never, never> =>
  Stream.asyncScoped<string>((emit) =>
    Effect.gen(function* () {
      const watcher = chokidar.watch(dir, {
        persistent:      true,
        ignoreInitial:   true,
        awaitWriteFinish: {
          stabilityThreshold: 200,
          pollInterval:       50,
        },
      })

      watcher.on("add", (filePath) => emit.single(filePath))

      // Quando done resolve, fechar o watcher e encerrar o stream
      yield* Deferred.await(done).pipe(
        Effect.andThen(Effect.promise(() => watcher.close())),
        Effect.andThen(emit.end()),
        Effect.forkScoped,
      )

      yield* Scope.addFinalizer(
        Effect.promise(() => watcher.close())
      )
    })
  )
```

---

### 9.2 FFmpeg Progress Parser

```typescript
// src/utils/ffmpeg-progress.ts
import { execa } from "execa"
import { Effect } from "effect"
import { FFprobeError } from "../errors/index.js"

/**
 * Parseia uma linha do stderr do FFmpeg para extrair o percentual de progresso.
 * Retorna null se a linha não for uma linha de progresso.
 *
 * Formato esperado: "...time=00:01:23.45..."
 */
export const parseProgressLine = (
  line:        string,
  durationSec: number,
): number | null => {
  const match = line.match(/time=(\d{2}):(\d{2}):(\d{2}\.\d+)/)
  if (!match) return null
  const [, h, m, s] = match
  const currentSec = Number(h) * 3600 + Number(m) * 60 + parseFloat(s)
  return Math.min(100, Math.round((currentSec / durationSec) * 100))
}

/**
 * Executa ffprobe para obter a duração do vídeo em segundos.
 * ffprobe é instalado junto com o pacote ffmpeg.
 */
export const probeVideoDuration = (inputUrl: string): Effect.Effect<number, FFprobeError> =>
  Effect.tryPromise({
    try: async () => {
      const result = await execa("ffprobe", [
        "-v",             "error",
        "-show_entries",  "format=duration",
        "-of",            "csv=p=0",
        inputUrl,
      ])
      const duration = parseFloat(result.stdout.trim())
      if (isNaN(duration)) throw new Error(`Invalid duration: ${result.stdout}`)
      return duration
    },
    catch: (e) => new FFprobeError({ inputUrl, cause: e }),
  })
```

---

## 10. Pipelines

### 10.1 Resolution Pipeline

Cada resolução executa dois Fibers concorrentes:
- **Fiber A**: FFmpeg escrevendo segmentos em `/tmp/<jobId>/<res>/`
- **Fiber B**: chokidar observando o diretório e fazendo upload de cada `.ts` para R2 em tempo real

```typescript
// src/pipelines/resolution.pipeline.ts
import { Effect, Fiber, Deferred, Schedule, Stream, Ref } from "effect"
import path from "node:path"
import fs from "node:fs/promises"
import { execa } from "execa"
import { R2Service } from "../layers/R2Layer.js"
import { SNSService } from "../layers/SNSLayer.js"
import { ConfigService } from "../config/index.js"
import { watchDirectory } from "../utils/chokidar-stream.js"
import { parseProgressLine } from "../utils/ffmpeg-progress.js"
import { FFmpegError, R2UploadError } from "../errors/index.js"
import {
  RESOLUTION_CONFIGS, RESOLUTIONS,
  type Resolution, type JobPayload, type JobState,
} from "../types/index.js"

export const transcodeResolution = (
  payload:    JobPayload,
  resolution: Resolution,
  stateRef:   Ref.Ref<JobState>,
) =>
  Effect.gen(function* () {
    const r2     = yield* R2Service
    const sns    = yield* SNSService
    const config = yield* ConfigService
    const cfg    = RESOLUTION_CONFIGS[resolution]
    const state  = yield* Ref.get(stateRef)
    const outDir = path.join(config.tempDir, payload.id, resolution)

    yield* Effect.promise(() => fs.mkdir(outDir, { recursive: true }))

    // Atualiza estado para "transcoding"
    yield* Ref.update(stateRef, (s) => ({
      ...s,
      progress: {
        ...s.progress,
        [resolution]: { ...s.progress[resolution], status: "transcoding" },
      },
    }))

    // Deferred sinaliza ao watcher que FFmpeg terminou
    const watcherDone = yield* Deferred.make<void, never>()

    // ── Fiber B: Upload em tempo real ──────────────────────────────────────
    const uploaderFiber = yield* Effect.fork(
      watchDirectory(outDir, watcherDone).pipe(
        Stream.filter((p) => p.endsWith(".ts")),
        Stream.tap(() =>
          // Atualiza estado: "uploading" na primeira vez
          Ref.update(stateRef, (s) =>
            s.progress[resolution].status === "transcoding"
              ? { ...s, progress: { ...s.progress, [resolution]: { ...s.progress[resolution], status: "uploading" } } }
              : s
          )
        ),
        Stream.mapEffect((segPath) =>
          Effect.gen(function* () {
            // Captura o tamanho do segmento ANTES do upload, para acumular no estado
            const stat = yield* Effect.promise(() => fs.stat(segPath))

            yield* r2.uploadFile(
              `${payload.outputPrefix}${resolution}/${path.basename(segPath)}`,
              segPath,
            ).pipe(
              Effect.retry({ times: 3, schedule: Schedule.exponential("500 millis") }),
            )

            // Incrementa contador de segmentos enviados + bytes totais do job
            yield* Ref.update(stateRef, (s) => ({
              ...s,
              totalOutputBytes: s.totalOutputBytes + stat.size,
              progress: {
                ...s.progress,
                [resolution]: {
                  ...s.progress[resolution],
                  segmentsUploaded: s.progress[resolution].segmentsUploaded + 1,
                },
              },
            }))
          })
        ),
        Stream.runDrain,
      )
    )

    // ── Fiber A: FFmpeg ────────────────────────────────────────────────────
    yield* Effect.tryPromise({
      try: async () => {
        const durationSec = (yield* Ref.get(stateRef)).videoDurationSec ?? 0

        const proc = execa("ffmpeg", [
          "-i",       state.videoDurationSec ? payload.inputUrl : payload.inputUrl, // sempre a URL original
          "-vf",      `scale=${cfg.scale}`,
          "-c:v",     "libx264",
          "-crf",     String(cfg.crf),
          "-preset",  config.ffmpegPreset,
          "-profile:v", "main",
          "-c:a",     "aac",
          "-b:a",     cfg.audioBitrate,
          "-ar",      "48000",
          "-hls_time",           "6",
          "-hls_list_size",      "0",
          "-hls_flags",          "independent_segments+append_list",
          "-hls_segment_type",   "mpegts",
          "-hls_segment_filename", path.join(outDir, "seg_%04d.ts"),
          path.join(outDir, "index.m3u8"),
        ], { reject: false, all: true })

        // Parse de progresso no stderr
        proc.stderr?.on("data", (chunk: Buffer) => {
          const lines = chunk.toString().split("\n")
          for (const line of lines) {
            const pct = parseProgressLine(line, durationSec)
            if (pct === null) continue

            // Fire-and-forget: atualiza Ref e publica SNS sem bloquear FFmpeg
            Effect.runFork(
              Effect.gen(function* () {
                yield* Ref.update(stateRef, (s) => ({
                  ...s,
                  progress: {
                    ...s.progress,
                    [resolution]: { ...s.progress[resolution], transcodePct: pct },
                  },
                }))
                const s = yield* Ref.get(stateRef)
                const overallPct = Math.round(
                  RESOLUTIONS.reduce((acc, r) => acc + s.progress[r].transcodePct, 0) / RESOLUTIONS.length
                )
                yield* sns.publish({
                  eventType:        "job.progress",
                  jobId:            payload.id,
                  resolution,
                  transcodePct:     pct,
                  segmentsUploaded: s.progress[resolution].segmentsUploaded,
                  overallPct,
                  timestamp:        new Date().toISOString(),
                })
              }).pipe(Effect.provide(/* layers injected via scope */))
            )
          }
        })

        const result = await proc
        if (result.exitCode !== 0) {
          throw { exitCode: result.exitCode, signal: result.signal, stderr: result.stderr }
        }
      },
      catch: (e: any) => new FFmpegError({
        resolution,
        exitCode: e.exitCode ?? null,
        signal:   e.signal   ?? null,
        stderr:   e.stderr   ?? String(e),
      }),
    })

    // FFmpeg terminou → sinaliza o watcher para encerrar o stream
    yield* Deferred.complete(watcherDone, Effect.unit)
    yield* Fiber.join(uploaderFiber)

    // Upload do playlist .m3u8 desta resolução
    const playlistPath = path.join(outDir, "index.m3u8")
    const playlistStat = yield* Effect.promise(() => fs.stat(playlistPath))
    yield* r2.uploadFile(`${payload.outputPrefix}${resolution}/index.m3u8`, playlistPath)

    // Atualiza estado final da resolução (status + bytes do playlist)
    yield* Ref.update(stateRef, (s) => ({
      ...s,
      totalOutputBytes: s.totalOutputBytes + playlistStat.size,
      progress: {
        ...s.progress,
        [resolution]: { ...s.progress[resolution], status: "completed", transcodePct: 100 },
      },
    }))
  })
```

---

### 10.2 Master Pipeline

```typescript
// src/pipelines/master.pipeline.ts
import { Effect, Ref } from "effect"
import path from "node:path"
import fs from "node:fs/promises"
import { execa } from "execa"
import { transcodeResolution } from "./resolution.pipeline.js"
import { R2Service } from "../layers/R2Layer.js"
import { SNSService } from "../layers/SNSLayer.js"
import { SQSService } from "../layers/SQSLayer.js"
import { ConfigService } from "../config/index.js"
import { forkSQSHeartbeat } from "../sqs/heartbeat.js"
import { probeVideoDuration } from "../utils/ffmpeg-progress.js"
import { CostService } from "../layers/CostLayer.js"
import {
  RESOLUTIONS, RESOLUTION_CONFIGS,
  makeInitialJobState,
  type JobPayload, type JobState, type OutputManifest,
} from "../types/index.js"

/**
 * Divide um array em lotes (chunks) de tamanho `size`.
 * Ex: chunk(["480p","720p","1080p"], 2) → [["480p","720p"], ["1080p"]]
 */
const chunk = <T>(arr: readonly T[], size: number): T[][] => {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push([...arr.slice(i, i + size)])
  return out
}

/**
 * Pipeline principal. Recebe o payload do job e um Ref de estado compartilhado.
 * Retorna o OutputManifest em caso de sucesso.
 */
export const transcodeJob = (
  payload:  JobPayload,
  stateRef: Ref.Ref<JobState>,
) =>
  Effect.gen(function* () {
    const r2     = yield* R2Service
    const sns    = yield* SNSService
    const sqs    = yield* SQSService
    const config = yield* ConfigService
    const state  = yield* Ref.get(stateRef)
    const startedAt = Date.now()

    // ── 1. SNS: job.started ────────────────────────────────────────────────
    yield* sns.publish({
      eventType:   "job.started",
      jobId:       payload.id,
      instanceId:  state.instanceId,
      inputUrl:    payload.inputUrl,
      timestamp:   new Date().toISOString(),
      callbackData: payload.callbackData,
    })

    // ── 2. Resolve duração via ffprobe ─────────────────────────────────────
    const durationSec = yield* probeVideoDuration(payload.inputUrl)
    yield* Ref.update(stateRef, (s) => ({ ...s, videoDurationSec: durationSec }))

    // ── 3. Inicia heartbeat SQS ────────────────────────────────────────────
    const heartbeatFiber = yield* forkSQSHeartbeat(state.receiptHandle)

    // ── 4. Transcode: resoluções em LOTES de `resolutionBatchSize` ──────────
    // Lotes executam em SÉRIE; resoluções DENTRO do lote, em PARALELO.
    // Rationale: rodar todas as resoluções ao mesmo tempo saturaria o CPU
    // (todas encodando simultaneamente). Rodar uma por vez deixaria o CPU
    // ocioso durante o upload dos segmentos (bound em bandwidth, não CPU).
    // Lotes de 2 equilibram isso: enquanto uma resolução do lote está fazendo
    // upload de segmentos já gerados, a outra ainda está consumindo CPU para
    // encodar — uso pleno de CPU e bandwidth simultaneamente.
    const batches = chunk(RESOLUTIONS, config.resolutionBatchSize)
    for (const batch of batches) {
      yield* Effect.all(
        batch.map((res) => transcodeResolution(payload, res, stateRef)),
        { concurrency: batch.length },
      )
    }

    // ── 5. Gera master.m3u8 e thumbnail em paralelo ────────────────────────
    yield* Effect.all([
      // Master playlist
      Effect.gen(function* () {
        const master = buildMasterPlaylist(payload.outputPrefix)
        const masterSize = Buffer.byteLength(master, "utf-8")
        yield* r2.uploadBuffer(
          `${payload.outputPrefix}master.m3u8`,
          master,
          "application/x-mpegURL",
        )
        yield* Ref.update(stateRef, (s) => ({ ...s, totalOutputBytes: s.totalOutputBytes + masterSize }))
      }),
      // Thumbnail
      extractAndUploadThumbnail(payload, stateRef),
    ], { concurrency: 2 })

    // ── 6. Calcula custo real do job via Spot Price History ────────────────
    const endedAt = new Date()
    const costService = yield* CostService
    const cost = yield* costService.calculateJobCost({
      instanceType: state.instanceType,
      startedAt:    state.startedAt,
      endedAt,
    })

    // ── 7. Monta OutputManifest ──────────────────────────────────────────────
    const finalState = yield* Ref.get(stateRef)
    const base       = config.r2PublicBaseUrl

    const output: OutputManifest = {
      masterPlaylistUrl: `${base}/${payload.outputPrefix}master.m3u8`,
      thumbnailUrl:      `${base}/${payload.outputPrefix}thumbnail.jpg`,
      durationSec,
      processingMs:    Date.now() - startedAt,
      outputSizeBytes: finalState.totalOutputBytes,
      cost,
      resolutions: Object.fromEntries(
        RESOLUTIONS.map((res) => [res, {
          playlistUrl:  `${base}/${payload.outputPrefix}${res}/index.m3u8`,
          segmentCount: finalState.progress[res].segmentsUploaded,
          bandwidth:    RESOLUTION_CONFIGS[res].bandwidth,
        }])
      ) as OutputManifest["resolutions"],
    }

    // ── 8. Deleta mensagem do SQS (somente em caso de sucesso) ─────────────
    yield* sqs.deleteMessage(state.receiptHandle)

    // ── 9. Para o heartbeat ──────────────────────────────────────────────────
    yield* Fiber.interrupt(heartbeatFiber)

    // ── 10. SNS: job.completed ────────────────────────────────────────────────
    yield* sns.publish({
      eventType:   "job.completed",
      jobId:       payload.id,
      instanceId:  state.instanceId,
      output,
      timestamp:   new Date().toISOString(),
      callbackData: payload.callbackData,
    })

    return output
  }).pipe(
    // ── Error handler global ───────────────────────────────────────────────
    Effect.catchAll((error) =>
      Effect.gen(function* () {
        const state = yield* Ref.get(stateRef)

        // Interrompe heartbeat
        // (Fiber já está disponível no closure em uma implementação real)

        // Cleanup /tmp
        yield* Effect.promise(() =>
          fs.rm(path.join(process.env.TEMP_DIR ?? "/tmp/transcoder", payload.id), {
            recursive: true, force: true,
          })
        )

        // SNS: job.failed (sempre, mesmo se SNS falhar antes)
        yield* Effect.flatMap(SNSService, (sns) =>
          sns.publish({
            eventType:   "job.failed",
            jobId:       payload.id,
            instanceId:  state.instanceId,
            error: {
              code:    (error as any)._tag ?? "UnknownError",
              message: String(error),
            },
            timestamp:   new Date().toISOString(),
            callbackData: payload.callbackData,
          })
        )

        return yield* Effect.fail(error)
      })
    ),
    // Cleanup final sempre (sucesso ou falha)
    Effect.ensuring(
      Effect.promise(() =>
        fs.rm(
          path.join(process.env.TEMP_DIR ?? "/tmp/transcoder", payload.id),
          { recursive: true, force: true },
        ).catch(() => undefined)
      )
    ),
  )

// ─── Helpers ──────────────────────────────────────────────────────────────────

const buildMasterPlaylist = (outputPrefix: string): string => {
  const lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
  for (const res of RESOLUTIONS) {
    const cfg = RESOLUTION_CONFIGS[res]
    lines.push(
      `#EXT-X-STREAM-INF:BANDWIDTH=${cfg.bandwidth},RESOLUTION=${cfg.scale.replace(":", "x")},CODECS="${cfg.codecs}"`,
      `${res}/index.m3u8`,
      "",
    )
  }
  return lines.join("\n")
}

const extractAndUploadThumbnail = (payload: JobPayload, stateRef: Ref.Ref<JobState>) =>
  Effect.gen(function* () {
    const r2     = yield* R2Service
    const config = yield* ConfigService
    const thumbPath = path.join(config.tempDir, payload.id, "thumbnail.jpg")

    yield* Effect.tryPromise({
      try: () => execa("ffmpeg", [
        "-i",     payload.inputUrl,
        "-ss",    "5",
        "-vframes", "1",
        "-q:v",   "2",
        "-vf",    "scale=1280:-1",
        thumbPath,
      ]).then(() => undefined),
      catch: (e) => new Error(`Thumbnail extraction failed: ${e}`),
    })

    yield* r2.uploadFile(
      `${payload.outputPrefix}thumbnail.jpg`,
      thumbPath,
    )
  })
```

---

## 11. Entry Points

### 11.1 `main.ts` — Produção (SQS + EC2)

```typescript
// src/main.ts
import { Effect, ManagedRuntime, Layer, Ref } from "effect"
import { Schema } from "@effect/schema"
import { ConfigLayer } from "./config/index.js"
import { R2Layer } from "./layers/R2Layer.js"
import { SNSLayerLive } from "./layers/SNSLayer.js"
import { SQSLayer } from "./layers/SQSLayer.js"
import { EC2MetadataLayerLive } from "./layers/EC2MetadataLayer.js"
import { CostLayerLive } from "./layers/CostLayer.js"
import { transcodeJob } from "./pipelines/master.pipeline.js"
import { JobPayloadSchema, makeInitialJobState } from "./types/index.js"
import { JobPayloadParseError } from "./errors/index.js"

const AppLayer = Layer.mergeAll(
  ConfigLayer,
  R2Layer,
  SNSLayerLive,
  SQSLayer,
  EC2MetadataLayerLive,
  CostLayerLive,
)

const program = Effect.gen(function* () {
  const sqs     = yield* SQSService
  const ec2Meta = yield* EC2MetadataService

  yield* Effect.log("Worker starting. Polling SQS...")

  const message = yield* sqs.receiveMessage()

  if (!message) {
    yield* Effect.log("SQS queue is empty. Exiting.")
    return
  }

  yield* Effect.log("Job received from SQS", { receiptHandle: message.receiptHandle.slice(0, 20) })

  // Parse e valida o payload do job
  const payload = yield* Effect.try({
    try: () => Schema.decodeUnknownSync(JobPayloadSchema)(JSON.parse(message.body)),
    catch: (e) => new JobPayloadParseError({ raw: message.body, cause: e }),
  })

  // Busca o instanceId e instanceType da EC2 via IMDS (instanceType é necessário
  // para o CostService consultar o Spot Price History correto ao final do job)
  const instanceId   = yield* ec2Meta.getInstanceId()
  const instanceType = yield* ec2Meta.getInstanceType()

  yield* Effect.log("Processing job", { jobId: payload.id, instanceId, instanceType })

  // Cria o Ref de estado do job
  const stateRef = yield* Ref.make(
    makeInitialJobState(payload.id, instanceId, instanceType, message.receiptHandle)
  )

  // Executa o pipeline principal
  yield* transcodeJob(payload, stateRef)

  yield* Effect.log("Job completed successfully", { jobId: payload.id })
}).pipe(
  Effect.provide(AppLayer)
)

const runtime = ManagedRuntime.make(AppLayer)

ManagedRuntime.runPromise(runtime)(program)
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("Fatal error:", e)
    process.exit(1)
  })
  .finally(() => ManagedRuntime.dispose(runtime))
```

---

### 11.2 `manual.ts` — Desenvolvimento Local

Para testar localmente sem SQS, sem EC2, e com SNS logando no console.

```typescript
// src/manual.ts
import { Effect, ManagedRuntime, Layer, Ref } from "effect"
import { ConfigLayer } from "./config/index.js"
import { R2Layer } from "./layers/R2Layer.js"
import { SNSLayerConsole } from "./layers/SNSLayer.js"   // ← logs no stdout
import { EC2MetadataLayerStub } from "./layers/EC2MetadataLayer.js"
import { transcodeJob } from "./pipelines/master.pipeline.js"
import { CostLayerStub } from "./layers/CostLayer.js"
import { makeInitialJobState, type JobPayload } from "./types/index.js"

// ─── Payload hardcoded — edite aqui para seus testes ──────────────────────────
const TEST_PAYLOAD: JobPayload = {
  id:           "test-job-001",
  // URL pública de um vídeo de amostra, ou uma presigned URL do seu R2
  inputUrl:     "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4",
  outputPrefix: "transcoded/test-job-001/",
  callbackData: {
    userId:  "user_test",
    videoId: "video_test_001",
  },
}

const MOCK_RECEIPT_HANDLE = "mock-receipt-handle-for-local-dev"
const MOCK_INSTANCE_TYPE  = "local-dev"   // sem EC2 real → CostLayerStub sempre retorna custo 0

// ─── Layer sem SQS (não há fila para consumir localmente) ─────────────────────
// SQSLayer é substituído por um mock inline que não faz nada.
const MockSQSLayer = Layer.succeed(SQSService, {
  receiveMessage:  () => Effect.succeed(null),
  deleteMessage:   () => Effect.succeed(undefined),
  extendVisibility: () => Effect.succeed(undefined),
})

const ManualAppLayer = Layer.mergeAll(
  ConfigLayer,
  R2Layer,
  SNSLayerConsole,
  MockSQSLayer,
  EC2MetadataLayerStub,
  CostLayerStub,        // custo sempre 0 — não há Spot Instance real em dev local
)

const program = Effect.gen(function* () {
  console.log("═══════════════════════════════════════════════")
  console.log("  VIDEO TRANSCODER — Manual Test Mode")
  console.log("═══════════════════════════════════════════════")
  console.log("Job payload:", JSON.stringify(TEST_PAYLOAD, null, 2))
  console.log("")

  const stateRef = yield* Ref.make(
    makeInitialJobState(TEST_PAYLOAD.id, "local-dev", MOCK_INSTANCE_TYPE, MOCK_RECEIPT_HANDLE)
  )

  const output = yield* transcodeJob(TEST_PAYLOAD, stateRef)

  console.log("\n✅ Transcoding completed!")
  console.log("Output:", JSON.stringify(output, null, 2))
}).pipe(
  Effect.provide(ManualAppLayer)
)

Effect.runPromise(program).catch((e) => {
  console.error("\n❌ Transcoding failed:", e)
  process.exit(1)
})
```

---

## 12. R2 Output Layout

```
<bucket>/
└── transcoded/
    └── <jobId>/              ← outputPrefix no payload
        ├── master.m3u8
        ├── thumbnail.jpg
        ├── 480p/
        │   ├── index.m3u8
        │   ├── seg_0000.ts
        │   └── seg_NNNN.ts
        ├── 720p/
        │   ├── index.m3u8
        │   └── seg_NNNN.ts
        └── 1080p/
            ├── index.m3u8
            └── seg_NNNN.ts
```

> **CORS no R2**: configure `AllowedMethods: [GET, HEAD]` e
> `AllowedHeaders: [Range]` para permitir que browsers façam streaming de HLS
> diretamente do bucket.

---

## 13. Docker

### Dockerfile

```dockerfile
# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build            # tsc → /app/dist

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM node:20-alpine AS runner

# FFmpeg + ffprobe
RUN apk add --no-cache ffmpeg

WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/dist         ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./

# main.ts compilado é o entrypoint de produção
CMD ["node", "dist/main.js"]
```

### `tsconfig.json`

```json
{
  "compilerOptions": {
    "target":          "ES2022",
    "module":          "NodeNext",
    "moduleResolution": "NodeNext",
    "outDir":          "./dist",
    "rootDir":         "./src",
    "strict":          true,
    "exactOptionalPropertyTypes": true,
    "noUncheckedIndexedAccess":   true,
    "declaration":     true,
    "sourceMap":       true,
    "esModuleInterop": true
  },
  "include": ["src"]
}
```

### `docker-compose.dev.yml` — Dev Local

Só para subir dependências externas durante desenvolvimento.
O serviço principal roda com `ts-node` ou `tsx` diretamente na sua máquina.

```yaml
version: "3.9"

# Não há Redis, não há Postgres, não há API — o serviço não tem dependências locais.
# Em dev local, você roda: npx tsx src/manual.ts
# Este compose é opcional — útil apenas se você quiser simular LocalStack.

services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=sqs,sns
      - DEFAULT_REGION=us-east-1
    volumes:
      - localstack-data:/var/lib/localstack

volumes:
  localstack-data:
```

---

## 14. IAM Permissions Necessárias

A EC2 Instance Role precisa das seguintes permissões (política mínima):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SQSAccess",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:<REGION>:<ACCOUNT>:transcode-jobs"
    },
    {
      "Sid": "SNSAccess",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:<REGION>:<ACCOUNT>:transcode-events"
    },
    {
      "Sid": "SpotPriceHistory",
      "Effect": "Allow",
      "Action": "ec2:DescribeSpotPriceHistory",
      "Resource": "*"
    }
  ]
}
```

> **Nota**: `ec2:DescribeSpotPriceHistory` não suporta restrição por ARN de
> recurso específico — a AWS exige `Resource: "*"` para esta action.

> **Nota**: permissões de R2 são gerenciadas por API Token da Cloudflare,
> **não** por IAM. Passe via variáveis de ambiente na instância EC2.

---

## 15. `package.json` (scripts)

```json
{
  "name": "video-transcoder",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "build":   "tsc",
    "start":   "node dist/main.js",
    "manual":  "tsx src/manual.ts",
    "dev":     "tsx watch src/manual.ts",
    "test":    "vitest run",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "effect":                "^3.0.0",
    "@effect/schema":        "^0.68.0",
    "@aws-sdk/client-sqs":   "^3.0.0",
    "@aws-sdk/client-sns":   "^3.0.0",
    "@aws-sdk/client-ec2":   "^3.0.0",
    "@aws-sdk/client-s3":    "^3.0.0",
    "@aws-sdk/lib-storage":  "^3.0.0",
    "@aws-sdk/s3-request-presigner": "^3.0.0",
    "chokidar":              "^4.0.0",
    "execa":                 "^8.0.0",
    "pino":                  "^9.0.0",
    "ulid":                  "^2.3.0"
  },
  "devDependencies": {
    "typescript":  "^5.4.0",
    "tsx":         "^4.0.0",
    "vitest":      "^1.6.0",
    "@types/node": "^20.0.0"
  }
}
```

---

## 16. Resumo do Fluxo de Dados

```
SQS Message Body (JSON)
  └─► Schema.decodeUnknownSync(JobPayloadSchema) → JobPayload
      └─► EC2Metadata.getInstanceId() + getInstanceType()
          └─► Ref.make(makeInitialJobState(...)) → Ref<JobState>
              └─► transcodeJob(payload, stateRef)
                  ├─► probeVideoDuration(inputUrl) → number
                  ├─► [Lote 1] Effect.all([480p, 720p], { concurrency: 2 })
                  │     Each resolution:
                  │       ├─► FFmpeg writes /tmp/<id>/<res>/seg_NNNN.ts
                  │       └─► chokidar emits path → R2.uploadFile (real-time)
                  │             └─► Ref.update(stateRef, totalOutputBytes += size, ...)
                  │             └─► SNS job.progress
                  ├─► [Lote 2] Effect.all([1080p], { concurrency: 1 })
                  │     (mesmo fluxo do Lote 1, sequencial após o Lote 1 terminar)
                  ├─► R2.uploadBuffer(master.m3u8)
                  ├─► FFmpeg thumbnail → R2.uploadFile(thumbnail.jpg)
                  ├─► CostService.calculateJobCost(instanceType, startedAt, endedAt)
                  │     └─► EC2.DescribeSpotPriceHistory → JobCostReport
                  ├─► SQS.deleteMessage(receiptHandle)    ← só se tudo OK
                  └─► SNS.publish(job.completed { instanceId, output: { cost, outputSizeBytes, ... } })
                        └─► Lambda → ec2:TerminateInstances(instanceId)
```
