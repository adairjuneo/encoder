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
        accessKeyId: config.R2_ACCESS_KEY_ID,
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
            client.send(
              new PutObjectCommand({
                Bucket: config.R2_BUCKET_NAME,
                Key: key,
                Body: data,
              }),
            ),
          catch: (e) => new R2UploadError({ key, cause: e }),
        }).pipe(Effect.asVoid),

      getPresignedUrl: (key) =>
        Effect.tryPromise({
          try: () =>
            getSignedUrl(
              client,
              new PutObjectCommand({ Bucket: config.R2_BUCKET_NAME, Key: key }),
              { expiresIn: 3600 },
            ),
          catch: (e) => new R2PresignError({ key, cause: e }),
        }).pipe(Effect.map(() => `${config.R2_PUBLIC_BASE_URL}/${key}`)),
    }
  }),
)

export function R2ServiceStub(
  uploads: Map<string, Buffer>,
): Layer.Layer<R2Service> {
  return Layer.succeed(R2Service, {
    uploadFile: (key, localPath) =>
      Effect.tryPromise({
        try: () =>
          fs.promises.readFile(localPath).then((buf) => {
            uploads.set(key, buf)
          }),
        catch: (e) => new R2UploadError({ key, cause: e }),
      }),
    uploadBuffer: (key, data) =>
      Effect.sync(() => {
        uploads.set(key, data)
      }),
    getPresignedUrl: (key) =>
      Effect.succeed(`https://r2-stub.local/${key}`),
  })
}
