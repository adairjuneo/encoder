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
    const ffmpeg    = yield* FFmpegService
    const r2        = yield* R2Service
    const binary    = yield* ffmpeg.getBinaryPath()
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
          if (result.exitCode !== 0) {
            throw Object.assign(new Error("thumbnail failed"), result)
          }
        }),
      catch: (e: unknown) => {
        const err = e as { exitCode?: number | null; stderr?: string }
        return new FFmpegError({
          resolution: "thumbnail",
          exitCode:   err.exitCode ?? null,
          stderr:     err.stderr ?? String(e),
        })
      },
    })

    const key = `transcoded/${jobId}/thumbnail.jpg`
    yield* r2.uploadBuffer(key, yield* Effect.promise(() => fs.readFile(thumbPath)))

    return key
  })
}
