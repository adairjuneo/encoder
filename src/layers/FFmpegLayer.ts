import { Context, Effect, Layer } from "effect"
import { execa } from "execa"
import { ConfigService } from "../config/index.js"
import { FFmpegError } from "../errors/index.js"

export interface FFmpegServiceShape {
  getBinaryPath(): Effect.Effect<string, FFmpegError>
  getPreset(): Effect.Effect<string>
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
        const { stdout } = await execa("which", ["ffmpeg"])
        return stdout.trim()
      },
      catch: (e) =>
        new FFmpegError({ resolution: "n/a", exitCode: null, stderr: String(e) }),
    })

    return {
      getBinaryPath: () => Effect.succeed(binaryPath),
      getPreset: () => Effect.succeed(config.FFMPEG_PRESET),
    }
  }),
)
