import fs from 'node:fs/promises';
import path from 'node:path';
import { Effect, Fiber, Stream } from 'effect';
import { execa } from 'execa';
import {
  FFmpegError,
  type R2UploadError,
  type SNSPublishError,
} from '../errors/index.js';
import { FFmpegService } from '../layers/FFmpegLayer.js';
import { R2Service } from '../layers/R2Layer.js';
import { SNSService } from '../layers/SNSLayer.js';
import type {
  JobState,
  Resolution,
  ResolutionProgress,
} from '../types/index.js';
import { RESOLUTION_CONFIGS, RESOLUTIONS } from '../types/index.js';
import { watchDirectory } from '../utils/chokidar-stream.js';
import { parseProgressLine } from '../utils/ffmpeg-progress.js';

export function transcodeResolution(
  resolution: Resolution,
  inputPath: string,
  outputDir: string,
  jobState: JobState,
): Effect.Effect<
  ResolutionProgress,
  FFmpegError | R2UploadError | SNSPublishError,
  SNSService | R2Service | FFmpegService
> {
  return Effect.gen(function* () {
    const ffmpeg = yield* FFmpegService;
    const sns = yield* SNSService;
    const r2 = yield* R2Service;
    const binary = yield* ffmpeg.getBinaryPath();
    const preset = yield* ffmpeg.getPreset();
    const cfg = RESOLUTION_CONFIGS[resolution];
    const segDir = path.join(outputDir, resolution);
    const m3u8 = path.join(segDir, 'index.m3u8');

    yield* Effect.promise(() => fs.mkdir(segDir, { recursive: true }));

    let segmentsUploaded = 0;
    let segmentsTotal = 0;

    // Fork chokidar watcher → upload each new .ts segment to R2 as it appears.
    const watchFiber = yield* Effect.forkDaemon(
      watchDirectory(segDir).pipe(
        Stream.mapEffect((segPath) =>
          Effect.gen(function* () {
            const key = `transcoded/${jobState.jobId}/${resolution}/${path.basename(segPath)}`;
            yield* r2.uploadFile(key, segPath);
            segmentsUploaded++;
            segmentsTotal = Math.max(segmentsTotal, segmentsUploaded);
          }),
        ),
        Stream.runDrain,
      ),
    );

    // Run FFmpeg transcode process.
    yield* Effect.tryPromise({
      try: () =>
        execa(
          binary,
          [
            '-i',
            inputPath,
            '-vf',
            `scale=${cfg.scale}`,
            '-c:v',
            'libx264',
            '-preset',
            preset,
            '-crf',
            String(cfg.crf),
            '-c:a',
            'aac',
            '-b:a',
            cfg.audioBitrate,
            '-f',
            'hls',
            '-hls_time',
            '6',
            '-hls_list_size',
            '0',
            '-hls_segment_filename',
            path.join(segDir, 'seg_%04d.ts'),
            m3u8,
          ],
          {
            all: true,
            reject: false,
          },
        ).then((result) => {
          if (result.exitCode !== 0) {
            throw Object.assign(new Error('ffmpeg failed'), {
              exitCode: result.exitCode,
              stderr: result.stderr,
            });
          }
        }),
      catch: (e: unknown) => {
        const err = e as { exitCode?: number | null; stderr?: string };
        return new FFmpegError({
          resolution,
          exitCode: err.exitCode ?? null,
          stderr: err.stderr ?? String(e),
        });
      },
    });

    // Wait for all uploads to drain, then interrupt watcher.
    yield* Fiber.interrupt(watchFiber);

    // Upload the per-resolution index.m3u8.
    const m3u8Key = `transcoded/${jobState.jobId}/${resolution}/index.m3u8`;
    yield* r2.uploadBuffer(
      m3u8Key,
      yield* Effect.promise(() => fs.readFile(m3u8)),
    );

    // Publish progress event.
    const overallPct = Math.round(
      Object.values(jobState.progress).reduce(
        (sum, p) => sum + p.transcodePct,
        0,
      ) / RESOLUTIONS.length,
    );
    yield* sns.publish({
      type: 'job.progress',
      jobId: jobState.jobId,
      resolution,
      transcodePct: 100,
      segmentsUploaded,
      overallPct,
      timestamp: new Date().toISOString(),
    });

    return {
      status: 'done',
      transcodePct: 100,
      segmentsUploaded,
      segmentsTotal,
    } satisfies ResolutionProgress;
  });
}
