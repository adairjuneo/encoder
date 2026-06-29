import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Effect, Layer } from 'effect';
import { describe, expect, it } from 'vitest';
import { FFmpegError } from '../errors/index.js';
import { FFmpegServiceLive } from '../layers/FFmpegLayer.js';
import { R2ServiceStub } from '../layers/R2Layer.js';
import { ConfigService } from '../config/index.js';
import { extractThumbnail } from './thumbnail.pipeline.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const testConfig = {
  NODE_ENV: 'test' as const,
  AWS_REGION: 'us-east-1',
  SQS_QUEUE_URL: 'https://sqs.test',
  SQS_VISIBILITY_TIMEOUT_SEC: 300,
  SQS_HEARTBEAT_INTERVAL_MS: 240_000,
  SNS_TOPIC_ARN: 'arn:aws:sns:test',
  R2_ACCOUNT_ID: 'test',
  R2_ACCESS_KEY_ID: 'test',
  R2_SECRET_ACCESS_KEY: 'test',
  R2_BUCKET_NAME: 'test-bucket',
  R2_PUBLIC_BASE_URL: 'https://r2-test.local',
  TEMP_DIR: '/tmp/transcoder-test',
  FFMPEG_PRESET: 'ultrafast' as const,
  RESOLUTION_BATCH_SIZE: 2,
  LOG_LEVEL: 'silent',
};

describe('extractThumbnail', () => {
  it('fails with FFmpegError (not raw rejection) when input file does not exist', async () => {
    const configLayer = Layer.succeed(ConfigService, testConfig);
    const testLayers = Layer.mergeAll(
      R2ServiceStub(new Map()),
      FFmpegServiceLive.pipe(Layer.provide(configLayer)),
      configLayer,
    );

    const result = await Effect.runPromise(
      extractThumbnail(
        '/tmp/does-not-exist-xyzzy.mkv',
        '/tmp/transcoder-test',
        'thumb-test-01',
        10,
      )
        .pipe(
          Effect.provide(testLayers),
          Effect.flip, // turn the failure into a success so we can inspect it
        ),
    );

    // Must be a tagged FFmpegError, not a raw ExecaError or unhandled rejection
    expect(result).toBeInstanceOf(FFmpegError);
    expect((result as FFmpegError).exitCode).not.toBeNull();
  }, 30_000);
});
