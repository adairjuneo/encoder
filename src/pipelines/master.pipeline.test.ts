import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Effect, Layer } from 'effect';
import { describe, expect, it } from 'vitest';
import { ConfigService } from '../config/index.js';
import { CostServiceStub } from '../layers/CostLayer.js';
import { EC2MetadataServiceStub } from '../layers/EC2MetadataLayer.js';
import { FFmpegServiceLive } from '../layers/FFmpegLayer.js';
import { R2ServiceStub } from '../layers/R2Layer.js';
import { SNSServiceCollecting } from '../layers/SNSLayer.js';
import { SQSServiceStub } from '../layers/SQSLayer.js';
import type { SNSEvent } from '../types/index.js';
import { transcodeJob } from './master.pipeline.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_VIDEO = path.resolve(__dirname, '../../test-fixtures/short.mkv');

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

describe('transcodeJob', () => {
  it('produces OutputManifest with all three resolutions', async () => {
    const publishedEvents: SNSEvent[] = [];
    const uploads = new Map<string, Buffer>();

    const configLayer = Layer.succeed(ConfigService, testConfig);

    const testLayers = Layer.mergeAll(
      SQSServiceStub({ messages: [] }),
      SNSServiceCollecting(publishedEvents),
      R2ServiceStub(uploads),
      EC2MetadataServiceStub,
      CostServiceStub,
      FFmpegServiceLive.pipe(Layer.provide(configLayer)),
      configLayer,
    );

    const payload = {
      id: 'test-job-01',
      inputUrl: FIXTURE_VIDEO,
      outputPrefix: 'test-job-01',
      callbackData: null,
    };

    const result = await Effect.runPromise(
      transcodeJob(payload, 'receipt-handle-test').pipe(
        Effect.provide(testLayers),
      ),
    );

    // OutputManifest shape
    expect(result.masterPlaylistUrl).toMatch(/master\.m3u8/);
    expect(result.thumbnailUrl).toMatch(/thumbnail\.jpg/);
    expect(result.resolutions).toHaveProperty('480p');
    expect(result.resolutions).toHaveProperty('720p');
    expect(result.resolutions).toHaveProperty('1080p');
    expect(result.durationSec).toBeGreaterThan(0);
    expect(result.processingMs).toBeGreaterThan(0);
    expect(result.cost.currency).toBe('USD');

    // SNS events published in correct order
    const eventTypes = publishedEvents.map((e) => e.type);
    expect(eventTypes[0]).toBe('job.started');
    expect(eventTypes[eventTypes.length - 1]).toBe('job.completed');
    expect(eventTypes).toContain('job.progress');

    // Temp dir cleaned up
    await expect(
      fs.access(`/tmp/transcoder-test/${payload.id}`),
    ).rejects.toThrow();
  }, 120_000);
});
