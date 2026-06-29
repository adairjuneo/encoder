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
import { SNSService, SNSServiceCollecting } from '../layers/SNSLayer.js';
import { SQSService } from '../layers/SQSLayer.js';
import { SQSServiceStub } from '../layers/SQSLayer.js';
import type { SNSEvent } from '../types/index.js';
import { SNSPublishError } from '../errors/index.js';
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

    // Bug 5: progress events must carry non-zero overallPct
    const progressEvents = publishedEvents.filter((e) => e.type === 'job.progress');
    const lastProgress = progressEvents[progressEvents.length - 1];
    expect(lastProgress).toBeDefined();
    if (lastProgress?.type === 'job.progress') {
      expect(lastProgress.overallPct).toBeGreaterThan(0);
    }

    // Bug 1: every .ts segment referenced in each resolution m3u8 must be in uploads
    for (const resolution of ['480p', '720p', '1080p'] as const) {
      const m3u8Key = `transcoded/${payload.id}/${resolution}/index.m3u8`;
      const m3u8Buf = uploads.get(m3u8Key);
      expect(m3u8Buf, `missing m3u8 for ${resolution}`).toBeDefined();
      if (!m3u8Buf) continue;
      const lines = m3u8Buf.toString('utf-8').split('\n');
      for (const line of lines) {
        if (line.trim() && !line.startsWith('#')) {
          const segKey = `transcoded/${payload.id}/${resolution}/${line.trim()}`;
          expect(uploads.has(segKey), `segment ${segKey} missing from R2`).toBe(true);
        }
      }
    }

    // Temp dir cleaned up
    await expect(
      fs.access(`/tmp/transcoder-test/${payload.id}`),
    ).rejects.toThrow();
  }, 120_000);
});

describe('transcodeJob — deleteMessage ordering', () => {
  it('does NOT delete SQS message when sns.publish(job.completed) fails', async () => {
    const ops: string[] = [];
    const uploads = new Map<string, Buffer>();

    const configLayer = Layer.succeed(ConfigService, testConfig);

    // SNS stub that throws on job.completed publish
    const failOnCompletedSNS = Layer.succeed(SNSService, {
      publish: (event: SNSEvent) =>
        Effect.gen(function* () {
          ops.push(`sns:${event.type}`);
          if (event.type === 'job.completed') {
            yield* Effect.fail(new SNSPublishError({ cause: new Error('SNS timeout') }));
          }
        }),
    });

    // SQS stub that tracks deleteMessage calls
    const trackingSQS = Layer.succeed(SQSService, {
      receiveMessage: () => Effect.succeed(null),
      deleteMessage: () => Effect.sync(() => { ops.push('sqs:deleteMessage'); }),
      extendVisibility: () => Effect.succeed(undefined),
    });

    const testLayers = Layer.mergeAll(
      trackingSQS,
      failOnCompletedSNS,
      R2ServiceStub(uploads),
      EC2MetadataServiceStub,
      CostServiceStub,
      FFmpegServiceLive.pipe(Layer.provide(configLayer)),
      configLayer,
    );

    const payload = {
      id: 'test-order-01',
      inputUrl: FIXTURE_VIDEO,
      outputPrefix: 'test-order-01',
      callbackData: null,
    };

    await expect(
      Effect.runPromise(
        transcodeJob(payload, 'receipt-handle-order').pipe(
          Effect.provide(testLayers),
        ),
      ),
    ).rejects.toThrow();

    // deleteMessage must not appear before job.completed, and must not appear at all if SNS failed
    const deleteIdx = ops.indexOf('sqs:deleteMessage');
    const completedIdx = ops.indexOf('sns:job.completed');
    expect(completedIdx, 'job.completed SNS should have been attempted').toBeGreaterThanOrEqual(0);
    expect(deleteIdx, 'deleteMessage must not be called when job.completed SNS fails').toBe(-1);
  }, 120_000);
});
