import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';
import { ConfigService, ConfigServiceLive } from './index.js';

describe('ConfigService', () => {
  it('parses valid environment variables', async () => {
    process.env.AWS_REGION = 'us-east-1';
    process.env.SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123/test';
    process.env.SQS_VISIBILITY_TIMEOUT_SEC = '300';
    process.env.SQS_HEARTBEAT_INTERVAL_MS = '240000';
    process.env.SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123:test';
    process.env.R2_ACCOUNT_ID = 'acct';
    process.env.R2_ACCESS_KEY_ID = 'key';
    process.env.R2_SECRET_ACCESS_KEY = 'secret';
    process.env.R2_BUCKET_NAME = 'my-bucket';
    process.env.R2_PUBLIC_BASE_URL = 'https://r2.example.com';
    process.env.TEMP_DIR = '/tmp/transcoder';
    process.env.FFMPEG_PRESET = 'veryfast';
    process.env.RESOLUTION_BATCH_SIZE = '2';
    process.env.LOG_LEVEL = 'info';
    process.env.NODE_ENV = 'test';

    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* ConfigService;
      }).pipe(Effect.provide(ConfigServiceLive)),
    );

    expect(config.AWS_REGION).toBe('us-east-1');
    expect(config.SQS_VISIBILITY_TIMEOUT_SEC).toBe(300);
    expect(config.RESOLUTION_BATCH_SIZE).toBe(2);
  });

  it('throws ConfigError when required variable is missing', async () => {
    const saved = process.env.AWS_REGION;
    delete process.env.AWS_REGION;

    await expect(
      Effect.runPromise(Effect.provide(ConfigService, ConfigServiceLive)),
    ).rejects.toThrow();

    process.env.AWS_REGION = saved;
  });

  it('throws ConfigError when NODE_ENV is not a valid enum value', async () => {
    const saved = process.env.NODE_ENV;
    process.env.NODE_ENV = 'staging';

    await expect(
      Effect.runPromise(Effect.provide(ConfigService, ConfigServiceLive)),
    ).rejects.toThrow('Invalid NODE_ENV');

    process.env.NODE_ENV = saved;
  });
});
