import { Context, Effect, Layer } from 'effect';
import { ConfigError } from '../errors/index.js';

export interface AppConfig {
  NODE_ENV: 'development' | 'test' | 'production';
  AWS_REGION: string;
  SQS_QUEUE_URL: string;
  SQS_VISIBILITY_TIMEOUT_SEC: number;
  SQS_HEARTBEAT_INTERVAL_MS: number;
  SNS_TOPIC_ARN: string;
  R2_ACCOUNT_ID: string;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
  R2_BUCKET_NAME: string;
  R2_PUBLIC_BASE_URL: string;
  TEMP_DIR: string;
  FFMPEG_PRESET: 'ultrafast' | 'veryfast' | 'medium';
  RESOLUTION_BATCH_SIZE: number;
  LOG_LEVEL: string;
}

export class ConfigService extends Context.Tag('ConfigService')<
  ConfigService,
  AppConfig
>() {}

function required(key: string): Effect.Effect<string, ConfigError> {
  const val = process.env[key];
  if (!val)
    return Effect.fail(
      new ConfigError({ message: `Missing required env var: ${key}` }),
    );
  return Effect.succeed(val);
}

function optionalInt(key: string, fallback: number): number {
  const val = process.env[key];
  if (!val) return fallback;
  const n = parseInt(val, 10);
  return isNaN(n) ? fallback : n;
}

function optionalStr(key: string, fallback: string): string {
  return process.env[key] ?? fallback;
}

export const ConfigServiceLive = Layer.effect(
  ConfigService,
  Effect.gen(function* () {
    const AWS_REGION = yield* required('AWS_REGION');
    const SQS_QUEUE_URL = yield* required('SQS_QUEUE_URL');
    const SNS_TOPIC_ARN = yield* required('SNS_TOPIC_ARN');
    const R2_ACCOUNT_ID = yield* required('R2_ACCOUNT_ID');
    const R2_ACCESS_KEY_ID = yield* required('R2_ACCESS_KEY_ID');
    const R2_SECRET_ACCESS_KEY = yield* required('R2_SECRET_ACCESS_KEY');
    const R2_BUCKET_NAME = yield* required('R2_BUCKET_NAME');
    const R2_PUBLIC_BASE_URL = yield* required('R2_PUBLIC_BASE_URL');

    const nodeEnv = process.env.NODE_ENV ?? 'development';
    if (!(['development', 'test', 'production'] as const).includes(nodeEnv as AppConfig['NODE_ENV'])) {
      yield* Effect.fail(
        new ConfigError({ message: `Invalid NODE_ENV: ${nodeEnv}` }),
      );
    }

    const preset = optionalStr('FFMPEG_PRESET', 'veryfast');
    if (!['ultrafast', 'veryfast', 'medium'].includes(preset)) {
      yield* Effect.fail(
        new ConfigError({ message: `Invalid FFMPEG_PRESET: ${preset}` }),
      );
    }

    return {
      NODE_ENV: nodeEnv as AppConfig['NODE_ENV'],
      AWS_REGION,
      SQS_QUEUE_URL,
      SQS_VISIBILITY_TIMEOUT_SEC: optionalInt(
        'SQS_VISIBILITY_TIMEOUT_SEC',
        300,
      ),
      SQS_HEARTBEAT_INTERVAL_MS: optionalInt(
        'SQS_HEARTBEAT_INTERVAL_MS',
        240_000,
      ),
      SNS_TOPIC_ARN,
      R2_ACCOUNT_ID,
      R2_ACCESS_KEY_ID,
      R2_SECRET_ACCESS_KEY,
      R2_BUCKET_NAME,
      R2_PUBLIC_BASE_URL,
      TEMP_DIR: optionalStr('TEMP_DIR', '/tmp/transcoder'),
      FFMPEG_PRESET: preset as AppConfig['FFMPEG_PRESET'],
      RESOLUTION_BATCH_SIZE: optionalInt('RESOLUTION_BATCH_SIZE', 2),
      LOG_LEVEL: optionalStr('LOG_LEVEL', 'info'),
    } satisfies AppConfig;
  }),
);
