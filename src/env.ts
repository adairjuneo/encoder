import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'test', 'production'])
    .default('development'),
  STORAGE_PROVIDER: z.string(),
  CLOUDFLARE_R2_ACCESS_KEY_ID: z.string(),
  CLOUDFLARE_R2_SECRET_ACCESS_KEY: z.string(),
  CLOUDFLARE_R2_ENDPOINT: z.string().url(),
  CLOUDFLARE_R2_DEFAULT_BUCKET: z.string(),
  AWS_REGION: z.string(),
  ENCODE_QUEUE_URL: z.string().url(),
  ENCODE_REPOSITORY_URL: z.string(),
  WEBHOOKS_SNS_TOPIC_ARN: z.string(),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url(),
  OTEL_EXPORTER_OTLP_HEADERS: z.string(),
});

const _env = envSchema.safeParse(process.env);

if (_env.success === false) {
  console.error(
    console.error('❌ Invalid environment variables.'),
    z.treeifyError(_env.error),
  );

  throw new Error('Invalid environment variables.');
}

export const env = _env.data;
