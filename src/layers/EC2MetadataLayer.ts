import { Context, Effect, Layer } from 'effect';
import { EC2MetadataError } from '../errors/index.js';

const IMDS_BASE = 'http://169.254.169.254';
const TOKEN_TTL = '21600';

export interface EC2MetadataServiceShape {
  getInstanceId(): Effect.Effect<string, EC2MetadataError>;
  getInstanceType(): Effect.Effect<string, EC2MetadataError>;
}

export class EC2MetadataService extends Context.Tag('EC2MetadataService')<
  EC2MetadataService,
  EC2MetadataServiceShape
>() {}

function fetchImds(path: string): Effect.Effect<string, EC2MetadataError> {
  return Effect.tryPromise({
    try: async () => {
      const tokenRes = await fetch(`${IMDS_BASE}/latest/api/token`, {
        method: 'PUT',
        headers: { 'X-aws-ec2-metadata-token-ttl-seconds': TOKEN_TTL },
      });
      if (!tokenRes.ok) {
        throw new Error(`IMDS token request failed: HTTP ${tokenRes.status}`);
      }
      const token = await tokenRes.text();
      const res = await fetch(`${IMDS_BASE}${path}`, {
        headers: { 'X-aws-ec2-metadata-token': token },
      });
      if (!res.ok) {
        throw new Error(`IMDS metadata request failed: HTTP ${res.status}`);
      }
      return res.text();
    },
    catch: (e) => new EC2MetadataError({ field: path, cause: e }),
  });
}

export const EC2MetadataServiceLive: Layer.Layer<
  EC2MetadataService,
  EC2MetadataError
> = Layer.succeed(EC2MetadataService, {
  getInstanceId: () => fetchImds('/latest/meta-data/instance-id'),
  getInstanceType: () => fetchImds('/latest/meta-data/instance-type'),
});

export const EC2MetadataServiceStub: Layer.Layer<EC2MetadataService> =
  Layer.succeed(EC2MetadataService, {
    getInstanceId: () => Effect.succeed('i-test-instance'),
    getInstanceType: () => Effect.succeed('t3.medium'),
  });
