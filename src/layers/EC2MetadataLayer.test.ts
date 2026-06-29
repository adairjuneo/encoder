import { Effect } from 'effect';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  EC2MetadataService,
  EC2MetadataServiceLive,
  EC2MetadataServiceStub,
} from './EC2MetadataLayer.js';

describe('EC2MetadataService Stub', () => {
  it('returns stub instance ID', async () => {
    const id = await Effect.runPromise(
      Effect.gen(function* () {
        const ec2 = yield* EC2MetadataService;
        return yield* ec2.getInstanceId();
      }).pipe(Effect.provide(EC2MetadataServiceStub)),
    );
    expect(id).toBe('i-test-instance');
  });

  it('returns stub instance type', async () => {
    const type = await Effect.runPromise(
      Effect.gen(function* () {
        const ec2 = yield* EC2MetadataService;
        return yield* ec2.getInstanceType();
      }).pipe(Effect.provide(EC2MetadataServiceStub)),
    );
    expect(type).toBe('t3.medium');
  });
});

describe('EC2MetadataService Live — IMDS error handling', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('fails with EC2MetadataError when IMDSv2 token request returns HTTP 400', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        text: async () => '400 Bad Request',
      }),
    );

    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const ec2 = yield* EC2MetadataService;
          return yield* ec2.getInstanceId();
        }).pipe(Effect.provide(EC2MetadataServiceLive)),
      ),
    ).rejects.toThrow();
  });
});
