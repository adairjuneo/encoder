import { Effect, pipe } from 'effect';

import { formatBytes } from '@/utils';

interface EncodeParams {
  inputUrl: string;
  externalId: string;
}

export const encode = (params: EncodeParams) =>
  Effect.gen(function* () {
    const { externalId, inputUrl } = params;

    yield* Effect.logInfo('All process encoded finished with successfully.');

    const endTimeOfEncode = new Date().toISOString();
    const sizeInBytes = 10000;
    const encodeId = 'fake-id-manual-test';

    yield* pipe(
      Effect.logInfo(`🏁 Encoding worker are finished for ${encodeId} at ${endTimeOfEncode}`),
      Effect.annotateLogs({
        inputUrl,
        encodeId,
        externalId,
        sizeInBytes: formatBytes(sizeInBytes),
        costInCents: 0,
      })
    );
  }).pipe(Effect.scoped, Effect.withSpan('/effect/encode'));