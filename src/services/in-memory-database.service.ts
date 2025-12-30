import { randomUUID } from 'node:crypto';
import { Context, Data, Effect, Layer, Ref } from 'effect';

export type EncodeStatus =
  | 'PENDING'
  | 'PROCESSING_VIDEO'
  | 'PROCESSING_AUDIO'
  | 'SUCCESS_VIDEO'
  | 'SUCCESS_AUDIO'
  | 'FAILED'
  | 'FINISHED';

export interface Encode {
  id: string;
  inputUrl: string;
  externalId: string;
  status: EncodeStatus;
  progress: number;
  sizeInBytes: number;
  createdAt: string;
  updatedAt: string;
}

export class InMemoryDatabaseServiceError extends Data.TaggedError(
  'InMemoryDatabaseServiceError',
)<{
  cause?: unknown;
  message?: unknown;
}> {}

export class InMemoryDatabaseServiceContext extends Context.Tag(
  'InMemoryDatabaseService',
)<
  InMemoryDatabaseServiceContext,
  {
    readonly create: (
      data: Pick<Encode, 'inputUrl' | 'externalId'>,
    ) => Effect.Effect<Encode, InMemoryDatabaseServiceError>;
    readonly get: (
      encodeId: string,
    ) => Effect.Effect<Encode, InMemoryDatabaseServiceError>;
    readonly update: (
      encodeId: string,
      data: Partial<Omit<Encode, 'id'>>,
    ) => Effect.Effect<Encode, InMemoryDatabaseServiceError>;
    readonly delete: (
      encodeId: string,
    ) => Effect.Effect<void, InMemoryDatabaseServiceError>;
    readonly updateProgress: (
      encodeId: string,
      progress: number,
    ) => Effect.Effect<void, InMemoryDatabaseServiceError>;
    readonly updateStatus: (
      encodeId: string,
      status: EncodeStatus,
    ) => Effect.Effect<void, InMemoryDatabaseServiceError>;
    readonly updateSizeInBytes: (
      encodeId: string,
      sizeInBytes: number,
    ) => Effect.Effect<void, InMemoryDatabaseServiceError>;
  }
>() {}

export const InMemoryDatabaseService = Layer.effect(
  InMemoryDatabaseServiceContext,
  Effect.gen(function* () {
    const db = yield* Ref.make(new Map<string, Encode>());

    const findEncodeOrFail = (encodeId: string, operation: string) =>
      Effect.gen(function* () {
        const mapOfDb = yield* Ref.get(db);
        const encodeInDb = mapOfDb.get(encodeId);

        if (!encodeInDb) {
          return yield* Effect.fail(
            new InMemoryDatabaseServiceError({
              cause: 'Encode not found',
              message: `Failed to find encode with id ${encodeId} to ${operation}`,
            }),
          );
        }

        return encodeInDb;
      });

    const persistEncode = (encode: Encode) =>
      Ref.update(db, (dbMap) => dbMap.set(encode.id, encode));

    const withUpdatedTimestamp = (
      encode: Encode,
      data: Partial<Encode>,
    ): Encode => ({
      ...encode,
      ...data,
      updatedAt: new Date().toISOString(),
    });

    return InMemoryDatabaseServiceContext.of({
      create: (data) =>
        Effect.gen(function* () {
          const encodeData: Encode = {
            ...data,
            id: randomUUID(),
            status: 'PENDING',
            progress: 0,
            sizeInBytes: 0,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          };

          yield* persistEncode(encodeData);
          return encodeData;
        }).pipe(Effect.withSpan('/in-memory-database/create')),

      get: (encodeId) =>
        findEncodeOrFail(encodeId, 'get by id').pipe(
          Effect.withSpan('/in-memory-database/get'),
        ),

      update: (encodeId, data) =>
        Effect.gen(function* () {
          const encode = yield* findEncodeOrFail(encodeId, 'update');
          const updatedEncode = withUpdatedTimestamp(encode, data);
          yield* persistEncode(updatedEncode);
          return updatedEncode;
        }).pipe(Effect.withSpan('/in-memory-database/update')),

      delete: (encodeId) =>
        Effect.gen(function* () {
          yield* findEncodeOrFail(encodeId, 'delete');
          yield* Ref.update(db, (dbMap) => {
            dbMap.delete(encodeId);
            return dbMap;
          });
        }).pipe(Effect.withSpan('/in-memory-database/delete')),

      updateProgress: (encodeId, progress) =>
        Effect.gen(function* () {
          const encode = yield* findEncodeOrFail(encodeId, 'update progress');
          const updatedEncode = withUpdatedTimestamp(encode, { progress });
          yield* persistEncode(updatedEncode);
        }).pipe(Effect.withSpan('/in-memory-database/update-progress')),

      updateStatus: (encodeId, status) =>
        Effect.gen(function* () {
          const encode = yield* findEncodeOrFail(encodeId, 'update status');
          const updatedEncode = withUpdatedTimestamp(encode, { status });
          yield* persistEncode(updatedEncode);
        }).pipe(Effect.withSpan('/in-memory-database/update-status')),

      updateSizeInBytes: (encodeId, sizeInBytes) =>
        Effect.gen(function* () {
          const encode = yield* findEncodeOrFail(
            encodeId,
            'update size in bytes',
          );
          const updatedEncode = withUpdatedTimestamp(encode, { sizeInBytes });
          yield* persistEncode(updatedEncode);
        }).pipe(Effect.withSpan('/in-memory-database/update-size-in-bytes')),
    });
  }),
);
