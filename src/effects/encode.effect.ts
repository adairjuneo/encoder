import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { FileSystem } from '@effect/platform';
import { Effect, pipe } from 'effect';
import { formatBytes } from '@/utils';

interface EncodeParams {
  inputUrl: string;
  externalId: string;
}

const tmpPath = '_tmp';

export const encode = (params: EncodeParams) =>
  Effect.gen(function* () {
    const { externalId, inputUrl } = params;
    const encodeId = randomUUID(); // 🔴 TODO - Alterar aqui para o id que será retornado do in-memory-database.
    const startTimeOfEncode = new Date().toJSON(); // 🔴 TODO - Alterar aqui para data de inicio gravada no in-memory-database.

    /**
     * 🟢 TODO - Criar pasta local no diretório _tmp para jogar os arquivos do encoding.
     */
    const fs = yield* FileSystem.FileSystem;
    const baseDirectory = path.join(process.cwd(), tmpPath, encodeId);
    yield* fs.makeDirectory(baseDirectory, { recursive: true }).pipe(
      Effect.tap(() =>
        Effect.logInfo('📁 Directory created successfully in _tmp folder.'),
      ),
      Effect.catchAll((error) =>
        Effect.logError(`Failed to create directory ${encodeId}`, {
          cause: error,
        }),
      ),
    );

    /**
     * 🔴 TODO - Criar index no banco de dados em memória para armazenar dados sobre o encoding.
     */
    yield* Effect.logInfo(
      `💾 Create index register in memory database with id ${encodeId}`,
    );

    /**
     * 🟡 TODO - Send notification to SNS - "Encoding will start the processing."
     */

    yield* Effect.logInfo(
      `🔄️ Encoding worker will start for ${encodeId} at ${startTimeOfEncode}`,
    );
    /**
     * 🔴 TODO - Chamar o effect de encode de vídeo para iniciar o processo.
     */

    yield* Effect.logInfo('All worker encoding finished successfully.');

    /**
     * 🟡 TODO - Send notification to SNS - "Encoding finished successfully."
     */

    const endTimeOfEncode = new Date().toISOString();
    const sizeInBytes = 10000;

    yield* pipe(
      Effect.logInfo(
        `🏁 Encoding worker are finished for ${encodeId} at ${endTimeOfEncode}`,
      ),
      Effect.annotateLogs({
        inputUrl,
        encodeId,
        externalId,
        sizeInBytes: formatBytes(sizeInBytes),
        costInCents: 0,
      }),
    );
  }).pipe(Effect.scoped, Effect.withSpan('/effect/encode'));
