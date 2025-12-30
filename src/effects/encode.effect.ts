import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { FileSystem } from '@effect/platform';
import { Data, Effect, pipe, Stream } from 'effect';
import ffmpeg from 'fluent-ffmpeg';

import { formatBytes } from '@/utils';
import { videoEffectCommand } from './video.effect';

export class FFMpegError extends Data.TaggedError('FFMpegError')<{
  message: string;
  originalError: unknown;
}> {}

export class EncoderError extends Data.TaggedError('EncoderError')<{
  message: string;
  originalError: unknown;
}> {}

interface EncodeParams {
  inputUrl: string;
  externalId: string;
}

// Configuração para cada rendição (resolução)
export interface Rendition {
  width: number;
  height: number;
  videoBitrate: string; // ex: '5000k'
  audioBitrate: string; // ex: '128k'
  profile: string; // ex: 'high'
  level: string; // ex: '4.2'
}

// A "Escada" de qualidade (ABR Ladder)
// Para alta qualidade, usamos bitrate generosos.
const RENDITIONS: Rendition[] = [
  {
    width: 1920,
    height: 1080,
    videoBitrate: '6000k',
    audioBitrate: '192k',
    profile: 'high',
    level: '4.2',
  },
  {
    width: 1280,
    height: 720,
    videoBitrate: '3500k',
    audioBitrate: '128k',
    profile: 'main',
    level: '4.0',
  },
  {
    width: 854,
    height: 480,
    videoBitrate: '1500k',
    audioBitrate: '128k',
    profile: 'main',
    level: '3.1',
  },
];

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
        Effect.logError(`Failed to create directory ${baseDirectory}`, {
          cause: error,
        }),
      ),
    );

    for (const rendition of RENDITIONS) {
      const subDir = path.join(baseDirectory, rendition.height.toString());
      yield* fs.makeDirectory(subDir, { recursive: true }).pipe(
        Effect.tap(() =>
          Effect.logInfo(
            `📁 Sub-directory ${rendition.height} created successfully in _tmp folder.`,
          ),
        ),
        Effect.catchAll((error) =>
          Effect.logError(`Failed to create sub-directory ${subDir}`, {
            cause: error,
          }),
        ),
      );
    }

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
    yield* Stream.fromIterable(RENDITIONS).pipe(
      Stream.zipWithIndex,
      Stream.mapEffect(
        (presetWithIndex) =>
          Effect.gen(function* () {
            const [rendition, _] = presetWithIndex;
            yield* Effect.logInfo(
              `🎥 Encoding resolution ${rendition.height}p`,
            );

            yield* videoEffectCommand({ rendition, inputUrl, baseDirectory });

            yield* Effect.logInfo(
              `✅ Completed encoding resolution ${rendition.height}p.`,
            ).pipe(
              Effect.withSpan('/effect/video/complete', {
                attributes: { resolution: rendition.height },
              }),
            );

            return { rendition };
          }),
        { concurrency: 2 },
      ),
      Stream.runCollect,
      Effect.map((results) => Array.from(results)),
    );

    yield* Effect.logInfo(`🎵 Encoding audio`);

    const audioSubDir = path.join(baseDirectory, 'audio');
    yield* fs.makeDirectory(audioSubDir, { recursive: true }).pipe(
      Effect.tap(() =>
        Effect.logInfo(
          `📁 Sub-directory audio created successfully in _tmp folder.`,
        ),
      ),
      Effect.catchAll((error) =>
        Effect.logError(`Failed to create sub-directory audio`, {
          cause: error,
        }),
      ),
    );

    const audioSegmentPath = path.join(audioSubDir, '%06d.ts');
    const audioPlaylistPath = path.join(audioSubDir, 'playlist.m3u8');

    yield* Effect.async<void, FFMpegError>((resume) => {
      ffmpeg(inputUrl)
        .addOption('-hide_banner', '-y')
        .addOption('-vn') // Remove Vídeo
        .addOption('-sn') // Remove Legendas

        .addOption('-map', '0:a:0') // Pega a primeira trilha de áudio
        // Configuração de Áudio AAC
        .addOption('-c:a', 'aac')
        .addOption('-b:a', '128k') // Bitrate padrão
        .addOption('-ac', '2') // Stereo
        // HLS Settings (Importante: hls_time deve ser igual ao do vídeo para sincronia perfeita)
        .addOption('-hls_time', '4')
        .addOption('-hls_playlist_type', 'vod')
        .addOption('-hls_segment_filename', audioSegmentPath)
        .output(audioPlaylistPath)

        .on('end', () => resume(Effect.void))
        .on('error', (err, _, stderr) => {
          Effect.runSync(
            Effect.logError(`❌ FFmpeg detailed error: ${stderr}`),
          );
          resume(
            Effect.fail(
              new FFMpegError({
                message: `Failed to encode audio`,
                originalError: err,
              }),
            ),
          );
        })
        .run();
    });

    yield* Effect.logInfo('✅ Completed audio encoding.').pipe(
      Effect.withSpan('/effect/video/complete', {
        attributes: { language: 'pt-br' },
      }),
    );

    yield* Effect.logInfo(`🗃️ Creating master.m3u8 playlist`);

    yield* Effect.gen(function* () {
      let content = '#EXTM3U\n#EXT-X-VERSION:3\n';

      // 1. Definição do Grupo de Áudio
      // URI: Onde está a playlist de áudio (relativo ao master)
      // GROUP-ID: Um nome interno para ligar com o vídeo
      content += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-aac",NAME="Português",DEFAULT=YES,AUTOSELECT=YES,URI="audio/playlist.m3u8"\n`;

      // 2. Definição dos Vídeos
      RENDITIONS.forEach((rendition) => {
        const videoBps =
          parseInt(rendition.videoBitrate.replace('k', ''), 10) * 1000;

        // Note o atributo AUDIO="audio-aac". Isso faz o link!
        content += `#EXT-X-STREAM-INF:BANDWIDTH=${videoBps},RESOLUTION=${rendition.width}x${rendition.height},CODECS="avc1.${rendition.profile === 'high' ? '64002a' : '4d401f'}",AUDIO="audio-aac"\n`;
        content += `${rendition.height}/playlist.m3u8\n`;
      });

      const masterPath = path.join(baseDirectory, 'master.m3u8');
      // yield* Effect.tryPromise(() => fs.writeFile(masterPath, content));
      yield* fs.writeFile(masterPath, Buffer.from(content, 'utf-8'));
      yield* Effect.logInfo(`✅ master.m3u8 playlist created.`);
    });

    yield* Effect.logInfo('All worker encoding finished successfully.');

    /**
     * 🟡 TODO - Send notification to SNS - "Encoding finished successfully."
     */

    const endTimeOfEncode = new Date().toJSON();
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
