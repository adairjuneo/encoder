import fs from 'node:fs/promises';
import path from 'node:path';
import { Data, Effect } from 'effect';
import ffmpeg from 'fluent-ffmpeg';

// --- Domain Definitions ---

// Estrutura do que encontramos no arquivo
interface DetectedAudioTrack {
  index: number; // O índice real no FFmpeg (ex: 0:a:0, 0:a:1)
  streamIndex: number; // O índice global do stream (ex: stream #2)
  language: string; // 'por', 'eng', 'spa' ou 'und'
  label: string; // O nome para mostrar no Player (ex: "Português")
  codec: string;
}

class ProbeError extends Data.TaggedError('ProbeError')<{
  message: string;
  originalError: unknown;
}> {}

// --- Probe Helper ---

export const getAudioTracks = (inputPath: string) =>
  Effect.async<DetectedAudioTrack[], ProbeError>((resume) => {
    ffmpeg.ffprobe(inputPath, (err, metadata) => {
      if (err) {
        resume(
          Effect.fail(
            new ProbeError({ message: 'Falha no FFprobe', originalError: err }),
          ),
        );
        return;
      }

      // Filtra apenas streams de áudio
      const audioStreams = metadata.streams.filter(
        (s) => s.codec_type === 'audio',
      );

      const tracks: DetectedAudioTrack[] = audioStreams.map(
        (stream, internalIndex) => {
          // Tenta extrair idioma das tags ou define 'und' (undefined)
          const lang = stream.tags?.language || 'und';
          const title = stream.tags?.title || stream.tags?.handler_name || lang;

          return {
            index: internalIndex, // 0, 1, 2 (relativo aos áudios)
            streamIndex: stream.index, // Índice absoluto
            language: lang,
            label: title,
            codec: stream.codec_name || 'aac',
          };
        },
      );

      resume(Effect.succeed(tracks));
    });
  });

export const encodeAudioTrack = (
  track: DetectedAudioTrack,
  inputUrl: string,
  baseDirectory: string,
) =>
  Effect.gen(function* (_) {
    // Cria pasta única para cada idioma: audio/por, audio/eng
    // Se não tiver idioma, usa audio/track_0, audio/track_1
    const folderSafeName =
      track.language !== 'und' ? track.language : `track_${track.index}`;

    const outputDir = path.join(baseDirectory, 'audio', folderSafeName);
    const playlistPath = path.join(outputDir, 'playlist.m3u8');
    const segmentPath = path.join(outputDir, 'segment_%03d.ts');

    yield* Effect.tryPromise({
      try: () => fs.mkdir(outputDir, { recursive: true }),
      catch: (_) => new Error(`Erro mkdir audio ${folderSafeName}`),
    });

    yield* Effect.logInfo(
      `🎵 Iniciando Áudio [${track.language.toUpperCase()}]: ${track.label}`,
    );

    yield* Effect.async<void, Error>((resume) => {
      ffmpeg(inputUrl)
        .addOption('-hide_banner', '-y')
        .addOption('-vn') // Sem vídeo
        .addOption('-sn') // Sem legenda

        // A MÁGICA: Mapeia especificamente o índice desta trilha
        // 0:a:0 = Primeiro áudio, 0:a:1 = Segundo áudio...
        .addOption('-map', `0:a:${track.index}`)

        .addOption('-c:a', 'aac')
        .addOption('-b:a', '128k')
        .addOption('-ac', '2')

        .addOption('-hls_time', '4')
        .addOption('-hls_playlist_type', 'vod')
        .addOption('-hls_segment_filename', segmentPath)

        .output(playlistPath)

        .on('end', () => resume(Effect.void))
        .on('error', (err) =>
          resume(Effect.fail(new Error(`Erro audio ${track.language}`))),
        )
        .run();
    });
  });
