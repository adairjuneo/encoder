import path from 'node:path';
import { Effect } from 'effect';
import ffmpeg from 'fluent-ffmpeg';
import { FFMpegError, type Rendition } from './encode.effect';

interface FfmpegCommandParams {
  inputUrl: string;
  rendition: Rendition;
  baseDirectory: string;
}

export const videoEffectCommand = (params: FfmpegCommandParams) =>
  Effect.gen(function* () {
    const { inputUrl, rendition, baseDirectory } = params;

    const outputDirectory = path.join(
      baseDirectory,
      rendition.height.toString(),
    );
    const playlistPath = path.join(outputDirectory, 'playlist.m3u8');
    const segmentPath = path.join(outputDirectory, '%06d.ts');

    yield* Effect.async<void, FFMpegError>((resume) => {
      ffmpeg(inputUrl)
        .addOption('-hide_banner', '-y')
        .addOption('-preset', 'veryslow')
        // Garantir que todas as rendições tenham keyframes no mesmo timestamp.
        .addOption('-g', '48')
        .addOption('-keyint_min', '48')
        .addOption('-sc_threshold', '0')
        .addOption('-an') // Remove Áudio
        .addOption('-sn') // Remove Legendas
        // Video
        .addOption('-c:v', 'libx264')
        .addOption('-b:v', rendition.videoBitrate)
        .addOption('-maxrate:v', rendition.videoBitrate)
        .addOption('-bufsize:v', `2*${rendition.videoBitrate}`)
        .addOption('-profile:v', rendition.profile)
        .addOption('-level', rendition.level)
        // Scale + Pad para evitar erro de largura ímpar
        .complexFilter(
          `scale=w=${rendition.width}:h=${rendition.height}:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2`,
        )
        // HLS Settings
        .addOption('-hls_time', '4')
        .addOption('-hls_playlist_type', 'vod')
        .addOption('-hls_flags', 'independent_segments')
        .addOption('-hls_segment_filename', segmentPath)
        .output(playlistPath)

        .on('end', () => resume(Effect.void))
        .on('error', (err, _, stderr) => {
          Effect.runSync(
            Effect.logError(`❌ FFmpeg detailed error: ${stderr}`),
          );
          resume(
            Effect.fail(
              new FFMpegError({
                message: `Failed to encode resolution ${rendition.height}p`,
                originalError: err,
              }),
            ),
          );
        })
        .run();
    });
  });
