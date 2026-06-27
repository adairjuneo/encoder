import { Effect } from 'effect';
import { execa } from 'execa';
import { ProbeError } from '../errors/index.js';

// Parses FFmpeg stderr lines that contain "time=HH:MM:SS.ss" progress.
// Returns elapsed seconds as a fraction of totalDuration (0–100), or null
// if the line does not contain time= progress data.
export function parseProgressLine(
  line: string,
  totalDurationSec: number,
): number | null {
  const match = line.match(/time=(\d{2}):(\d{2}):(\d{2}\.\d+)/);
  if (!match) return null;

  const hours = parseInt(match[1], 10);
  const minutes = parseInt(match[2], 10);
  const seconds = parseFloat(match[3]);
  const elapsed = hours * 3600 + minutes * 60 + seconds;

  if (totalDurationSec === 0) return 0;
  return Math.min(100, Math.round((elapsed / totalDurationSec) * 100));
}

// Runs ffprobe to get video duration in seconds.
export function probeVideoDuration(
  inputPath: string,
): Effect.Effect<number, ProbeError> {
  return Effect.tryPromise({
    try: async () => {
      const { stdout } = await execa('ffprobe', [
        '-v',
        'error',
        '-show_entries',
        'format=duration',
        '-of',
        'default=noprint_wrappers=1:nokey=1',
        inputPath,
      ]);
      const duration = parseFloat(stdout.trim());
      if (isNaN(duration))
        throw new Error(`ffprobe returned non-numeric duration: ${stdout}`);
      return duration;
    },
    catch: (e) => new ProbeError({ inputUrl: inputPath, cause: e }),
  });
}
