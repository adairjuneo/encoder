import chokidar from 'chokidar';
import { Effect, Stream } from 'effect';

// Returns a Stream that emits the absolute path of each .ts segment file
// added to the watched directory. The stream does not error — chokidar
// watcher errors are ignored (uploads that fail are caught at the R2 layer).
// Callers are responsible for interrupting the stream when transcoding ends.
export function watchDirectory(dir: string): Stream.Stream<string> {
  return Stream.asyncScoped<string>((emit) =>
    Effect.gen(function* () {
      const watcher = chokidar.watch(dir, {
        persistent: true,
        ignoreInitial: true,
        awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
      });

      watcher.on('add', (filePath: string) => {
        if (filePath.endsWith('.ts')) {
          void emit.single(filePath);
        }
      });

      yield* Effect.addFinalizer((_exit) =>
        Effect.promise(() => watcher.close()),
      );
    }),
  );
}
