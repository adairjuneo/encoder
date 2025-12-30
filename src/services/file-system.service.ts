import path from 'node:path';
import { FileSystem } from '@effect/platform';
import { Context, Data, Effect, Layer, Stream } from 'effect';

export class FileSystemServiceError extends Data.TaggedError(
  'FileSystemServiceError',
)<{
  cause?: unknown;
  message?: unknown;
}> {}

interface CreateDirReturn {
  path: string;
  asStream: (recursivePath: string) => Stream.Stream<Uint8Array, Error>;
  createSubDir: (subPath: string) => string;
  getRecursivePath: (...paths: string[]) => string;
}

export class FileSystemServiceContext extends Context.Tag('FileSystemService')<
  FileSystemServiceContext,
  {
    readonly createDir: (
      rootPath: string,
    ) => Effect.Effect<string, FileSystemServiceError, never>;
    readonly resolveDir: (
      rootPath: string,
    ) => Effect.Effect<CreateDirReturn, Error, never>;
    readonly countFilesInDir: (rootPath: string) => number;
  }
>() {}

export const FileSystemService = Layer.effect(
  FileSystemServiceContext,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDirectory = path.join(process.cwd(), '_tmp');

    return FileSystemServiceContext.of({
      createDir: (rootPath: string) => {
        const directoryPathToCreate = path.join(baseDirectory, rootPath);

        return fs
          .makeDirectory(directoryPathToCreate, { recursive: true })
          .pipe(
            Effect.tap(() =>
              Effect.logInfo(
                '📁 Directory created successfully in _tmp folder.',
              ),
            ),
            Effect.map(() => directoryPathToCreate),
            Effect.catchAll((error) =>
              Effect.fail(
                new FileSystemServiceError({
                  cause: error.cause,
                  message: `Failed to create directory: ${error.message}`,
                }),
              ),
            ),
          );
      },
      resolveDir: (rootPath: string) => {
        const directoryPathToResolve = path.join(baseDirectory, rootPath);
        return fs.exists(directoryPathToResolve).pipe(
          Effect.catchAll((error) =>
            Effect.fail(
              new Error(
                `Directory ${rootPath} does not exists in _tmp folder: ${error.message}`,
              ),
            ),
          ),
          Effect.map(() => ({
            path: rootPath,
            createSubDir: (subPath: string) => {
              const subDirectoryPathToCreate = path.join(
                directoryPathToResolve,
                subPath,
              );
              fs.makeDirectory(subDirectoryPathToCreate, {
                recursive: true,
              }).pipe(
                Effect.tap(() =>
                  Effect.logInfo(
                    '📁 Sub-Directory created successfully in root folder.',
                  ),
                ),
              );
              return subDirectoryPathToCreate;
            },
            getRecursivePath: (...paths: string[]) => {
              const recursivePath = path.join(directoryPathToResolve, ...paths);
              return recursivePath;
            },
            asStream: (recursivePath: string) => {
              const fullPath = path.join(directoryPathToResolve, recursivePath);

              return Stream.fromEffect(
                Effect.gen(function* () {
                  yield* fs.exists(fullPath).pipe(
                    Effect.tap(() => {
                      Effect.logDebug(`Reading file as stream in ${fullPath}`);
                    }),
                    Effect.catchAll((error) =>
                      Effect.fail(
                        new Error(
                          `File does not exists in ${fullPath}: ${error.message}`,
                        ),
                      ),
                    ),
                  );

                  return fullPath;
                }),
              ).pipe(Stream.flatMap((streamPath) => fs.readFile(streamPath)));
            },
          })),
        );
      },
      countFilesInDir: (pathDir: string) => {
        const directoryPathToWatch = path.join(baseDirectory, pathDir);
        let countOfFiles = 0;
        const files = fs.readDirectory(directoryPathToWatch, {
          recursive: true,
        });
        files.pipe(
          Effect.tap((filesInDir) => {
            countOfFiles = filesInDir.length;
          }),
        );
        return countOfFiles;
      },
    });
  }),
);
