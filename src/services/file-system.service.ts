import path from 'node:path';
import { FileSystem } from '@effect/platform';
import { Context, Effect, Layer, Stream } from 'effect';

interface CreateDirReturn {
  path: string;
  asStream: (recursivePath: string) => Stream.Stream<Uint8Array, Error>;
  createSubDir: (subPath: string) => string;
  getRecursivePath: (...paths: Array<string>) => string;
}

export class FileSystemServiceContext extends Context.Tag('FileSystemService')<
  FileSystemServiceContext,
  {
    readonly createDir: (
      rootPath: string,
    ) => Effect.Effect<CreateDirReturn, Error, never>;
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
            Effect.map(() => ({
              path: rootPath,
              createSubDir: (subPath) => {
                const subDirectoryPathToCreate = path.join(
                  baseDirectory,
                  rootPath,
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
              getRecursivePath: (...paths) => {
                const recursivePath = path.join(
                  directoryPathToCreate,
                  ...paths,
                );
                return recursivePath;
              },
              asStream: (recursivePath) => {
                const fullPath = path.join(
                  baseDirectory,
                  rootPath,
                  recursivePath,
                );

                return Stream.fromEffect(
                  Effect.gen(function* () {
                    const fileStreamExists = yield* fs.exists(fullPath);
                    if (!fileStreamExists) {
                      return yield* Effect.fail(
                        new Error(`File does not exists in ${fullPath}`),
                      );
                    }

                    yield* Effect.logDebug(
                      `Reading file as stream in ${fullPath}`,
                    );
                    return fullPath;
                  }),
                ).pipe(Stream.flatMap((streamPath) => fs.readFile(streamPath)));
              },
            })),
          );
      },
    });
  }),
);
