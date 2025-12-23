import { NodeFileSystem, NodeRuntime } from '@effect/platform-node';
import { Effect } from 'effect';
import { encode } from '@/effects/encode.effect';
import { env } from '@/env';

const url =
  'https://pub-d161d9f2b7ce41e4a08dfd8c7f742dc7.r2.dev/raw/tests/PB.S01E01.150s.1080p.mkv';
const environment = env.NODE_ENV;

console.info(
  `Worker Initiated on ${environment} ${new Date(Date.now()).toISOString()}`,
);

encode({
  inputUrl: url,
  externalId: `manual-${new Date(Date.now()).toISOString()}`,
}).pipe(
  Effect.provide(NodeFileSystem.layer),
  Effect.withSpan('/encode'),
  NodeRuntime.runMain,
);
