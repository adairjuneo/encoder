import { Data } from 'effect';

export class ConfigError extends Data.TaggedError('ConfigError')<{
  message: string;
}> {}

export class SQSReceiveError extends Data.TaggedError('SQSReceiveError')<{
  cause: unknown;
}> {}

export class SQSDeleteError extends Data.TaggedError('SQSDeleteError')<{
  cause: unknown;
}> {}

export class HeartbeatError extends Data.TaggedError('HeartbeatError')<{
  cause: unknown;
}> {}

export class SNSPublishError extends Data.TaggedError('SNSPublishError')<{
  cause: unknown;
}> {}

export class R2UploadError extends Data.TaggedError('R2UploadError')<{
  key: string;
  cause: unknown;
}> {}

export class R2PresignError extends Data.TaggedError('R2PresignError')<{
  key: string;
  cause: unknown;
}> {}

export class FFmpegError extends Data.TaggedError('FFmpegError')<{
  resolution: string;
  exitCode: number | null;
  stderr: string;
}> {}

export class ProbeError extends Data.TaggedError('ProbeError')<{
  inputUrl: string;
  cause: unknown;
}> {}

export class EC2MetadataError extends Data.TaggedError('EC2MetadataError')<{
  field: string;
  cause: unknown;
}> {}

export class CostCalculationError extends Data.TaggedError(
  'CostCalculationError',
)<{
  cause: unknown;
}> {}

export class JobPayloadParseError extends Data.TaggedError(
  'JobPayloadParseError',
)<{
  raw: string;
  cause: unknown;
}> {}
