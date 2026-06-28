import { Layer } from "effect"
import { ConfigServiceLive } from "./config/index.js"
import { SQSServiceLive, SQSServiceStub } from "./layers/SQSLayer.js"
import { SNSServiceLive, SNSServiceConsoleLive } from "./layers/SNSLayer.js"
import { R2ServiceLive, R2ServiceStub } from "./layers/R2Layer.js"
import { EC2MetadataServiceLive, EC2MetadataServiceStub } from "./layers/EC2MetadataLayer.js"
import { CostServiceLive, CostServiceStub } from "./layers/CostLayer.js"
import { FFmpegServiceLive } from "./layers/FFmpegLayer.js"

export const AppLayer = Layer.mergeAll(
  ConfigServiceLive,
  SQSServiceLive.pipe(Layer.provide(ConfigServiceLive)),
  SNSServiceLive.pipe(Layer.provide(ConfigServiceLive)),
  R2ServiceLive.pipe(Layer.provide(ConfigServiceLive)),
  EC2MetadataServiceLive,
  CostServiceLive.pipe(Layer.provide(ConfigServiceLive)),
  FFmpegServiceLive.pipe(Layer.provide(ConfigServiceLive)),
)

export const ManualAppLayer = Layer.mergeAll(
  ConfigServiceLive,
  SQSServiceStub({ messages: [] }),
  SNSServiceConsoleLive,
  R2ServiceStub(new Map()),
  EC2MetadataServiceStub,
  CostServiceStub,
  FFmpegServiceLive.pipe(Layer.provide(ConfigServiceLive)),
)
