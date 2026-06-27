import { Context, Effect, Layer } from "effect"
import { EC2Client, DescribeSpotPriceHistoryCommand, _InstanceType } from "@aws-sdk/client-ec2"
import { ConfigService } from "../config/index.js"
import { CostCalculationError } from "../errors/index.js"
import type { JobCostReport } from "../types/index.js"

export interface CostServiceShape {
  calculateJobCost(
    instanceType: string,
    startedAt:    Date,
    endedAt:      Date
  ): Effect.Effect<JobCostReport, CostCalculationError>
}

export class CostService extends Context.Tag("CostService")<
  CostService,
  CostServiceShape
>() {}

export const CostServiceLive = Layer.effect(
  CostService,
  Effect.gen(function* () {
    const config = yield* ConfigService
    const client = new EC2Client({ region: config.AWS_REGION })

    return {
      calculateJobCost: (instanceType, startedAt, endedAt) =>
        Effect.tryPromise({
          try: async () => {
            const res = await client.send(new DescribeSpotPriceHistoryCommand({
              InstanceTypes:       [instanceType as _InstanceType],
              ProductDescriptions: ["Linux/UNIX"],
              StartTime:           startedAt,
              EndTime:             endedAt,
              MaxResults:          1,
            }))
            const price = parseFloat(res.SpotPriceHistory?.[0]?.SpotPrice ?? "0")
            const durationHours = (endedAt.getTime() - startedAt.getTime()) / 3_600_000
            return {
              costCents:           Math.round(price * durationHours * 100),
              spotPricePerHourUsd: price,
              durationHours,
              instanceType,
              currency: "USD" as const,
            }
          },
          catch: (e) => new CostCalculationError({ cause: e }),
        }),
    }
  })
)

const STUB_SPOT_PRICE_USD = 0.0416 // t3.medium on-demand fallback

export const CostServiceStub: Layer.Layer<CostService> = Layer.succeed(CostService, {
  calculateJobCost: (instanceType, startedAt, endedAt) =>
    Effect.sync(() => {
      const durationHours = (endedAt.getTime() - startedAt.getTime()) / 3_600_000
      return {
        costCents:           Math.round(STUB_SPOT_PRICE_USD * durationHours * 100),
        spotPricePerHourUsd: STUB_SPOT_PRICE_USD,
        durationHours,
        instanceType,
        currency: "USD" as const,
      }
    }),
})
