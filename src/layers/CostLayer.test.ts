import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';
import { CostService, CostServiceStub } from './CostLayer.js';

describe('CostService Stub', () => {
  it('returns a deterministic cost report', async () => {
    const start = new Date('2024-01-01T00:00:00Z');
    const end = new Date('2024-01-01T00:30:00Z'); // 30 min

    const report = await Effect.runPromise(
      Effect.gen(function* () {
        const cost = yield* CostService;
        return yield* cost.calculateJobCost('t3.medium', start, end);
      }).pipe(Effect.provide(CostServiceStub)),
    );

    expect(report.currency).toBe('USD');
    expect(report.durationHours).toBeCloseTo(0.5, 2);
    expect(report.instanceType).toBe('t3.medium');
    expect(typeof report.costCents).toBe('number');
  });
});
