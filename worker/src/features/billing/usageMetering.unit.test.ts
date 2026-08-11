import { describe, expect, it, vi } from "vitest";

vi.mock("@langfuse/shared", () => ({
  parseDbOrg: (organization: unknown) => organization,
  Prisma: { DbNull: "DbNull" },
}));

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));

vi.mock("@langfuse/shared/src/server", () => ({
  getBillingCycleBoundaries: (
    org: { cloudBillingCycleAnchor: Date },
    start: Date,
    end: Date,
  ) => {
    const boundary = org.cloudBillingCycleAnchor;
    return boundary > start && boundary < end ? [boundary] : [];
  },
  getBillingUnitCountForProjects: vi.fn(),
  getObservationCountsByProjectInCreationInterval: vi.fn(),
  getScoreCountsByProjectInCreationInterval: vi.fn(),
  getTraceCountsByProjectInCreationInterval: vi.fn(),
  logger: { info: vi.fn() },
}));

vi.mock("../../env", () => ({ env: { STRIPE_SECRET_KEY: "sk_test" } }));

import { buildMeteringSegments } from "./usageMetering";

describe("buildMeteringSegments", () => {
  it("excludes usage recorded before the subscription began", () => {
    expect(
      buildMeteringSegments({
        intervalStart: new Date("2026-07-30T10:00:00.000Z"),
        intervalEnd: new Date("2026-07-30T11:00:00.000Z"),
        meteringStartAt: new Date("2026-07-30T10:37:00.000Z"),
        meteringEndAt: null,
        cycleAnchor: new Date("2026-07-30T10:37:00.000Z"),
      }),
    ).toEqual([
      {
        start: new Date("2026-07-30T10:37:00.000Z"),
        end: new Date("2026-07-30T11:00:00.000Z"),
      },
    ]);
  });

  it("splits an hourly interval at the exact Stripe cycle boundary", () => {
    expect(
      buildMeteringSegments({
        intervalStart: new Date("2026-08-30T10:00:00.000Z"),
        intervalEnd: new Date("2026-08-30T11:00:00.000Z"),
        meteringStartAt: new Date("2026-07-30T10:37:00.000Z"),
        meteringEndAt: null,
        cycleAnchor: new Date("2026-08-30T10:37:00.000Z"),
      }),
    ).toEqual([
      {
        start: new Date("2026-08-30T10:00:00.000Z"),
        end: new Date("2026-08-30T10:37:00.000Z"),
      },
      {
        start: new Date("2026-08-30T10:37:00.000Z"),
        end: new Date("2026-08-30T11:00:00.000Z"),
      },
    ]);
  });

  it("does not report usage after the subscription ends", () => {
    expect(
      buildMeteringSegments({
        intervalStart: new Date("2026-07-30T10:00:00.000Z"),
        intervalEnd: new Date("2026-07-30T11:00:00.000Z"),
        meteringStartAt: new Date("2026-07-01T00:00:00.000Z"),
        meteringEndAt: new Date("2026-07-30T10:12:00.000Z"),
        cycleAnchor: new Date("2026-07-01T00:00:00.000Z"),
      }),
    ).toEqual([
      {
        start: new Date("2026-07-30T10:00:00.000Z"),
        end: new Date("2026-07-30T10:12:00.000Z"),
      },
    ]);
  });
});
