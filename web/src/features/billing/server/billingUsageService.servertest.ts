/** @jest-environment node */

import { prisma } from "@langfuse/shared/src/db";
import {
  getBillingCycleStart,
  getBillingUnitCountForProjects,
  logger,
} from "@langfuse/shared/src/server";
import {
  getFreshBillingUsage,
  getPaidBillingUsage,
} from "./billingUsageService";

jest.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    organization: { update: jest.fn() },
    cronJobs: { findUnique: jest.fn() },
    billingMeterBackup: { aggregate: jest.fn() },
  },
}));

jest.mock("@langfuse/shared/src/server", () => ({
  BILLING_METER_EVENT_NAME: "litefuse_units",
  CLOUD_USAGE_METERING_CRON_NAME: "cloud-usage-metering-hourly",
  getBillingCycleStart: jest.fn(),
  getBillingUnitCountForProjects: jest.fn(),
  startOfDayUTC: jest.fn((date: Date) => date),
  logger: { debug: jest.fn(), warn: jest.fn() },
  redis: undefined,
}));

const mockedGetBillingCycleStart = jest.mocked(getBillingCycleStart);
const mockedGetBillingUnitCount = jest.mocked(getBillingUnitCountForProjects);
const mockedUpdateOrganization = jest.mocked(prisma.organization.update);
const mockedLoggerWarn = jest.mocked(logger.warn);
const mockedFindCron = jest.mocked(prisma.cronJobs.findUnique);
const mockedAggregateBackups = jest.mocked(prisma.billingMeterBackup.aggregate);

const now = new Date("2026-07-22T10:36:55.000Z");
const cycleStart = new Date("2026-07-22T00:00:00.000Z");

function organization(updatedAt: Date | null = null) {
  return {
    id: "org_test",
    name: "Test organization",
    createdAt: new Date("2026-01-22T00:00:00.000Z"),
    updatedAt: now,
    cloudConfig: {},
    metadata: {},
    cloudBillingCycleAnchor: cycleStart,
    cloudBillingCycleUpdatedAt: updatedAt,
    cloudCurrentCycleUsage: 7,
    cloudFreeTierUsageThresholdState: null,
    aiFeaturesEnabled: false,
    projects: [{ id: "project_a" }, { id: "project_b" }],
  };
}

describe("getFreshBillingUsage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockedGetBillingCycleStart.mockReturnValue(cycleStart);
    mockedUpdateOrganization.mockResolvedValue({} as never);
    mockedFindCron.mockResolvedValue(null);
    mockedAggregateBackups.mockResolvedValue({
      _sum: { aggregatedValue: null },
    } as never);
  });

  it("recalculates stale usage for only the organization's projects", async () => {
    mockedGetBillingUnitCount.mockResolvedValue({
      traces: 2,
      observations: 3,
      scores: 4,
      total: 9,
    });

    await expect(
      getFreshBillingUsage({ organization: organization(), now }),
    ).resolves.toEqual({ currentUnits: 9, updatedAt: now });

    expect(mockedGetBillingUnitCount).toHaveBeenCalledWith({
      projectIds: ["project_a", "project_b"],
      start: cycleStart,
      end: now,
    });
    expect(mockedUpdateOrganization).not.toHaveBeenCalled();
  });

  it("uses the cached value while it is fresh", async () => {
    const updatedAt = new Date(now.getTime() - 60_000);

    await expect(
      getFreshBillingUsage({ organization: organization(updatedAt), now }),
    ).resolves.toEqual({ currentUnits: 7, updatedAt });

    expect(mockedGetBillingUnitCount).not.toHaveBeenCalled();
    expect(mockedUpdateOrganization).not.toHaveBeenCalled();
  });

  it("keeps the cached value when the analytics query fails", async () => {
    mockedGetBillingUnitCount.mockRejectedValue(new Error("Doris unavailable"));

    await expect(
      getFreshBillingUsage({ organization: organization(), now }),
    ).resolves.toEqual({ currentUnits: 7, updatedAt: null });

    expect(mockedUpdateOrganization).not.toHaveBeenCalled();
    expect(mockedLoggerWarn).toHaveBeenCalledWith(
      "Unable to refresh organization billing usage",
      expect.objectContaining({ orgId: "org_test" }),
    );
  });

  it("combines reported Stripe usage with the unreported tail", async () => {
    const reportedThrough = new Date("2026-07-22T10:00:00.000Z");
    mockedFindCron.mockResolvedValue({ lastRun: reportedThrough } as never);
    mockedAggregateBackups.mockResolvedValue({
      _sum: { aggregatedValue: 100 },
    } as never);
    mockedGetBillingUnitCount.mockResolvedValue({
      traces: 2,
      observations: 3,
      scores: 4,
      total: 9,
    });

    await expect(
      getPaidBillingUsage({
        organization: organization(),
        customerId: "cus_test",
        now,
      }),
    ).resolves.toEqual({
      currentUnits: 109,
      reportedUnits: 100,
      pendingUnits: 9,
      reportedThrough,
      updatedAt: now,
    });
  });

  it("starts paid usage at the Stripe period anchor after a Test Clock advance", async () => {
    const stripeCycleStart = new Date("2026-09-10T04:41:00.000Z");
    const applicationNow = new Date("2026-08-11T12:00:00.000Z");
    const org = {
      ...organization(),
      cloudBillingCycleAnchor: stripeCycleStart,
    };
    mockedFindCron.mockResolvedValue({
      lastRun: stripeCycleStart,
    } as never);
    mockedAggregateBackups.mockResolvedValue({
      _sum: { aggregatedValue: null },
    } as never);
    mockedGetBillingUnitCount.mockResolvedValue({
      traces: 0,
      observations: 0,
      scores: 0,
      total: 0,
    });

    await getPaidBillingUsage({
      organization: org,
      customerId: "cus_test",
      now: applicationNow,
    });

    expect(mockedAggregateBackups).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          startTime: { gte: stripeCycleStart },
          endTime: { lte: stripeCycleStart },
        }),
      }),
    );
    expect(mockedGetBillingUnitCount).toHaveBeenCalledWith({
      projectIds: ["project_a", "project_b"],
      start: stripeCycleStart,
      end: stripeCycleStart,
    });
  });
});
