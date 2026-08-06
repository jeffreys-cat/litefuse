import { beforeEach, describe, expect, it, vi } from "vitest";

const { findMany } = vi.hoisted(() => ({ findMany: vi.fn() }));

vi.mock("../../../db", () => ({
  prisma: {
    dorisProjectTableSplit: { findMany },
  },
}));

import {
  executeDorisProjectFanout,
  resolveDorisProjectTableTargets,
} from "../crossProjectTableRouting";

describe("cross-project Doris table routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("routes every project to its own split table without a shared target", async () => {
    findMany.mockResolvedValue([
      { projectId: "live1", split: true },
      { projectId: "pending1", split: false },
    ]);

    await expect(
      resolveDorisProjectTableTargets({
        logicalTable: "events_full",
        projectIds: ["legacy1", "live1", "pending1", "live1"],
      }),
    ).resolves.toEqual([
      {
        logicalTable: "events_full",
        physicalTable: "events_full_legacy1",
        projectIds: ["legacy1"],
        split: true,
      },
      {
        logicalTable: "events_full",
        physicalTable: "events_full_live1",
        projectIds: ["live1"],
        split: true,
      },
      {
        logicalTable: "events_full",
        physicalTable: "events_full_pending1",
        projectIds: ["pending1"],
        split: true,
      },
    ]);
  });

  it("does not query PostgreSQL for an empty project list", async () => {
    await expect(
      resolveDorisProjectTableTargets({
        logicalTable: "traces_scalar",
        projectIds: [],
      }),
    ).resolves.toEqual([]);
    expect(findMany).not.toHaveBeenCalled();
  });

  it("executes every resolved target and flattens results", async () => {
    findMany.mockResolvedValue([{ projectId: "live1", split: true }]);
    const queryTarget = vi.fn(async (target: { physicalTable: string }) => [
      target.physicalTable,
    ]);

    await expect(
      executeDorisProjectFanout({
        logicalTable: "events_full",
        projectIds: ["legacy1", "live1"],
        queryTarget,
        concurrency: 1,
      }),
    ).resolves.toEqual(["events_full_legacy1", "events_full_live1"]);
    expect(queryTarget).toHaveBeenCalledTimes(2);
  });

  it("returns an empty result for missing split-table read targets", async () => {
    const queryTarget = vi.fn(async () => {
      throw new Error("Table [events_full_legacy1] does not exist");
    });

    await expect(
      executeDorisProjectFanout({
        logicalTable: "events_full",
        projectIds: ["legacy1"],
        queryTarget,
      }),
    ).resolves.toEqual([]);
  });

  it("never exceeds the configured target concurrency", async () => {
    findMany.mockResolvedValue([]);
    let active = 0;
    let maxActive = 0;

    await executeDorisProjectFanout({
      logicalTable: "events_full",
      projectIds: ["one", "two", "three", "four"],
      concurrency: 2,
      queryTarget: async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        active -= 1;
        return [];
      },
    });

    expect(maxActive).toBe(2);
  });

  it("rejects an unsafe project id before building a table name", async () => {
    findMany.mockResolvedValue([]);

    await expect(
      resolveDorisProjectTableTargets({
        logicalTable: "events_full",
        projectIds: ["bad-id"],
      }),
    ).rejects.toThrow("Invalid Doris split-table project id");
  });
});
