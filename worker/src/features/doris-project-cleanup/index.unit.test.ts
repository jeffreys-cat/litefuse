import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  deleteControl: vi.fn(),
  refreshCache: vi.fn(),
  dropTables: vi.fn(),
  deleteScores: vi.fn(),
  deleteDataset: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  deleteDatasetRunItemsByProjectId: mocks.deleteDataset,
  deleteDorisProjectTableSplit: mocks.deleteControl,
  deleteScoresByProjectId: mocks.deleteScores,
  dropSplitTablesForProject: mocks.dropTables,
  logger: { info: vi.fn() },
  refreshSplitCache: mocks.refreshCache,
}));

import { cleanupDorisProjectData } from ".";

describe("cleanupDorisProjectData", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("drops split tables then clears shared resources without touching shared telemetry", async () => {
    await cleanupDorisProjectData("project1");

    expect(mocks.deleteControl).toHaveBeenCalledWith("project1");
    expect(mocks.refreshCache).toHaveBeenCalledOnce();
    expect(mocks.dropTables).toHaveBeenCalledWith("project1");
    expect(mocks.deleteScores).toHaveBeenCalledWith("project1");
    expect(mocks.deleteDataset).toHaveBeenCalledWith("project1");
    expect(mocks.dropTables.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.deleteScores.mock.invocationCallOrder[0],
    );
  });

  it("is retry-safe when the physical tables are already absent", async () => {
    mocks.dropTables.mockResolvedValueOnce(undefined);

    await expect(cleanupDorisProjectData("project1")).resolves.toBeUndefined();
  });
});
