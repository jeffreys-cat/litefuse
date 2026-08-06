import { describe, it, expect, beforeEach, vi } from "vitest";

const {
  projectFindUniqueMock,
  splitFindUniqueMock,
  splitCreateMock,
  upsertMock,
  enqueueMock,
  publishMock,
} = vi.hoisted(() => ({
  projectFindUniqueMock: vi.fn(),
  splitFindUniqueMock: vi.fn(),
  splitCreateMock: vi.fn(),
  upsertMock: vi.fn(),
  enqueueMock: vi.fn(),
  publishMock: vi.fn(),
}));
vi.mock("../../../db", () => ({
  prisma: {
    project: { findUnique: projectFindUniqueMock },
    dorisProjectTableSplit: {
      create: splitCreateMock,
      findUnique: splitFindUniqueMock,
      upsert: upsertMock,
    },
  },
}));
vi.mock("../../logger", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));
vi.mock("../../redis/dorisSplitTableProvisioningQueue", () => ({
  enqueueDorisSplitTableProvisioning: enqueueMock,
}));
vi.mock("../tableSplitCache", () => ({
  publishSplitCacheInvalidation: publishMock,
}));

import {
  classifyMissingSplitTable,
  ensureProjectSplitDesignated,
  handleMissingSplitTable,
  upsertDorisProjectTableSplit,
} from "../dorisProjectTableSplitControl";

const PID = "cmqiwxsca0006pj070fdkn0vd";

beforeEach(() => {
  projectFindUniqueMock.mockReset();
  splitFindUniqueMock.mockReset();
  splitCreateMock.mockReset();
  upsertMock.mockReset();
  enqueueMock.mockReset();
  publishMock.mockReset();
});

describe("upsertDorisProjectTableSplit", () => {
  it("writes the control row, enqueues provisioning, invalidates cache", async () => {
    await upsertDorisProjectTableSplit({ projectId: PID });
    expect(upsertMock).toHaveBeenCalledTimes(1);
    expect(enqueueMock).toHaveBeenCalledWith(PID);
    expect(publishMock).toHaveBeenCalledTimes(1);
  });

  it("carries NO retention (single-sourced on Project.retentionDays)", async () => {
    await upsertDorisProjectTableSplit({ projectId: PID, split: true });
    const arg = JSON.stringify(upsertMock.mock.calls[0][0]);
    expect(arg).not.toContain("retention");
  });
});

describe("ensureProjectSplitDesignated", () => {
  it("does nothing when the split control row already exists", async () => {
    splitFindUniqueMock.mockResolvedValue({ projectId: PID });

    await ensureProjectSplitDesignated(PID);

    expect(splitCreateMock).not.toHaveBeenCalled();
    expect(enqueueMock).not.toHaveBeenCalled();
    expect(publishMock).not.toHaveBeenCalled();
  });

  it("creates a pending row and enqueues provisioning when missing", async () => {
    splitFindUniqueMock.mockResolvedValue(null);

    await ensureProjectSplitDesignated(PID);

    expect(splitCreateMock).toHaveBeenCalledWith({
      data: {
        projectId: PID,
        split: false,
        note: "auto-designated by all-split ingestion",
      },
    });
    expect(enqueueMock).toHaveBeenCalledWith(PID);
    expect(publishMock).toHaveBeenCalledTimes(1);
  });
});

describe("classifyMissingSplitTable (write-path 3-way)", () => {
  it("live project → reprovision", async () => {
    projectFindUniqueMock.mockResolvedValue({ id: PID, deletedAt: null });
    expect(await classifyMissingSplitTable(PID)).toBe("reprovision");
  });

  it("deleted project → skip-tombstoned", async () => {
    projectFindUniqueMock.mockResolvedValue({ id: PID, deletedAt: new Date() });
    expect(await classifyMissingSplitTable(PID)).toBe("skip-tombstoned");
  });

  it("missing project row → skip-tombstoned", async () => {
    projectFindUniqueMock.mockResolvedValue(null);
    expect(await classifyMissingSplitTable(PID)).toBe("skip-tombstoned");
  });

  it("PG error → pg-error (never guesses)", async () => {
    projectFindUniqueMock.mockRejectedValue(new Error("connection refused"));
    expect(await classifyMissingSplitTable(PID)).toBe("pg-error");
  });
});

describe("handleMissingSplitTable", () => {
  it("reprovision → re-enqueues provisioning and retries", async () => {
    projectFindUniqueMock.mockResolvedValue({ id: PID, deletedAt: null });
    expect(await handleMissingSplitTable(PID)).toBe("retry");
    expect(enqueueMock).toHaveBeenCalledWith(PID);
  });

  it("pg-error → retry, does NOT re-enqueue (can't decide)", async () => {
    projectFindUniqueMock.mockRejectedValue(new Error("pg down"));
    expect(await handleMissingSplitTable(PID)).toBe("retry");
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it("tombstoned → skip, does NOT resurrect tables", async () => {
    projectFindUniqueMock.mockResolvedValue({ id: PID, deletedAt: new Date() });
    expect(await handleMissingSplitTable(PID)).toBe("skip");
    expect(enqueueMock).not.toHaveBeenCalled();
  });
});
