import { describe, it, expect, beforeEach, vi } from "vitest";

const { findUniqueMock, upsertMock, enqueueMock, publishMock } = vi.hoisted(
  () => ({
    findUniqueMock: vi.fn(),
    upsertMock: vi.fn(),
    enqueueMock: vi.fn(),
    publishMock: vi.fn(),
  }),
);
vi.mock("../../../db", () => ({
  prisma: {
    project: { findUnique: findUniqueMock },
    dorisProjectTableSplit: { upsert: upsertMock },
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
  handleMissingSplitTable,
  upsertDorisProjectTableSplit,
} from "../dorisProjectTableSplitControl";

const PID = "cmqiwxsca0006pj070fdkn0vd";

beforeEach(() => {
  findUniqueMock.mockReset();
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

describe("classifyMissingSplitTable (write-path 3-way)", () => {
  it("live project → reprovision", async () => {
    findUniqueMock.mockResolvedValue({ id: PID, deletedAt: null });
    expect(await classifyMissingSplitTable(PID)).toBe("reprovision");
  });

  it("deleted project → skip-tombstoned", async () => {
    findUniqueMock.mockResolvedValue({ id: PID, deletedAt: new Date() });
    expect(await classifyMissingSplitTable(PID)).toBe("skip-tombstoned");
  });

  it("missing project row → skip-tombstoned", async () => {
    findUniqueMock.mockResolvedValue(null);
    expect(await classifyMissingSplitTable(PID)).toBe("skip-tombstoned");
  });

  it("PG error → pg-error (never guesses)", async () => {
    findUniqueMock.mockRejectedValue(new Error("connection refused"));
    expect(await classifyMissingSplitTable(PID)).toBe("pg-error");
  });
});

describe("handleMissingSplitTable", () => {
  it("reprovision → re-enqueues provisioning and retries", async () => {
    findUniqueMock.mockResolvedValue({ id: PID, deletedAt: null });
    expect(await handleMissingSplitTable(PID)).toBe("retry");
    expect(enqueueMock).toHaveBeenCalledWith(PID);
  });

  it("pg-error → retry, does NOT re-enqueue (can't decide)", async () => {
    findUniqueMock.mockRejectedValue(new Error("pg down"));
    expect(await handleMissingSplitTable(PID)).toBe("retry");
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it("tombstoned → skip, does NOT resurrect tables", async () => {
    findUniqueMock.mockResolvedValue({ id: PID, deletedAt: new Date() });
    expect(await handleMissingSplitTable(PID)).toBe("skip");
    expect(enqueueMock).not.toHaveBeenCalled();
  });
});
