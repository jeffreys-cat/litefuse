import { describe, it, expect, beforeEach, vi } from "vitest";

// Drive prisma.dorisProjectTableSplit.{findMany,findUnique} from the test.
const { findManyMock, findUniqueMock, createRedisMock } = vi.hoisted(() => ({
  findManyMock: vi.fn(),
  findUniqueMock: vi.fn(),
  createRedisMock: vi.fn(),
}));
vi.mock("../../../db", () => ({
  prisma: {
    dorisProjectTableSplit: {
      findMany: findManyMock,
      findUnique: findUniqueMock,
    },
  },
}));
vi.mock("../../logger", () => ({
  logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() },
}));
vi.mock("../../redis/redis", () => ({
  createNewRedisInstance: createRedisMock,
}));

import {
  isSplitCacheReady,
  splitProjectInCache,
  resolveIngestionSplitState,
  refreshSplitCache,
  initializeSplitCache,
  startSplitCacheRefresh,
  stopSplitCacheRefresh,
  publishSplitCacheInvalidation,
  __setSplitSnapshotForTest,
} from "../tableSplitCache";

const A = "cmqiwxsca0006pj070fdkn0vd";
const B = "cmqiwxsca0006pj070fdkn0ve";

// Fake ioredis connection, one per createNewRedisInstance() call.
type FakeRedis = {
  publish: ReturnType<typeof vi.fn>;
  subscribe: ReturnType<typeof vi.fn>;
  on: ReturnType<typeof vi.fn>;
  disconnect: ReturnType<typeof vi.fn>;
};
let madeRedis: FakeRedis[] = [];
const makeFakeRedis = (publishImpl?: () => Promise<unknown>): FakeRedis => {
  const fake: FakeRedis = {
    publish: publishImpl ? vi.fn(publishImpl) : vi.fn(async () => 1),
    subscribe: vi.fn((_ch: string, cb?: (e: Error | null) => void) =>
      cb?.(null),
    ),
    on: vi.fn(),
    disconnect: vi.fn(),
  };
  madeRedis.push(fake);
  return fake;
};

beforeEach(() => {
  findManyMock.mockReset();
  findUniqueMock.mockReset();
  stopSplitCacheRefresh(); // reset module singletons (timer + pub/sub conns)
  createRedisMock.mockReset();
  createRedisMock.mockImplementation(() => makeFakeRedis());
  madeRedis = [];
  __setSplitSnapshotForTest(null); // cold
});

describe("tableSplitCache", () => {
  it("cold cache: not ready, splitProjectInCache reads not-split", () => {
    expect(isSplitCacheReady()).toBe(false);
    expect(splitProjectInCache(A)).toBe(false);
  });

  it("refreshSplitCache loads ALL rows (live + pending) and marks ready", async () => {
    findManyMock.mockResolvedValue([
      { projectId: A, split: true },
      { projectId: B, split: false }, // pending
    ]);
    await refreshSplitCache();

    expect(isSplitCacheReady()).toBe(true);
    expect(splitProjectInCache(A)).toBe(true); // live
    expect(splitProjectInCache(B)).toBe(false); // pending is NOT "live"
    // No where clause — the cache needs pending (split=false) rows too.
    const call = findManyMock.mock.calls[0][0];
    expect(call.where).toBeUndefined();
  });

  it("startup barrier remains unready after failure and retries until loaded", async () => {
    vi.useFakeTimers();
    try {
      findManyMock
        .mockRejectedValueOnce(new Error("postgres unavailable"))
        .mockResolvedValueOnce([{ projectId: A, split: true }]);

      const ready = initializeSplitCache(10);
      expect(isSplitCacheReady()).toBe(false);
      await vi.advanceTimersByTimeAsync(10);
      await ready;

      expect(isSplitCacheReady()).toBe(true);
      expect(findManyMock).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("resolveIngestionSplitState: live/pending/not_split from cache", async () => {
    __setSplitSnapshotForTest([
      [A, true],
      [B, false],
    ]);
    expect(await resolveIngestionSplitState(A)).toBe("live");
    expect(await resolveIngestionSplitState(B)).toBe("pending");
    // Not in cache → PG fallback: no row → not_split.
    findUniqueMock.mockResolvedValue(null);
    expect(await resolveIngestionSplitState("unknown")).toBe("not_split");
    expect(findUniqueMock).toHaveBeenCalledTimes(1);
    // Negative-cached now → no second PG hit.
    expect(await resolveIngestionSplitState("unknown")).toBe("not_split");
    expect(findUniqueMock).toHaveBeenCalledTimes(1);
  });

  it("resolveIngestionSplitState: PG fallback catches a just-designated pending project", async () => {
    // Warm cache WITHOUT the new project (pub/sub not propagated yet).
    __setSplitSnapshotForTest([[A, true]]);
    findUniqueMock.mockResolvedValue({ split: false }); // pending row exists in PG
    expect(await resolveIngestionSplitState(B)).toBe("pending"); // held, not shared
  });

  it("full atomic replace — a project dropped from PG disappears next refresh", async () => {
    findManyMock.mockResolvedValueOnce([
      { projectId: A, split: true },
      { projectId: B, split: true },
    ]);
    await refreshSplitCache();
    expect(splitProjectInCache(A)).toBe(true);

    findManyMock.mockResolvedValueOnce([{ projectId: B, split: true }]);
    await refreshSplitCache();
    expect(splitProjectInCache(A)).toBe(false); // no residue
    expect(splitProjectInCache(B)).toBe(true);
  });

  it("__setSplitSnapshotForTest installs [pid, split] entries directly", () => {
    __setSplitSnapshotForTest([
      [A, true],
      [B, false],
    ]);
    expect(isSplitCacheReady()).toBe(true);
    expect(splitProjectInCache(A)).toBe(true);
    expect(splitProjectInCache(B)).toBe(false); // pending
  });
});

describe("pub/sub lifecycle (Stage 1 review #7/#8)", () => {
  it("#7: publish returns (does not hang) when the connection never resolves", async () => {
    vi.useFakeTimers();
    try {
      // publish() that never settles — the exact Redis-outage shape (offline
      // queue + maxRetriesPerRequest:null neither reject nor drop).
      createRedisMock.mockImplementation(() =>
        makeFakeRedis(() => new Promise(() => {})),
      );
      const p = publishSplitCacheInvalidation();
      await vi.advanceTimersByTimeAsync(3_000); // past the 2s bound
      await expect(p).resolves.toBeUndefined(); // returned, not wedged
    } finally {
      vi.useRealTimers();
    }
  });

  it("#7: publish resolves promptly on a healthy connection", async () => {
    await expect(publishSplitCacheInvalidation()).resolves.toBeUndefined();
    expect(madeRedis[0]?.publish).toHaveBeenCalledWith(
      "litefuse:doris-split-cache:invalidate",
      "1",
    );
  });

  it("#8: stopSplitCacheRefresh disconnects the dedicated pub/sub connections", async () => {
    findManyMock.mockResolvedValue([]);
    startSplitCacheRefresh(60_000); // creates the subscriber
    await publishSplitCacheInvalidation(); // creates the publisher
    const conns = madeRedis.slice();
    expect(conns.length).toBeGreaterThanOrEqual(2);

    stopSplitCacheRefresh();
    for (const c of conns) expect(c.disconnect).toHaveBeenCalledTimes(1);
  });
});
