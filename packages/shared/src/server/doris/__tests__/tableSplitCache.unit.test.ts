import { describe, it, expect, beforeEach, vi } from "vitest";

// Drive prisma.dorisProjectTableSplit.findMany from the test.
const { findManyMock, createRedisMock } = vi.hoisted(() => ({
  findManyMock: vi.fn(),
  createRedisMock: vi.fn(),
}));
vi.mock("../../../db", () => ({
  prisma: { dorisProjectTableSplit: { findMany: findManyMock } },
}));
vi.mock("../../logger", () => ({
  logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() },
}));
vi.mock("../../redis/redis", () => ({ createNewRedisInstance: createRedisMock }));

import {
  isSplitCacheReady,
  splitProjectInCache,
  refreshSplitCache,
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
    subscribe: vi.fn((_ch: string, cb?: (e: Error | null) => void) => cb?.(null)),
    on: vi.fn(),
    disconnect: vi.fn(),
  };
  madeRedis.push(fake);
  return fake;
};

beforeEach(() => {
  findManyMock.mockReset();
  stopSplitCacheRefresh(); // reset module singletons (timer + pub/sub conns)
  createRedisMock.mockReset();
  createRedisMock.mockImplementation(() => makeFakeRedis());
  madeRedis = [];
  __setSplitSnapshotForTest(null); // cold
});

describe("tableSplitCache", () => {
  it("cold cache: not ready, everything reads not-split", () => {
    expect(isSplitCacheReady()).toBe(false);
    expect(splitProjectInCache(A)).toBe(false);
  });

  it("refreshSplitCache loads only split=true rows and marks ready", async () => {
    findManyMock.mockResolvedValue([{ projectId: A }, { projectId: B }]);
    await refreshSplitCache();

    expect(isSplitCacheReady()).toBe(true);
    expect(splitProjectInCache(A)).toBe(true);
    expect(splitProjectInCache(B)).toBe(true);
    expect(splitProjectInCache("unknown")).toBe(false);

    // Only split=true is queried (the where clause is the cache's contract).
    expect(findManyMock).toHaveBeenCalledWith(
      expect.objectContaining({ where: { split: true } }),
    );
  });

  it("full atomic replace — a project dropped from PG disappears next refresh", async () => {
    findManyMock.mockResolvedValueOnce([{ projectId: A }, { projectId: B }]);
    await refreshSplitCache();
    expect(splitProjectInCache(A)).toBe(true);

    findManyMock.mockResolvedValueOnce([{ projectId: B }]);
    await refreshSplitCache();
    expect(splitProjectInCache(A)).toBe(false); // no negative-cache residue
    expect(splitProjectInCache(B)).toBe(true);
  });

  it("__setSplitSnapshotForTest installs project ids directly", () => {
    __setSplitSnapshotForTest([A]);
    expect(isSplitCacheReady()).toBe(true);
    expect(splitProjectInCache(A)).toBe(true);
    expect(splitProjectInCache(B)).toBe(false);
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
