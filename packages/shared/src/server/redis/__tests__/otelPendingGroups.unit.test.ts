import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  afterEach,
  type TestContext,
} from "vitest";
import Redis from "ioredis";
import { randomUUID } from "crypto";

import {
  registerOtelFile,
  cutOtelGroup,
  reconcileOtelPending,
  acquireOtelGrouperLease,
  renewOtelGrouperLease,
  scanStagedOtelGroups,
  readOtelStagingManifest,
  clearOtelStagingManifest,
  otelPendingDepth,
  otelQuarantineDepth,
  otelPendingOldestAgeMs,
  otelPendingListKey,
  otelStagingKey,
  otelStagedSetKey,
  otelQuarantineKey,
  otelGrouperLockKey,
  otelRegisteredKey,
  computeGroupId,
  eventsFullLabelForGroup,
  sha1Hex,
} from "../otelPendingGroups";
import type { OtelPendingEntryType } from "../../queues";

/**
 * These tests exercise the REAL Lua scripts against the dev Redis
 * (infra:dev:up) — Lua atomicity/fencing semantics cannot be mocked
 * meaningfully. Each test run uses a throwaway shard namespace and cleans up
 * after itself; skipped automatically when Redis is unreachable.
 */

const redis = new Redis({
  host: process.env.REDIS_HOST ?? "127.0.0.1",
  port: Number(process.env.REDIS_PORT ?? 6379),
  password: process.env.REDIS_AUTH || undefined,
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  retryStrategy: () => null,
});

// Availability probe runs in beforeAll (tsc compiles tests as CJS — no
// top-level await); unavailable Redis skips at RUNTIME via ctx.skip().
let redisUp = false;
beforeAll(async () => {
  try {
    await redis.connect();
    redisUp = (await redis.ping()) === "PONG";
  } catch {
    redisUp = false;
  }
});
afterAll(async () => {
  redis.disconnect();
});

/** it(), but runtime-skipped when the dev Redis is unreachable. */
const itR = (name: string, fn: () => Promise<void>) =>
  it(name, async (ctx: TestContext) => {
    if (!redisUp) return ctx.skip();
    await fn();
  });

// Throwaway shard per test for isolation; tracked for cleanup.
const shards: string[] = [];
const freshShard = () => {
  const shard = `otel-test-${randomUUID().slice(0, 8)}`;
  shards.push(shard);
  return shard;
};
afterEach(async () => {
  if (!redisUp) return;
  for (const shard of shards.splice(0)) {
    const staged = await redis.smembers(otelStagedSetKey(shard));
    const keys = [
      otelPendingListKey(shard),
      otelStagedSetKey(shard),
      otelQuarantineKey(shard),
      otelGrouperLockKey(shard),
      ...staged.map((g) => otelStagingKey(shard, g)),
    ];
    // registered keys are content-addressed; flush by scanning our namespace.
    const regPattern = `${otelRegisteredKey(shard, "x").split(":otel-reg:")[0]}:otel-reg:*`;
    const regKeys = await redis.keys(regPattern);
    await redis.del(...keys, ...regKeys);
  }
});

const entry = (
  overrides: Partial<OtelPendingEntryType> = {},
): OtelPendingEntryType => ({
  v: 1,
  fileKey: `otel/p1/2026/07/21/${randomUUID()}.json`,
  size: 1000,
  spanCount: 10,
  ts: Date.now(),
  projectId: "p1",
  publicKey: "pk",
  ...overrides,
});

// flushMs: 0 → the dispatch gate always passes (tests drive ripeness
// explicitly where it matters).
const BIG = {
  targetBytes: 1 << 30,
  targetRows: 1 << 30,
  maxFiles: 100,
  flushMs: 0,
};

const cutAsLeader = async (shard: string, opts: Partial<typeof BIG> = {}) => {
  const token = randomUUID();
  expect(
    await acquireOtelGrouperLease({ redis, shard, token, ttlMs: 10_000 }),
  ).toBe(true);
  return cutOtelGroup({ redis, shard, token, ...BIG, ...opts });
};

describe("otelPendingGroups (real Redis Lua)", () => {
  describe("registerOtelFile (idempotent registration)", () => {
    itR("registers once and absorbs duplicate registrations", async () => {
      const shard = freshShard();
      const e = entry();
      expect(
        await registerOtelFile({ redis, shard, entry: e, ttlMs: 60_000 }),
      ).toBe(true);
      // Same fileKey again (client resend / app-level retry) → absorbed.
      expect(
        await registerOtelFile({ redis, shard, entry: e, ttlMs: 60_000 }),
      ).toBe(false);
      expect(await otelPendingDepth({ redis, shard })).toBe(1);
    });
  });

  describe("cutOtelGroup", () => {
    itR("cuts the whole head into one group with deterministic identity", async () => {
      const shard = freshShard();
      const entries = [entry(), entry(), entry()];
      for (const e of entries) {
        await registerOtelFile({ redis, shard, entry: e, ttlMs: 60_000 });
      }

      const cut = await cutAsLeader(shard);
      expect(cut).not.toBeNull();
      expect(cut!.entries.map((e) => e.fileKey).sort()).toEqual(
        entries.map((e) => e.fileKey).sort(),
      );
      // groupId = sha1(sorted fileKeys) — replayable identity.
      expect(cut!.groupId).toBe(
        computeGroupId(entries.map((e) => e.fileKey)),
      );
      expect(eventsFullLabelForGroup(cut!.groupId)).toBe(
        `lf2_${sha1Hex(`${cut!.groupId}_events_full`)}`,
      );
      // Manifest persisted, members trimmed off the pending list.
      expect(await scanStagedOtelGroups({ redis, shard })).toEqual([
        cut!.groupId,
      ]);
      const manifest = await readOtelStagingManifest({
        redis,
        shard,
        groupId: cut!.groupId,
      });
      expect(manifest!.entries).toHaveLength(3);
      expect(await otelPendingDepth({ redis, shard })).toBe(0);
    });

    itR("stops at the byte target and leaves the tail in pending", async () => {
      const shard = freshShard();
      const e1 = entry({ size: 600 });
      const e2 = entry({ size: 600 });
      const e3 = entry({ size: 600 });
      for (const e of [e1, e2, e3]) {
        await registerOtelFile({ redis, shard, entry: e, ttlMs: 60_000 });
      }

      const cut = await cutAsLeader(shard, { targetBytes: 1000 });
      // 600 + 600 >= 1000 → first two form the group, third stays queued.
      expect(cut!.entries.map((e) => e.fileKey)).toEqual([
        e1.fileKey,
        e2.fileKey,
      ]);
      expect(await otelPendingDepth({ redis, shard })).toBe(1);
    });

    itR("dedups a fileKey that slipped into the list twice", async () => {
      const shard = freshShard();
      const e = entry();
      // Bypass idempotent registration to simulate the raw double-RPUSH.
      await redis.rpush(
        otelPendingListKey(shard),
        JSON.stringify(e),
        JSON.stringify(e),
      );

      const cut = await cutAsLeader(shard);
      expect(cut!.entries).toHaveLength(1);
      // BOTH copies consumed from the list — the duplicate cannot leak into
      // a later group under a different label.
      expect(await otelPendingDepth({ redis, shard })).toBe(0);
    });

    itR("quarantines undecodable entries instead of stalling the shard", async () => {
      const shard = freshShard();
      const good = entry();
      await redis.rpush(otelPendingListKey(shard), "not-json{{{");
      await redis.rpush(otelPendingListKey(shard), JSON.stringify(good));

      const cut = await cutAsLeader(shard);
      expect(cut!.entries.map((e) => e.fileKey)).toEqual([good.fileKey]);
      expect(await otelQuarantineDepth({ redis, shard })).toBe(1);
      expect(await otelPendingDepth({ redis, shard })).toBe(0);
    });

    itR("is fenced: a stale token writes NOTHING", async () => {
      const shard = freshShard();
      await registerOtelFile({ redis, shard, entry: entry(), ttlMs: 60_000 });
      const owner = randomUUID();
      expect(
        await acquireOtelGrouperLease({
          redis,
          shard,
          token: owner,
          ttlMs: 10_000,
        }),
      ).toBe(true);

      const fenced = await cutOtelGroup({
        redis,
        shard,
        token: "stale-token",
        ...BIG,
      });
      expect(fenced).toBeNull();
      expect(await otelPendingDepth({ redis, shard })).toBe(1);
      expect(await scanStagedOtelGroups({ redis, shard })).toEqual([]);
    });

    itR("returns null on an empty list", async () => {
      const shard = freshShard();
      expect(await cutAsLeader(shard)).toBeNull();
    });

    itR("dispatch gate: below-target fresh entries are NOT cut", async () => {
      const shard = freshShard();
      await registerOtelFile({
        redis,
        shard,
        entry: entry({ size: 10, ts: Date.now() }),
        ttlMs: 60_000,
      });
      // Fresh + tiny + high flush timeout → not ripe → nil, zero writes.
      const cut = await cutAsLeader(shard, { flushMs: 60_000 });
      expect(cut).toBeNull();
      expect(await otelPendingDepth({ redis, shard })).toBe(1);
      expect(await scanStagedOtelGroups({ redis, shard })).toEqual([]);
    });

    itR("dispatch gate: flush timeout ripens a below-target group", async () => {
      const shard = freshShard();
      const e = entry({ size: 10, ts: Date.now() - 5_000 });
      await registerOtelFile({ redis, shard, entry: e, ttlMs: 60_000 });
      // Oldest entry has waited 5s > flushMs 1s → ripe despite tiny size.
      const cut = await cutAsLeader(shard, { flushMs: 1_000 });
      expect(cut!.entries.map((x) => x.fileKey)).toEqual([e.fileKey]);
      expect(await otelPendingDepth({ redis, shard })).toBe(0);
    });
  });

  describe("recovery primitives", () => {
    itR("reconcile removes manifest members still sitting in pending", async () => {
      const shard = freshShard();
      const e1 = entry();
      const e2 = entry();
      const raws = [JSON.stringify(e1), JSON.stringify(e2)];
      // Simulate the isolation-without-rollback residue: manifest exists AND
      // the members are still in the list (LTRIM never ran).
      await redis.rpush(otelPendingListKey(shard), ...raws);

      const removed = await reconcileOtelPending({
        redis,
        shard,
        rawEntries: raws,
      });
      expect(removed).toBe(2);
      expect(await otelPendingDepth({ redis, shard })).toBe(0);
      // Idempotent: a second reconcile is a no-op.
      expect(
        await reconcileOtelPending({ redis, shard, rawEntries: raws }),
      ).toBe(0);
    });

    itR("dangling staged member (no manifest) reads as null", async () => {
      const shard = freshShard();
      await redis.sadd(otelStagedSetKey(shard), "ghost-group");
      expect(
        await readOtelStagingManifest({
          redis,
          shard,
          groupId: "ghost-group",
        }),
      ).toBeNull();
    });

    itR("clearOtelStagingManifest drops manifest and index entry", async () => {
      const shard = freshShard();
      await registerOtelFile({ redis, shard, entry: entry(), ttlMs: 60_000 });
      const cut = await cutAsLeader(shard);
      await clearOtelStagingManifest({ redis, shard, groupId: cut!.groupId });
      expect(await scanStagedOtelGroups({ redis, shard })).toEqual([]);
      expect(
        await readOtelStagingManifest({
          redis,
          shard,
          groupId: cut!.groupId,
        }),
      ).toBeNull();
    });
  });

  describe("lease", () => {
    itR("single owner, fence-safe renewal", async () => {
      const shard = freshShard();
      const a = randomUUID();
      const b = randomUUID();
      expect(
        await acquireOtelGrouperLease({ redis, shard, token: a, ttlMs: 5_000 }),
      ).toBe(true);
      expect(
        await acquireOtelGrouperLease({ redis, shard, token: b, ttlMs: 5_000 }),
      ).toBe(false);
      expect(
        await renewOtelGrouperLease({ redis, shard, token: a, ttlMs: 5_000 }),
      ).toBe(true);
      expect(
        await renewOtelGrouperLease({ redis, shard, token: b, ttlMs: 5_000 }),
      ).toBe(false);
    });
  });

  describe("monitoring probes", () => {
    itR("oldest-age: null when empty, ~age when populated, MAX for poison head", async () => {
      const shard = freshShard();
      expect(await otelPendingOldestAgeMs({ redis, shard })).toBeNull();

      const e = entry({ ts: Date.now() - 5_000 });
      await registerOtelFile({ redis, shard, entry: e, ttlMs: 60_000 });
      const age = await otelPendingOldestAgeMs({ redis, shard });
      expect(age).toBeGreaterThanOrEqual(5_000);
      expect(age).toBeLessThan(60_000);

      const poisoned = freshShard();
      await redis.rpush(otelPendingListKey(poisoned), "garbage");
      expect(await otelPendingOldestAgeMs({ redis, shard: poisoned })).toBe(
        Number.MAX_SAFE_INTEGER,
      );
    });
  });
});
