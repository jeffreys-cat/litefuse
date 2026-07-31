import type { Cluster, Redis } from "ioredis";
import { prisma } from "../../db";
import { logger } from "../logger";
import { createNewRedisInstance } from "../redis/redis";

/**
 * In-memory snapshot of the doris_project_table_split control table, so that
 * isSplitProject can stay SYNCHRONOUS (it backs tableFor, called at hundreds of
 * sync SQL-build sites — making it async would ripple through the whole query
 * layer). Only relevant when LITEFUSE_DORIS_TABLE_SPLIT_MODE = project_id_with_rule.
 *
 * Design invariants (docs/project-per-table-*.md §二):
 *   - NO negative caching: the snapshot holds ONLY split=true projects; a miss
 *     means "not split". A newly-split project appears at the next refresh; there
 *     is no negative entry that could pin a project to the wrong answer.
 *   - Full atomic replace: each refresh swaps in a brand-new Map. A failed
 *     refresh keeps the LAST good snapshot (logged, never overwrite good data
 *     with an error) — or null on cold start.
 *   - Readiness: snapshot === null means "never loaded". Reads tolerate this
 *     (default to the shared table); the WRITE path must gate on
 *     isSplitCacheReady() and fail-and-retry rather than misroute a split
 *     project's rows to the shared table (wired in Stage 1.3/1.6).
 *
 * Ordering that keeps this safe (Stage 1.2/1.3): the creation hook creates the
 * two BASE tables (events_full_<pid> / traces_scalar_<pid> — fast) BEFORE the
 * control row flips split=true, so once split=true is visible the base tables
 * exist (reads return empty until data; writes have a target). The MV
 * (trace_metrics_agg_<pid>) is provisioned async and gated separately — it only
 * affects the rollup rewrite, not correctness (base queries have a fallback).
 *
 * Staleness window (IMPORTANT — closed in 1.3, not yet here): with periodic
 * refresh alone, a just-flipped split project keeps reading not-split in OTHER
 * processes for up to the refresh interval. If the worker grouper cuts a group
 * during that lag it would route to the SHARED table → stranded rows. The fix
 * is an eager Redis-pub/sub invalidation broadcast (control-table change →
 * every process refreshes immediately) PLUS the write-path fail-and-retry gate
 * on isSplitCacheReady(). Until both land, only flip split=true for a project
 * that is not yet ingesting (new project, provisioned before its first trace).
 */

type SplitEntry = { retentionDays: number | null };
type SplitSnapshot = ReadonlyMap<string, SplitEntry>;

let snapshot: SplitSnapshot | null = null;
let refreshTimer: ReturnType<typeof setInterval> | null = null;

export const DEFAULT_SPLIT_CACHE_REFRESH_MS = 15_000;

/** Whether the cache has ever successfully loaded (write-path gate). */
export const isSplitCacheReady = (): boolean => snapshot !== null;

/** Sync membership test — the hot path behind isSplitProject. */
export const splitProjectInCache = (projectId: string): boolean =>
  snapshot?.has(projectId) ?? false;

/** Per-project retention (days) for a split project, or null (global default). */
export const splitRetentionDays = (projectId: string): number | null =>
  snapshot?.get(projectId)?.retentionDays ?? null;

/** All currently-split project ids (the grouper's PG-split lane candidates). */
export const getSplitProjectIds = (): string[] =>
  snapshot ? [...snapshot.keys()] : [];

/** Load the full split=true set from PG and atomically swap it in. */
export const refreshSplitCache = async (): Promise<void> => {
  const rows = await prisma.dorisProjectTableSplit.findMany({
    where: { split: true },
    select: { projectId: true, retentionDays: true },
  });
  const next = new Map<string, SplitEntry>();
  for (const r of rows)
    next.set(r.projectId, { retentionDays: r.retentionDays });
  snapshot = next;
};

/**
 * Start the periodic refresh loop (idempotent per process). Call once at web
 * and worker boot when the split mode needs the control table. Primes
 * immediately, then refreshes every intervalMs; a failed tick keeps the last
 * good snapshot and is logged.
 */
export const startSplitCacheRefresh = (
  intervalMs: number = DEFAULT_SPLIT_CACHE_REFRESH_MS,
): void => {
  if (refreshTimer) return;
  const tick = async () => {
    try {
      await refreshSplitCache();
    } catch (e) {
      logger.error(
        "doris split-cache refresh failed (keeping last snapshot)",
        e,
      );
    }
  };
  void tick();
  refreshTimer = setInterval(() => void tick(), intervalMs);
  // Never keep the event loop alive just for the refresh timer.
  refreshTimer.unref?.();
  // Eager cross-process invalidation on top of the periodic floor.
  subscribeSplitCacheInvalidation();
};

export const stopSplitCacheRefresh = (): void => {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  // Close the dedicated pub/sub connections (#8). stop() is the shutdown hook,
  // and nothing else disconnects these — the worker shutdown closes only the
  // shared `redis`. Leaving them open leaks the connection AND keeps the event
  // loop alive (the subscriber socket is not unref'd), hanging test runners.
  if (subscriber) {
    subscriber.disconnect();
    subscriber = null;
  }
  if (publisher) {
    publisher.disconnect();
    publisher = null;
  }
};

// ---------------------------------------------------------------------------
// Eager invalidation broadcast (Stage 1.2e / 1.3)
// ---------------------------------------------------------------------------
// Periodic refresh alone leaves a ≤interval window where a just-changed control
// row is stale in OTHER processes. A control-table write publishes on this
// channel; every process's subscriber refreshes immediately, closing the
// cross-process window that would otherwise misroute a freshly-split project.

const INVALIDATION_CHANNEL = "litefuse:doris-split-cache:invalidate";
let publisher: Redis | Cluster | null = null;
let subscriber: Redis | Cluster | null = null;

// Best-effort broadcast: the awaited publish is bounded so a Redis outage can
// never wedge the caller (#7). createNewRedisInstance keeps enableOfflineQueue
// (a publish issued before the connection is ready still flushes) AND
// maxRetriesPerRequest:null (never fails) — together those mean an offline
// publish neither rejects nor drops, it BLOCKS. The 15s periodic refresh is the
// correctness floor, so a timed-out/dropped broadcast only costs propagation
// latency, never consistency — blocking on it is pure downside.
const PUBLISH_TIMEOUT_MS = 2_000;

/** Publish an invalidation so every process refreshes its split-cache now. Call
 * after any doris_project_table_split write (designate / flip split / delete).
 * Best-effort and time-bounded — never blocks the caller on a Redis outage. */
export const publishSplitCacheInvalidation = async (): Promise<void> => {
  if (!publisher) {
    publisher = createNewRedisInstance();
  }
  if (!publisher) return;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), PUBLISH_TIMEOUT_MS);
    timer.unref?.();
  });
  // .catch folds a publish failure — or a LATE rejection after the timeout has
  // already won the race — into a value, so Promise.race never rejects and no
  // unhandled rejection escapes the abandoned publish promise.
  const published = publisher
    .publish(INVALIDATION_CHANNEL, "1")
    .then(() => "ok" as const)
    .catch((e) => {
      logger.error("doris split-cache invalidation publish failed", e);
      return "error" as const;
    });
  const result = await Promise.race([published, timeout]);
  if (timer) clearTimeout(timer);
  if (result === "timeout") {
    logger.warn(
      "doris split-cache invalidation publish timed out (best-effort; 15s refresh backstops)",
    );
  }
};

/** Subscribe to invalidations (dedicated connection — a subscriber cannot run
 * other commands). Idempotent per process; wired into startSplitCacheRefresh. */
export const subscribeSplitCacheInvalidation = (): void => {
  if (subscriber) return;
  // A subscriber must let the SUBSCRIBE queue until the connection is ready
  // (enableOfflineQueue:false would drop it) — it runs no other commands.
  subscriber = createNewRedisInstance();
  if (!subscriber) return;
  subscriber.subscribe(INVALIDATION_CHANNEL, (err) => {
    if (err) {
      logger.error("doris split-cache invalidation subscribe failed", err);
    }
  });
  subscriber.on("message", (channel) => {
    if (channel !== INVALIDATION_CHANNEL) return;
    void refreshSplitCache().catch((e) =>
      logger.error("doris split-cache refresh on invalidation failed", e),
    );
  });
};

/** Test-only: install a snapshot directly (bypasses PG). */
export const __setSplitSnapshotForTest = (
  entries: ReadonlyArray<[string, SplitEntry]> | null,
): void => {
  snapshot = entries === null ? null : new Map(entries);
};
