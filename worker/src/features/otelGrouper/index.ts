import { randomUUID } from "crypto";
import { Cluster, Redis } from "ioredis";

import {
  OtelIngestionQueue,
  QueueJobs,
  createNewRedisInstance,
  redisQueueRetryOptions,
  logger,
  recordGauge,
  recordIncrement,
  acquireOtelGrouperLease,
  renewOtelGrouperLease,
  cutOtelGroup,
  scanStagedOtelGroups,
  readOtelStagingManifest,
  clearOtelStagingManifest,
  reconcileOtelPending,
  otelPendingDepth,
  otelQuarantineDepth,
  otelPendingOldestAgeMs,
  type OtelGroupCut,
} from "@langfuse/shared/src/server";
import { env } from "../../env";
import { env as sharedEnv } from "@langfuse/shared/src/env";

type RedisHandle = Redis | Cluster;

export type OtelGrouperConfig = {
  targetBytes: number;
  targetRows: number;
  maxFiles: number;
  flushMs: number;
  lockTtlMs: number;
  tickMs: number;
};

type Deps = {
  redis?: RedisHandle;
  /** Test seam — replaces the BullMQ publish. */
  addGroupJob?: (shard: string, cut: OtelGroupCut) => Promise<void>;
  config?: Partial<OtelGrouperConfig>;
  shardNames?: string[];
};

/**
 * The otel grouper (exactly-once design §3.2): a resident candidate loop in
 * EVERY worker. Per shard, a Redis lease elects one leader; the leader's tick
 *
 *   1. recovers leftover staging manifests (periodic — not only on takeover:
 *      an add-failure on a healthy leader must self-heal too),
 *   2. invokes the atomic cut Lua (fence + dispatch gate + member decision +
 *      manifest persistence + LTRIM in ONE script),
 *   3. publishes the group job (jobId = groupId — replays dedup) and clears
 *      the manifest.
 *
 * Losing the lease, crashing, or a failed publish at ANY point leaves either
 * nothing (list untouched) or a manifest that the next tick / next leader
 * republishes verbatim. Non-leaders stay useful: they emit the shard gauges
 * (pending depth/age, quarantine, staged count) so a dead leader shows up as
 * an alarming metric, never as silent metric absence.
 *
 * Lifecycle: runs whenever the otel queue consumer is enabled — deliberately
 * NOT gated on LITEFUSE_OTEL_GROUPING_ENABLED (web-side registration switch):
 * after a rollback flips registration off, the resident grouper drains the
 * pending backlog instead of stranding it.
 */
export class OtelGrouper {
  private readonly token = randomUUID();
  private readonly cfg: OtelGrouperConfig;
  private readonly shardNames: string[];
  private readonly addGroupJob: (
    shard: string,
    cut: OtelGroupCut,
  ) => Promise<void>;
  private redis: RedisHandle | null = null;
  private ownsRedis = false;
  private timer: NodeJS.Timeout | null = null;
  private stopped = true;
  private tickInFlight: Promise<void> | null = null;
  private tickCount = 0;
  /** shard → we believe we currently hold its lease (renew before re-acquire). */
  private held = new Set<string>();

  constructor(private readonly deps: Deps = {}) {
    this.cfg = {
      targetBytes: env.LITEFUSE_OTEL_GROUP_TARGET_BYTES,
      targetRows: env.LITEFUSE_OTEL_GROUP_TARGET_ROWS,
      maxFiles: env.LITEFUSE_OTEL_GROUP_MAX_FILES,
      flushMs: env.LITEFUSE_OTEL_GROUP_FLUSH_MS,
      lockTtlMs: env.LITEFUSE_OTEL_GROUPER_LOCK_TTL_MS,
      tickMs: env.LITEFUSE_OTEL_GROUPER_TICK_MS,
      ...deps.config,
    };
    this.shardNames = deps.shardNames ?? OtelIngestionQueue.getShardNames();
    this.addGroupJob = deps.addGroupJob ?? this.publishToQueue.bind(this);
  }

  public async start(): Promise<void> {
    if (!this.stopped) return;
    this.stopped = false;

    if (this.deps.redis) {
      this.redis = this.deps.redis;
      this.ownsRedis = false;
    } else {
      this.redis = createNewRedisInstance({
        enableOfflineQueue: false,
        ...redisQueueRetryOptions,
      });
      this.ownsRedis = true;
    }
    if (!this.redis) {
      logger.error("[OtelGrouper] no Redis available — grouper NOT started");
      this.stopped = true;
      return;
    }

    // Deployment-contract self-check (design §5.2/L4): with an eviction
    // policy other than noeviction, Redis may silently evict pending entries,
    // staging manifests and registered keys — losing registered data and
    // breaking idempotency with no error signal anywhere. Refuse to run.
    if (!(await this.assertNoEviction(this.redis))) {
      this.stopped = true;
      if (this.ownsRedis) this.redis.disconnect();
      this.redis = null;
      return;
    }

    logger.info(
      `[OtelGrouper] started (shards=${this.shardNames.join(",")}, target=${this.cfg.targetBytes}B/${this.cfg.targetRows}rows/${this.cfg.maxFiles}files, flush=${this.cfg.flushMs}ms)`,
    );
    this.scheduleNext();
  }

  public async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    // Let an in-flight tick finish — its writes are all idempotent, but a
    // clean drain keeps shutdown logs readable.
    if (this.tickInFlight) await this.tickInFlight.catch(() => {});
    if (this.ownsRedis && this.redis) this.redis.disconnect();
    this.redis = null;
    logger.info("[OtelGrouper] stopped");
  }

  // -------------------------------------------------------------------------

  private scheduleNext(): void {
    if (this.stopped) return;
    // ±30% jitter de-synchronizes the candidate herd across workers.
    const jitter = 0.7 + Math.random() * 0.6;
    this.timer = setTimeout(
      () => {
        this.tickInFlight = this.tickAll()
          .catch((e) => {
            logger.error(
              `[OtelGrouper] tick failed: ${e instanceof Error ? e.message : String(e)}`,
            );
          })
          .finally(() => {
            this.tickInFlight = null;
            this.scheduleNext();
          });
      },
      Math.round(this.cfg.tickMs * jitter),
    );
  }

  private async tickAll(): Promise<void> {
    if (!this.redis) return;
    this.tickCount++;
    // Gauges from EVERY candidate every ~5s (leadership-independent — a dead
    // leader must surface as a rising gauge, not as gauge absence).
    const emitGauges = this.tickCount % Math.max(1, Math.round(5_000 / this.cfg.tickMs)) === 0;
    for (const shard of this.shardNames) {
      if (this.stopped) return;
      try {
        if (emitGauges) await this.emitShardGauges(shard);
        await this.tickShard(shard);
      } catch (e) {
        logger.error(
          `[OtelGrouper] shard ${shard} tick failed: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
  }

  private async tickShard(shard: string): Promise<void> {
    const redis = this.redis!;
    if (!(await this.acquireOrRenew(shard))) return; // not the leader

    await this.recoverStaged(shard);

    const cut = await cutOtelGroup({
      redis,
      shard,
      token: this.token,
      targetBytes: this.cfg.targetBytes,
      targetRows: this.cfg.targetRows,
      maxFiles: this.cfg.maxFiles,
      flushMs: this.cfg.flushMs,
    }).catch(async (e) => {
      // Lua is isolation-without-rollback: an error may have left a manifest
      // whose members still sit in the list. Reconcile-before-recut via the
      // standard recovery path (it LREMs manifest members), then stop this
      // tick — the next tick republishes and cuts fresh.
      logger.error(
        `[OtelGrouper] cut failed on ${shard} (reconciling): ${e instanceof Error ? e.message : String(e)}`,
      );
      await this.recoverStaged(shard);
      return null;
    });
    if (!cut) return;

    recordIncrement("langfuse.otel_grouper.groups_cut", 1, { shard });
    recordIncrement("langfuse.otel_grouper.files_grouped", cut.entries.length, {
      shard,
    });
    await this.publish(shard, cut);
  }

  /**
   * Publish a decided group. Failure is NOT an error state: the manifest
   * stays in staging and the next tick's recovery republishes it (jobId
   * dedup + load label make every retry idempotent).
   */
  private async publish(shard: string, cut: OtelGroupCut): Promise<void> {
    try {
      await this.addGroupJob(shard, cut);
    } catch (e) {
      recordIncrement("langfuse.otel_grouper.publish_errors", 1, { shard });
      logger.error(
        `[OtelGrouper] publish failed for group ${cut.groupId} on ${shard} (manifest retained for republish): ${e instanceof Error ? e.message : String(e)}`,
      );
      return;
    }
    await clearOtelStagingManifest({
      redis: this.redis!,
      shard,
      groupId: cut.groupId,
    });
  }

  /**
   * Republish leftover staging manifests. Runs EVERY leader tick: covers the
   * grouper crash between cut and publish, a publish failure on a healthy
   * leader, and (via reconcile) the partial-cut residue of a mid-script
   * error. A nil manifest for a scanned groupId only means a concurrent
   * clear between HKEYS and HGET (already published) — skip.
   */
  private async recoverStaged(shard: string): Promise<void> {
    const redis = this.redis!;
    const groupIds = await scanStagedOtelGroups({ redis, shard });
    for (const groupId of groupIds) {
      try {
        const manifest = await readOtelStagingManifest({
          redis,
          shard,
          groupId,
        });
        if (!manifest) continue;
        // Self-heal the isolation-without-rollback case (manifest written,
        // LTRIM never ran): idempotent LREM of every manifest member.
        await reconcileOtelPending({
          redis,
          shard,
          rawEntries: manifest.rawEntries,
        });
        recordIncrement("langfuse.otel_grouper.republished", 1, { shard });
        await this.publish(shard, manifest);
      } catch (e) {
        logger.error(
          `[OtelGrouper] recovery failed for group ${groupId} on ${shard}: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
  }

  private async acquireOrRenew(shard: string): Promise<boolean> {
    const redis = this.redis!;
    const { lockTtlMs } = this.cfg;
    if (this.held.has(shard)) {
      if (
        await renewOtelGrouperLease({
          redis,
          shard,
          token: this.token,
          ttlMs: lockTtlMs,
        })
      ) {
        return true;
      }
      this.held.delete(shard); // lost it (expiry/takeover) — fall through
    }
    const acquired = await acquireOtelGrouperLease({
      redis,
      shard,
      token: this.token,
      ttlMs: lockTtlMs,
    });
    if (acquired) this.held.add(shard);
    return acquired;
  }

  private async emitShardGauges(shard: string): Promise<void> {
    const redis = this.redis!;
    const [depth, quarantine, oldestAge, staged] = await Promise.all([
      otelPendingDepth({ redis, shard }),
      otelQuarantineDepth({ redis, shard }),
      otelPendingOldestAgeMs({ redis, shard }),
      scanStagedOtelGroups({ redis, shard }),
    ]);
    recordGauge("langfuse.otel_grouper.pending_depth", depth, { shard });
    recordGauge("langfuse.otel_grouper.quarantine_depth", quarantine, {
      shard,
    });
    recordGauge("langfuse.otel_grouper.staging_count", staged.length, {
      shard,
    });
    recordGauge(
      "langfuse.otel_grouper.pending_oldest_age_ms",
      oldestAge ?? 0,
      { shard },
    );

    // Queue-side health (design §6.3): wait depth = consumption keeping up;
    // failed depth + oldest failed age = the DLQ/age-guard chain's SLA input
    // (failed_oldest_age alert must fire well before the label retention
    // window closes the redrive path). Only for REAL configured shards — the
    // tests' throwaway shard names must not summon queue connections.
    if (!OtelIngestionQueue.getShardNames().includes(shard)) return;
    try {
      const queue = OtelIngestionQueue.getInstance({ shardName: shard });
      if (!queue) return;
      const counts = await queue.getJobCounts("wait", "failed");
      recordGauge("langfuse.otel_queue.wait_depth", counts.wait ?? 0, {
        shard,
      });
      recordGauge("langfuse.otel_queue.failed_depth", counts.failed ?? 0, {
        shard,
      });
      const oldestFailed = await queue.getJobs(["failed"], 0, 0, true);
      recordGauge(
        "langfuse.otel_queue.failed_oldest_age_ms",
        oldestFailed[0]?.timestamp
          ? Date.now() - oldestFailed[0].timestamp
          : 0,
        { shard },
      );
    } catch (e) {
      logger.debug(
        `[OtelGrouper] queue gauge emit failed for ${shard}: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  private async publishToQueue(
    shard: string,
    cut: OtelGroupCut,
  ): Promise<void> {
    const queue = OtelIngestionQueue.getInstance({ shardName: shard });
    if (!queue) {
      throw new Error(`otel queue instance unavailable for shard ${shard}`);
    }
    await queue.add(
      QueueJobs.OtelIngestionJob,
      {
        id: randomUUID(),
        timestamp: new Date(),
        name: QueueJobs.OtelIngestionJob as const,
        payload: {
          shape: "group-v1" as const,
          groupId: cut.groupId,
          entries: cut.entries,
        },
      },
      // jobId = groupId: BullMQ dedups replays of the same decided group; the
      // events_full load label derives from the same identity.
      { jobId: cut.groupId },
    );
  }

  /** Returns true when the policy is safe (or the check is exempted). */
  private async assertNoEviction(redis: RedisHandle): Promise<boolean> {
    if (sharedEnv.REDIS_CLUSTER_ENABLED === "true") {
      // CONFIG GET on a cluster handle reaches one node only; checking every
      // node needs topology iteration — explicitly exempted, contract moves
      // to the deployment checklist (design §5.2).
      logger.warn(
        "[OtelGrouper] noeviction self-check skipped in cluster mode — ensure maxmemory-policy=noeviction on every node (deployment contract)",
      );
      return true;
    }
    try {
      const res = (await (redis as Redis).config(
        "GET",
        "maxmemory-policy",
      )) as unknown as string[];
      const policy = Array.isArray(res) ? res[1] : undefined;
      if (policy !== "noeviction") {
        logger.error(
          `[OtelGrouper] REFUSING to start: maxmemory-policy=${policy ?? "unknown"} (must be noeviction — any eviction can silently drop pending entries/staging manifests/registered keys and break exactly-once)`,
        );
        return false;
      }
      return true;
    } catch (e) {
      // CONFIG may be disabled on managed Redis — log and proceed (contract
      // moves to the deployment checklist, same as cluster mode).
      logger.warn(
        `[OtelGrouper] noeviction self-check unavailable (CONFIG GET failed: ${e instanceof Error ? e.message : String(e)}) — verify maxmemory-policy=noeviction manually`,
      );
      return true;
    }
  }
}
