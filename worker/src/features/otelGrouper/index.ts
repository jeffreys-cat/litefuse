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
  setOtelGrouperLease,
  cutOtelGroup,
  scanStagedOtelGroups,
  readOtelStagingManifest,
  clearOtelStagingManifest,
  reconcileOtelPending,
  otelPendingDepth,
  otelQuarantineDepth,
  otelPendingOldestAgeMs,
  getLaneIndex,
  getSplitProjectIds,
  isSplitProject,
  getShardIndex,
  type OtelGroupCut,
} from "@langfuse/shared/src/server";
import { env } from "../../env";
import { env as sharedEnv } from "@langfuse/shared/src/env";

/**
 * The single Redis lease key that elects ONE domain leader for ALL project
 * lanes (design §4.2 / F1/F7). Its value is a groupingKey passed to the shared
 * lease helpers — it is NOT a shard and NOT a lane, just the domain election key.
 */
const LANE_DOMAIN_LEASE = "otel-lane-domain";

/** `lane-<pid>` → `<pid>`. */
const laneToProjectId = (lane: string): string =>
  lane.startsWith("lane-") ? lane.slice("lane-".length) : lane;
import { groupJobLoadLimiter } from "../../queues/otelGroupJobProcessor";

type RedisHandle = Redis | Cluster;

export type OtelGrouperConfig = {
  targetBytes: number;
  targetRows: number;
  maxFiles: number;
  flushMs: number;
  lockTtlMs: number;
  laneDomainLeaseTtlMs: number;
  tickMs: number;
};

type Deps = {
  redis?: RedisHandle;
  /** Test seam — replaces the BullMQ publish. The first arg is the groupingKey
   * (shard or lane) for logging only; the queue is chosen from the groupId. */
  addGroupJob?: (groupingKey: string, cut: OtelGroupCut) => Promise<void>;
  /** Test seam — lane readiness (MV FINISHED). Default lazily calls
   * getSplitTablesReadiness (kept out of the static import graph to avoid the
   * repositories/doris require cycle under dd-trace). */
  isLaneReady?: (projectId: string) => Promise<boolean>;
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
    groupingKey: string,
    cut: OtelGroupCut,
  ) => Promise<void>;
  private redis: RedisHandle | null = null;
  private ownsRedis = false;
  private timer: NodeJS.Timeout | null = null;
  private stopped = true;
  private tickInFlight: Promise<void> | null = null;
  private tickCount = 0;
  private healthLineCounter = 0;
  /** shard/domain → we believe we currently hold its lease (renew before re-acquire). */
  private held = new Set<string>();
  /** Lanes whose MV build has FINISHED (monotonic — checked once, then cached).
   * The grouper must not cut a lane until its rollup is live (Stage 1.2c gate). */
  private readyLanes = new Set<string>();

  constructor(private readonly deps: Deps = {}) {
    this.cfg = {
      targetBytes: env.LITEFUSE_OTEL_GROUP_TARGET_BYTES,
      targetRows: env.LITEFUSE_OTEL_GROUP_TARGET_ROWS,
      maxFiles: env.LITEFUSE_OTEL_GROUP_MAX_FILES,
      flushMs: env.LITEFUSE_OTEL_GROUP_FLUSH_MS,
      lockTtlMs: env.LITEFUSE_OTEL_GROUPER_LOCK_TTL_MS,
      laneDomainLeaseTtlMs: env.LITEFUSE_OTEL_LANE_DOMAIN_LEASE_TTL_MS,
      tickMs: env.LITEFUSE_OTEL_GROUPER_TICK_MS,
      ...deps.config,
    };
    this.shardNames = deps.shardNames ?? OtelIngestionQueue.getShardNames();
    this.addGroupJob = deps.addGroupJob ?? this.publishToQueue.bind(this);
    this.isLaneReady = deps.isLaneReady ?? this.defaultIsLaneReady.bind(this);
  }

  private readonly isLaneReady: (projectId: string) => Promise<boolean>;

  /** Default readiness: lazy-import getSplitTablesReadiness (its doris-query
   * chain must stay out of the grouper's static graph — dd-trace require-cycle
   * TDZ); the barrel is already cached at runtime so this is a no-cost lookup. */
  private async defaultIsLaneReady(projectId: string): Promise<boolean> {
    const { getSplitTablesReadiness } = await import(
      "@langfuse/shared/src/server"
    );
    const r = await getSplitTablesReadiness(projectId).catch(() => null);
    return r?.ready ?? false;
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
    // Local handle: stop() may run during the awaits below and null out
    // this.redis — dereferencing the field after an await would throw.
    // disconnect() on an already-disconnected connection is a no-op.
    const conn = this.redis;

    // ioredis connects ASYNCHRONOUSLY and our connection has
    // enableOfflineQueue=false — any command issued before the socket is
    // ready throws "Stream isn't writeable". Wait for readiness first, or
    // the noeviction self-check below silently degrades to "unavailable"
    // (deployment contract unchecked!) and the first ticks spray errors.
    // start() is fire-and-forget from app.ts, so waiting here blocks nobody.
    if (!(await this.waitForRedisReady(conn))) {
      // stopped during the wait
      if (this.ownsRedis) conn.disconnect();
      this.redis = null;
      return;
    }

    // Deployment-contract self-check (design §5.2/L4): with an eviction
    // policy other than noeviction, Redis may silently evict pending entries,
    // staging manifests and registered keys — losing registered data and
    // breaking idempotency with no error signal anywhere. Refuse to run.
    if (!(await this.assertNoEviction(conn)) || this.stopped) {
      this.stopped = true;
      if (this.ownsRedis) conn.disconnect();
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

  /**
   * Poll until the connection reports "ready" (or the grouper is stopped —
   * returns false). No hard timeout: if Redis is down at boot the grouper
   * simply keeps waiting instead of dying permanently; a warn every 10s
   * keeps the wait visible.
   */
  private async waitForRedisReady(redis: RedisHandle): Promise<boolean> {
    let lastWarn = Date.now();
    while (!this.stopped) {
      if ((redis as Redis).status === "ready") return true;
      if (Date.now() - lastWarn >= 10_000) {
        lastWarn = Date.now();
        logger.warn(
          `[OtelGrouper] waiting for Redis connection (status=${(redis as Redis).status})…`,
        );
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    return false;
  }

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
    const emitGauges =
      this.tickCount % Math.max(1, Math.round(5_000 / this.cfg.tickMs)) === 0;
    if (emitGauges) {
      // In-flight stream loads of THIS worker's group path (process-wide,
      // not per shard): active = loads on the wire, pending = queued on the
      // semaphore. The group-path counterpart of DorisWriter's
      // "pool loads=x/y waiters=n" gauge line.
      recordGauge(
        "langfuse.otel_group.loads_active",
        groupJobLoadLimiter.activeCount,
      );
      recordGauge(
        "langfuse.otel_group.loads_pending",
        groupJobLoadLimiter.pendingCount,
      );
    }
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

    // ── B. project-lane domain (Stage 1.4) ──
    if (this.stopped) return;
    // Lane backlog gauges are leadership-INDEPENDENT (like shards — #10): EVERY
    // worker emits them, so a dead domain leader surfaces as a RISING gauge, not
    // as gauge absence. A lane has no backpressure — pending_depth/oldest_age is
    // the only pre-OOM early warning and must not go dark exactly when the leader
    // is down. Only the cut/recover work in tickLanes() needs the domain lease.
    const laneGaugeRedis = this.redis;
    if (
      emitGauges &&
      sharedEnv.LITEFUSE_DORIS_TABLE_SPLIT_MODE !== "none" &&
      laneGaugeRedis
    ) {
      try {
        for (const lane of await this.candidateLanes(laneGaugeRedis)) {
          if (this.stopped) return;
          // #9: one lane's gauge failure must not starve the rest.
          try {
            await this.emitLaneGauges(lane);
          } catch (e) {
            logger.error(
              `[OtelGrouper] lane ${lane} gauge failed: ${e instanceof Error ? e.message : String(e)}`,
            );
          }
        }
      } catch (e) {
        logger.error(
          `[OtelGrouper] lane gauge scan failed: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
    try {
      await this.tickLanes();
    } catch (e) {
      logger.error(
        `[OtelGrouper] lane domain tick failed: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  /**
   * Project-lane domain tick (design §4.2). A SINGLE domain leader (elected via
   * LANE_DOMAIN_LEASE) full-scans every project lane (≤100 short-term). Two
   * phases (F2): probe each lane's staging residue in parallel, then cut the
   * residue-free majority and serially recover the few with residue (cut must
   * follow recover — recovering is multi-RTT + can't pipeline with cut).
   */
  private async tickLanes(): Promise<void> {
    if (sharedEnv.LITEFUSE_DORIS_TABLE_SPLIT_MODE === "none") return;
    const redis = this.redis!;
    // One leader for ALL lanes (shorter TTL → fast takeover of a stalled leader).
    // NOTE: lane backlog gauges are emitted BEFORE this gate, in tickAll(), by
    // EVERY worker (#10) — only the cut/recover work below needs the lease.
    if (
      !(await this.acquireOrRenew(
        LANE_DOMAIN_LEASE,
        this.cfg.laneDomainLeaseTtlMs,
      ))
    )
      return;

    const lanes = await this.candidateLanes(redis);
    if (lanes.length === 0) return;

    // Readiness gate: only cut a lane whose MV build has FINISHED. Checked once
    // per lane (Doris query) then cached — ready is monotonic, so a warm lane
    // costs nothing on later ticks.
    const ready: string[] = [];
    for (const lane of lanes) {
      if (this.readyLanes.has(lane)) {
        ready.push(lane);
        continue;
      }
      try {
        if (await this.isLaneReady(laneToProjectId(lane))) {
          this.readyLanes.add(lane);
          ready.push(lane);
        }
        // not ready (MV still building / tables absent) → skip this tick
      } catch (e) {
        // #9: a readiness-probe error (Doris blip) skips only THIS lane, not
        // the whole domain tick.
        logger.error(
          `[OtelGrouper] lane ${lane} readiness check failed: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
    if (ready.length === 0) return;

    // Phase 1 (F2): batch-probe staging residue.
    const probes = await Promise.all(
      ready.map((lane) =>
        scanStagedOtelGroups({ redis, groupingKey: lane })
          .then((ids) => ({ lane, residue: ids.length > 0 }))
          // A probe failure is treated as residue → serial recover (safe): we
          // never cut a lane we couldn't confirm is clean.
          .catch(() => ({ lane, residue: true })),
      ),
    );

    // Phase 2b: lanes WITH residue → serial recover, no cut this tick.
    for (const { lane, residue } of probes) {
      if (!residue || this.stopped) continue;
      try {
        await this.recoverStaged(lane);
      } catch (e) {
        // #9: recover failure is per-lane — the lane keeps its residue and is
        // retried next tick (it is never cut while dirty).
        logger.error(
          `[OtelGrouper] lane ${lane} recover failed: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
    // Phase 2a: residue-free lanes → one cut each (fair: one group per lane/tick).
    for (const { lane, residue } of probes) {
      if (residue || this.stopped) continue;
      // #6: re-confirm domain ownership before EACH cut. A long tick (per-lane
      // readiness Doris queries + serial recovers) can outlast the short
      // domain-lease TTL; without this, an expired leader keeps cutting
      // alongside the freshly elected one (dual-leader), defeating F1's fence —
      // the per-lane token is a plain SET that the stale leader re-stamps. The
      // renew also keeps a genuinely-live leader's lease fresh so a slow-but-
      // alive tick is not spuriously taken over.
      if (
        !(await this.acquireOrRenew(
          LANE_DOMAIN_LEASE,
          this.cfg.laneDomainLeaseTtlMs,
        ))
      ) {
        logger.warn(
          "[OtelGrouper] lost lane-domain lease mid-tick — stopping cuts (a new leader now owns the domain)",
        );
        return;
      }
      try {
        await this.tickLaneCut(lane);
      } catch (e) {
        // #9: cut failure is per-lane (tickLaneCut reconciles its own cut-Lua
        // errors; this guards a publish/stamp failure from starving the rest).
        logger.error(
          `[OtelGrouper] lane ${lane} cut failed: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
  }

  /**
   * Candidate lanes = (PG split list ∪ lane-index) still split. lane-index is
   * the PG-outage fallback; filtering by isSplitProject drops un-split /
   * tombstoned projects (design F6 — never cut into a table about to be
   * dropped; their residual keys are cleaned by the deletion flow §4.4).
   */
  /** Per-lane backlog gauges (the lane counterpart of emitShardGauges). Tagged
   * with the lane as `shard` so dashboards read both pools on one metric. */
  private async emitLaneGauges(lane: string): Promise<void> {
    const redis = this.redis!;
    const [depth, quarantine, oldestAge, staged] = await Promise.all([
      otelPendingDepth({ redis, groupingKey: lane }),
      otelQuarantineDepth({ redis, groupingKey: lane }),
      otelPendingOldestAgeMs({ redis, groupingKey: lane }),
      scanStagedOtelGroups({ redis, groupingKey: lane }),
    ]);
    recordGauge("langfuse.otel_grouper.pending_depth", depth, { shard: lane });
    recordGauge("langfuse.otel_grouper.quarantine_depth", quarantine, {
      shard: lane,
    });
    recordGauge("langfuse.otel_grouper.staging_count", staged.length, {
      shard: lane,
    });
    recordGauge("langfuse.otel_grouper.pending_oldest_age_ms", oldestAge ?? 0, {
      shard: lane,
    });
  }

  private async candidateLanes(redis: RedisHandle): Promise<string[]> {
    const fromPg = getSplitProjectIds().map((pid) => `lane-${pid}`);
    const fromIndex = await getLaneIndex(redis).catch(() => [] as string[]);
    const seen = new Set<string>();
    const lanes: string[] = [];
    for (const lane of [...fromPg, ...fromIndex]) {
      if (seen.has(lane)) continue;
      seen.add(lane);
      if (isSplitProject(laneToProjectId(lane))) lanes.push(lane);
    }
    return lanes;
  }

  /**
   * Cut one group from a lane. F1: stamp this domain leader's token into the
   * lane's lock (plain SET — the domain lease already guarantees a single
   * writer) so the unchanged cut Lua fence `GET lock==token` passes; a handover
   * overwrites it, fencing the old leader's late cut.
   */
  private async tickLaneCut(lane: string): Promise<void> {
    const redis = this.redis!;
    await setOtelGrouperLease({
      redis,
      groupingKey: lane,
      token: this.token,
      ttlMs: this.cfg.laneDomainLeaseTtlMs,
    });
    const cut = await cutOtelGroup({
      redis,
      groupingKey: lane,
      token: this.token,
      targetBytes: this.cfg.targetBytes,
      targetRows: this.cfg.targetRows,
      maxFiles: this.cfg.maxFiles,
      flushMs: this.cfg.flushMs,
    }).catch(async (e) => {
      logger.error(
        `[OtelGrouper] cut failed on lane ${lane} (reconciling): ${e instanceof Error ? e.message : String(e)}`,
      );
      await this.recoverStaged(lane);
      return null;
    });
    if (!cut) return;

    recordIncrement("langfuse.otel_grouper.groups_cut", 1, { shard: lane });
    recordIncrement("langfuse.otel_grouper.files_grouped", cut.entries.length, {
      shard: lane,
    });
    const bytes = cut.entries.reduce((a, e) => a + e.size, 0);
    const spans = cut.entries.reduce((a, e) => a + e.spanCount, 0);
    const waitedMs = Date.now() - Math.min(...cut.entries.map((e) => e.ts));
    logger.info(
      `[OtelGrouper] cut lane=${lane} group=${cut.groupId.slice(0, 12)} files=${cut.entries.length} bytes=${(bytes / (1024 * 1024)).toFixed(1)}MB spans=${spans} waited=${waitedMs}ms`,
    );
    await this.publish(lane, cut);
  }

  private async tickShard(shard: string): Promise<void> {
    const redis = this.redis!;
    if (!(await this.acquireOrRenew(shard))) return; // not the leader

    // Recovery must COMPLETE before the next cut: a leftover manifest whose
    // members still sit in pending (mid-script residue) would otherwise be
    // re-cut into a second group under a second label. A transient failure
    // on any single group's recovery therefore skips cutting for this tick —
    // better one 200ms-late cut than a double-grouped file.
    if (!(await this.recoverStaged(shard))) return;

    const cut = await cutOtelGroup({
      redis,
      groupingKey: shard,
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

    // Cut decision line — reads together with the [OtelGroupJob] line:
    // waited = oldest member's registration → cut (the grouping latency
    // component of e2e_lag); reason = which dispatch-gate condition fired
    // (undersized "timeout" groups at high traffic ⇒ raise flush/tick;
    // constant "bytes" at low traffic ⇒ target too small).
    const bytes = cut.entries.reduce((a, e) => a + e.size, 0);
    const spans = cut.entries.reduce((a, e) => a + e.spanCount, 0);
    const waitedMs = Date.now() - Math.min(...cut.entries.map((e) => e.ts));
    const reason =
      bytes >= this.cfg.targetBytes
        ? "bytes"
        : spans >= this.cfg.targetRows
          ? "rows"
          : cut.entries.length >= this.cfg.maxFiles
            ? "files"
            : "timeout";
    logger.info(
      `[OtelGrouper] cut shard=${shard} group=${cut.groupId.slice(0, 12)} files=${cut.entries.length} bytes=${(bytes / (1024 * 1024)).toFixed(1)}MB spans=${spans} waited=${waitedMs}ms reason=${reason}`,
    );

    await this.publish(shard, cut);
  }

  /**
   * Publish a decided group. Failure is NOT an error state: the manifest
   * stays in staging and the next tick's recovery republishes it (jobId
   * dedup + load label make every retry idempotent).
   */
  private async publish(groupingKey: string, cut: OtelGroupCut): Promise<void> {
    try {
      await this.addGroupJob(groupingKey, cut);
    } catch (e) {
      recordIncrement("langfuse.otel_grouper.publish_errors", 1, {
        shard: groupingKey,
      });
      logger.error(
        `[OtelGrouper] publish failed for group ${cut.groupId} on ${groupingKey} (manifest retained for republish): ${e instanceof Error ? e.message : String(e)}`,
      );
      return;
    }
    await clearOtelStagingManifest({
      redis: this.redis!,
      groupingKey,
      groupId: cut.groupId,
    });
  }

  /**
   * Republish leftover staging manifests. Runs EVERY leader tick: covers the
   * grouper crash between cut and publish, a publish failure on a healthy
   * leader, and (via reconcile) the partial-cut residue of a mid-script
   * error. A nil manifest for a scanned groupId only means a concurrent
   * clear between HKEYS and HGET (already published) — skip.
   *
   * Returns false when any group's recovery hit a TRANSIENT error (redis
   * read failure etc.) — the caller must then skip cutting this tick, or
   * unreconciled mid-script residue could be re-cut into a second group
   * under a second label. (An unparsable manifest reads as null and does NOT
   * dirty the tick: Redis doesn't bit-rot values, so it can only be a
   * foreign/buggy write — alarmed in the read helper, never fixable by
   * waiting, must not stall the shard.)
   */
  private async recoverStaged(shard: string): Promise<boolean> {
    const redis = this.redis!;
    let clean = true;
    const groupIds = await scanStagedOtelGroups({ redis, groupingKey: shard });
    for (const groupId of groupIds) {
      try {
        const manifest = await readOtelStagingManifest({
          redis,
          groupingKey: shard,
          groupId,
        });
        if (!manifest) continue;
        // Self-heal the isolation-without-rollback case (manifest written,
        // LTRIM never ran): idempotent LREM of every manifest member.
        await reconcileOtelPending({
          redis,
          groupingKey: shard,
          rawEntries: manifest.rawEntries,
          // Head-window bound for the residue scan (see RECONCILE_LUA):
          // members can only live within the cut window, so recovery cost is
          // independent of the pending backlog depth.
          windowSize: this.cfg.maxFiles,
        });
        recordIncrement("langfuse.otel_grouper.republished", 1, { shard });
        // Crash-recovery in action — expect a matching [OtelGroupJob] line
        // (or a LABEL_DEDUP no-op) shortly after; repeated lines for the
        // SAME group mean the publish keeps failing.
        logger.warn(
          `[OtelGrouper] republishing leftover group=${groupId.slice(0, 12)} shard=${shard} files=${manifest.entries.length}`,
        );
        await this.publish(shard, manifest);
      } catch (e) {
        logger.error(
          `[OtelGrouper] recovery failed for group ${groupId} on ${shard}: ${e instanceof Error ? e.message : String(e)}`,
        );
        clean = false; // dirty tick — caller must not cut until recovery completes
      }
    }
    return clean;
  }

  private async acquireOrRenew(
    groupingKey: string,
    ttlMs: number = this.cfg.lockTtlMs,
  ): Promise<boolean> {
    const redis = this.redis!;
    if (this.held.has(groupingKey)) {
      if (
        await renewOtelGrouperLease({
          redis,
          groupingKey,
          token: this.token,
          ttlMs,
        })
      ) {
        return true;
      }
      this.held.delete(groupingKey); // lost it (expiry/takeover) — fall through
    }
    const acquired = await acquireOtelGrouperLease({
      redis,
      groupingKey,
      token: this.token,
      ttlMs,
    });
    if (acquired) this.held.add(groupingKey);
    return acquired;
  }

  private async emitShardGauges(shard: string): Promise<void> {
    const redis = this.redis!;
    const [depth, quarantine, oldestAge, staged] = await Promise.all([
      otelPendingDepth({ redis, groupingKey: shard }),
      otelQuarantineDepth({ redis, groupingKey: shard }),
      otelPendingOldestAgeMs({ redis, groupingKey: shard }),
      scanStagedOtelGroups({ redis, groupingKey: shard }),
    ]);
    recordGauge("langfuse.otel_grouper.pending_depth", depth, { shard });
    recordGauge("langfuse.otel_grouper.quarantine_depth", quarantine, {
      shard,
    });
    recordGauge("langfuse.otel_grouper.staging_count", staged.length, {
      shard,
    });
    recordGauge("langfuse.otel_grouper.pending_oldest_age_ms", oldestAge ?? 0, {
      shard,
    });

    // Queue-side health (design §6.3): wait depth = consumption keeping up;
    // failed depth + oldest failed age = the DLQ/age-guard chain's SLA input
    // (failed_oldest_age alert must fire well before the label retention
    // window closes the redrive path). Only for REAL configured shards — the
    // tests' throwaway shard names must not summon queue connections.
    let wait: number | null = null;
    let failed: number | null = null;
    let failedOldestMs = 0;
    if (OtelIngestionQueue.getShardNames().includes(shard)) {
      try {
        const queue = OtelIngestionQueue.getInstance({ shardName: shard });
        if (queue) {
          const counts = await queue.getJobCounts("wait", "failed");
          wait = counts.wait ?? 0;
          failed = counts.failed ?? 0;
          recordGauge("langfuse.otel_queue.wait_depth", wait, { shard });
          recordGauge("langfuse.otel_queue.failed_depth", failed, { shard });
          const oldestFailed = await queue.getJobs(["failed"], 0, 0, true);
          failedOldestMs = oldestFailed[0]?.timestamp
            ? Date.now() - oldestFailed[0].timestamp
            : 0;
          recordGauge(
            "langfuse.otel_queue.failed_oldest_age_ms",
            failedOldestMs,
            { shard },
          );
        }
      } catch (e) {
        logger.debug(
          `[OtelGrouper] queue gauge emit failed for ${shard}: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }

    // Human-readable health line every ~60s (gauges fire every ~5s → every
    // 12th emission). All-zero = healthy steady state; the interesting
    // fields for troubleshooting: pending(oldest) rising = grouper stalled /
    // undersized; wait rising = consumption behind; failed>0 = jobs riding
    // the DLQ ladder (check failed_oldest vs the 3d label window); anything
    // in staging/quarantine persisting = recovery/poison-entry problems.
    this.healthLineCounter = (this.healthLineCounter + 1) % 12;
    if (this.healthLineCounter === 0) {
      const oldestSec = oldestAge != null ? (oldestAge / 1000).toFixed(1) : "-";
      const queuePart =
        wait != null
          ? ` wait=${wait} failed=${failed}${(failed ?? 0) > 0 ? `(oldest ${(failedOldestMs / 3600_000).toFixed(1)}h)` : ""}`
          : "";
      logger.info(
        `[OtelGrouper.health] shard=${shard} leader=${this.held.has(shard)} pending=${depth}(oldest ${oldestSec}s) staging=${staged.length} quarantine=${quarantine}${queuePart} loads=${groupJobLoadLimiter.activeCount}/${env.LITEFUSE_OTEL_LOAD_CONCURRENCY}${groupJobLoadLimiter.pendingCount > 0 ? `(+${groupJobLoadLimiter.pendingCount} queued)` : ""}`,
      );
    }
  }

  private async publishToQueue(
    groupingKey: string,
    cut: OtelGroupCut,
  ): Promise<void> {
    // F3: the consumer queue is DECOUPLED from the groupingKey (shard or lane).
    // A lane name would NaN → shard 0 (consumption skew); instead pick the shard
    // deterministically from the groupId, so a lane's groups spread evenly AND
    // every replay / DLQ redrive of the same group hits the same queue.
    const shardNames = OtelIngestionQueue.getShardNames();
    const shardName =
      shardNames[getShardIndex(cut.groupId, shardNames.length)] ??
      shardNames[0];
    const queue = OtelIngestionQueue.getInstance({ shardName });
    if (!queue) {
      throw new Error(
        `otel queue instance unavailable (group ${cut.groupId}, groupingKey ${groupingKey})`,
      );
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
