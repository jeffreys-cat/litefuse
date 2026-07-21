#!/usr/bin/env tsx

/**
 * Otel S3 ↔ ledger reconciliation (exactly-once design §3.5; the final
 * backstop for every loss window: Redis tail-loss on failover, poison
 * eviction, A2 orphans).
 *
 * Usage (from worker/):
 *   pnpm run reconcile-otel-files                    # dry-run, last 8 days
 *   pnpm run reconcile-otel-files -- --execute       # re-inject "reinject"s
 *   pnpm run reconcile-otel-files -- --window-days 3 --older-than-hours 80
 *   pnpm run reconcile-otel-files -- --dup-check     # duplicate-rate SQL only
 *
 * Classification per S3 file (see classify.ts):
 *   ok        — ledger row exists (completed end-to-end)          → skip
 *   in-flight — no ledger, younger than --older-than-hours        → skip
 *   poisoned  — tracked in otel_poison_jobs                       → manual
 *   reinject  — over-age + registered key still present ⇒ pointer lost
 *               inside the pipeline → DEL registered key, re-register
 *               (--execute only; dry-run just reports)
 *   audit     — over-age, no evidence ⇒ A2 orphan vs expired loss → manual
 *               (auto-reinjection could DUPLICATE an SDK re-sent copy)
 *
 * The re-registered entry carries size/spanCount = 0 (unknown without a
 * download; the grouper's flush timeout ships such entries regardless —
 * sizing precision is irrelevant at reconciliation volumes).
 */

import Redis from "ioredis";

import {
  OtelIngestionQueue,
  dorisClient,
  getS3EventStorageClient,
  logger,
  registerOtelFile,
  otelRegisteredKey,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";
import { env as sharedEnv } from "@langfuse/shared/src/env";

import { classifyOtelFile, type OtelFileVerdict } from "./classify";

const arg = (name: string): string | undefined => {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : undefined;
};
const flag = (name: string): boolean => process.argv.includes(`--${name}`);

const EXECUTE = flag("execute");
const DUP_CHECK_ONLY = flag("dup-check");
const WINDOW_DAYS = Number(arg("window-days") ?? 8);
const OLDER_THAN_MS = Number(arg("older-than-hours") ?? 80) * 3600_000;
const BATCH = 500;

const main = async () => {
  const client = dorisClient();

  if (DUP_CHECK_ONLY) {
    // Duplicate-rate accounting SQL (design §3.6) per recent day.
    for (let d = 0; d < WINDOW_DAYS; d++) {
      const day = new Date(Date.now() - d * 86_400_000)
        .toISOString()
        .slice(0, 10);
      const rows = await client.query(
        `SELECT
           COUNT(*) AS total_rows,
           COUNT(DISTINCT project_id, trace_id, start_time, span_id) AS distinct_spans,
           COUNT(*) - COUNT(DISTINCT project_id, trace_id, start_time, span_id) AS duplicate_rows
         FROM events_full
         WHERE date_trunc(start_time, 'day') = date_trunc('${day} 00:00:00', 'day')`,
      );
      console.log(JSON.stringify({ day, ...(rows?.[0] ?? {}) }));
    }
    return;
  }

  // Age-guard sanity (design §6.4 threshold contract).
  const labelKeepMs = 3 * 24 * 3600_000;
  if (OLDER_THAN_MS <= labelKeepMs) {
    throw new Error(
      `--older-than-hours must exceed the DLQ age guard (${labelKeepMs / 3600_000}h): still-redrivable jobs must never be re-injected`,
    );
  }
  if (OLDER_THAN_MS >= sharedEnv.LITEFUSE_OTEL_REGISTERED_TTL_MS) {
    logger.warn(
      "--older-than-hours >= registered-key TTL: the auto-reinject category degenerates (registration evidence expires first); everything unexplained will fall to manual audit",
    );
  }

  const redis = new Redis({
    host: process.env.REDIS_HOST ?? "127.0.0.1",
    port: Number(process.env.REDIS_PORT ?? 6379),
    password: process.env.REDIS_AUTH || undefined,
  });
  const shards = OtelIngestionQueue.getShardNames();
  const storage = getS3EventStorageClient(
    sharedEnv.LITEFUSE_S3_EVENT_UPLOAD_BUCKET!,
  );

  // ① S3 inventory inside the window.
  const prefix = `${sharedEnv.LITEFUSE_S3_EVENT_UPLOAD_PREFIX}otel/`;
  const cutoff = Date.now() - WINDOW_DAYS * 86_400_000;
  const files = (await storage.listFiles(prefix)).filter(
    (f) => f.createdAt.getTime() >= cutoff,
  );
  logger.info(
    `[reconcile] ${files.length} otel file(s) in the last ${WINDOW_DAYS}d window`,
  );

  // ② Poison ledger (rare rows — load once, index by fileKey).
  const poisonRows = await prisma.otelPoisonJob.findMany({
    where: { createdAt: { gte: new Date(cutoff) } },
    select: { fileKeys: true },
  });
  const poisoned = new Set<string>(
    poisonRows.flatMap((r) => (Array.isArray(r.fileKeys) ? (r.fileKeys as string[]) : [])),
  );

  // ③ Ledger membership, batched IN queries (multiple rows per fileKey are
  // possible — event_id is part of the UNIQUE KEY — membership only).
  const withLedger = new Set<string>();
  for (let i = 0; i < files.length; i += BATCH) {
    const batch = files.slice(i, i + BATCH);
    const inList = batch
      .map((f) => `'${f.file.replace(/'/g, "''")}'`)
      .join(",");
    const rows = await client.query(
      `SELECT DISTINCT entity_id FROM blob_storage_file_log
       WHERE entity_type = 'otel-file' AND entity_id IN (${inList})`,
    );
    for (const r of rows ?? []) withLedger.add(String(r.entity_id));
  }

  // ④ Classify + act.
  const now = Date.now();
  const counts: Record<OtelFileVerdict["verdict"], number> = {
    ok: 0,
    "in-flight": 0,
    poisoned: 0,
    reinject: 0,
    audit: 0,
  };
  for (const f of files) {
    let hasRegisteredKey = false;
    if (!withLedger.has(f.file)) {
      for (const shard of shards) {
        if ((await redis.exists(otelRegisteredKey(shard, f.file))) === 1) {
          hasRegisteredKey = true;
          break;
        }
      }
    }
    const verdict = classifyOtelFile(
      {
        fileKey: f.file,
        createdAtMs: f.createdAt.getTime(),
        hasLedger: withLedger.has(f.file),
        isPoisoned: poisoned.has(f.file),
        hasRegisteredKey,
      },
      { now, olderThanMs: OLDER_THAN_MS },
    );
    counts[verdict.verdict]++;

    if (verdict.verdict === "ok" || verdict.verdict === "in-flight") continue;
    console.log(
      JSON.stringify({ fileKey: f.file, createdAt: f.createdAt, ...verdict }),
    );

    if (verdict.verdict === "reinject" && EXECUTE) {
      // projectId is encoded in the path: <prefix>otel/<projectId>/...
      const projectId = f.file.slice(prefix.length).split("/")[0];
      const shard = shards[Math.floor(Math.random() * shards.length)] as string;
      // Force re-admission: the registered key would absorb the register as
      // an idempotent no-op (review: reconciliation × SETNX conflict).
      for (const s of shards) await redis.del(otelRegisteredKey(s, f.file));
      const admitted = await registerOtelFile({
        redis,
        shard,
        ttlMs: sharedEnv.LITEFUSE_OTEL_REGISTERED_TTL_MS,
        entry: {
          v: 1,
          fileKey: f.file,
          size: 0,
          spanCount: 0,
          ts: Date.now(),
          projectId,
        },
      });
      console.log(
        JSON.stringify({ fileKey: f.file, reinjected: admitted, shard }),
      );
    }
  }

  console.log(
    JSON.stringify({
      summary: counts,
      mode: EXECUTE ? "execute" : "dry-run",
      windowDays: WINDOW_DAYS,
      olderThanHours: OLDER_THAN_MS / 3600_000,
    }),
  );
  redis.disconnect();
  await prisma.$disconnect();
};

main().catch((e) => {
  logger.error("[reconcile] failed", e);
  process.exit(1);
});
