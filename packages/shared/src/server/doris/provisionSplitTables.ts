import { commandDoris, queryDoris } from "../repositories/doris";
import { env } from "../../env";
import { logger } from "../logger";
import {
  buildSplitTableStatements,
  buildTraceMetricsAggMV,
  buildAlterTtlStatement,
} from "./splitTableTemplates";
import {
  assertValidDorisProjectId,
  splitTableNameForProject,
} from "./tableRouting";

/**
 * Provisioning + readiness for a split project's Doris objects (Stage 1.2b/1.2c).
 *
 * events_full_<pid> / traces_scalar_<pid> are `CREATE TABLE IF NOT EXISTS`
 * (idempotent). The trace_metrics_agg_<pid> sync MV is NOT `IF NOT EXISTS` and
 * its build is ASYNC — re-issuing CREATE while it builds errors with
 * "state(ROLLUP) is not NORMAL", so we check MV status first and only CREATE
 * when it is genuinely absent. The same status feeds the readiness gate: a
 * project is ready = both base tables exist AND the MV build FINISHED.
 */

const physical = (projectId: string) => {
  assertValidDorisProjectId(projectId);
  return {
    eventsFull: splitTableNameForProject(projectId, "events_full"),
    tracesScalar: splitTableNameForProject(projectId, "traces_scalar"),
    mv: `trace_metrics_agg_${projectId}`,
  };
};

/**
 * Teardown counterpart of provisionSplitTablesForProject — DROPs a project's
 * split base tables at project-deletion time. The trace_metrics_agg_<pid> sync
 * MV is attached to events_full_<pid> and is removed with it, so dropping the
 * two base tables is sufficient. IF EXISTS makes this idempotent: a no-op for a
 * project that was never provisioned, and safe for a delete job that retries
 * after a partial run.
 */
export const dropSplitTablesForProject = async (
  projectId: string,
): Promise<void> => {
  const names = physical(projectId);
  for (const table of [names.eventsFull, names.tracesScalar]) {
    await commandDoris({
      query: `DROP TABLE IF EXISTS \`${table}\``,
      tags: { feature: "table-split", kind: "drop", projectId },
    });
    logger.info("Dropped Doris split table if it existed", {
      projectId,
      table,
    });
  }
};

/** Whether a physical table exists (SHOW TABLES LIKE is exact-match here). */
export const dorisTableExists = async (table: string): Promise<boolean> => {
  const rows = await queryDoris<Record<string, unknown>>({
    query: `SHOW TABLES LIKE '${table}'`,
    tags: { feature: "table-split", kind: "exists" },
  });
  return rows.length > 0;
};

export type SplitMvStatus = "absent" | "building" | "finished" | "cancelled";

/**
 * Status of a base table's sync MV build. Uses SHOW ALTER TABLE MATERIALIZED
 * VIEW (the build-job state) as the source of truth, falling back to DESC …
 * ALL (the MV registers as an index once built) when no alter job is retained.
 */
export const getSplitMvStatus = async (
  baseTable: string,
  mvName: string,
): Promise<SplitMvStatus> => {
  const alterRows = await queryDoris<{
    RollupIndexName?: string;
    IndexName?: string;
    State?: string;
  }>({
    query: `SHOW ALTER TABLE MATERIALIZED VIEW FROM \`${env.DORIS_DB}\` WHERE TableName = '${baseTable}'`,
    tags: { feature: "table-split", kind: "mv-status" },
  });
  const job = alterRows
    .filter((r) => (r.RollupIndexName ?? r.IndexName) === mvName)
    .pop();
  if (job) {
    const state = (job.State ?? "").toUpperCase();
    if (state === "FINISHED") return "finished";
    if (state === "CANCELLED") return "cancelled";
    return "building"; // PENDING / WAITING_TXN / RUNNING
  }
  // No alter job retained — fall back to the index list.
  const descRows = await queryDoris<{ IndexName?: string }>({
    query: `DESC \`${baseTable}\` ALL`,
    tags: { feature: "table-split", kind: "mv-desc" },
  });
  const hasIndex = descRows.some((r) => r.IndexName === mvName);
  return hasIndex ? "finished" : "absent";
};

/**
 * Idempotently create a split project's tables + MV. Safe to re-run (retry,
 * concurrent workers): base tables are IF NOT EXISTS; the MV is only issued
 * when absent/cancelled. Does NOT wait for the MV build — readiness is polled
 * separately (getSplitTablesReadiness / the grouper's not-ready skip).
 */
export const provisionSplitTablesForProject = async (params: {
  projectId: string;
  retentionDays?: number | null;
  replication?: number;
}): Promise<void> => {
  const { projectId } = params;
  const replication = params.replication ?? env.DORIS_REPLICATION_NUM;
  const names = physical(projectId);
  const { eventsFull, tracesScalar } = buildSplitTableStatements({
    projectId,
    retentionDays: params.retentionDays ?? null,
    replication,
  });

  await commandDoris({
    query: eventsFull,
    tags: { feature: "table-split", kind: "create-events-full", projectId },
  });
  await commandDoris({
    query: tracesScalar,
    tags: { feature: "table-split", kind: "create-traces-scalar", projectId },
  });

  // Reconcile TTL to the CURRENT retention on both base tables. CREATE IF NOT
  // EXISTS is a no-op for an already-existing table, so its dynamic_partition
  // keeps the OLD retention — the ALTER makes a Project.retentionDays change
  // (setRetention / billing) take effect. Harmless right after a fresh CREATE
  // (re-sets the same value). This is why re-enqueuing provisioning is the
  // retention-change propagation path.
  for (const table of [names.eventsFull, names.tracesScalar]) {
    await commandDoris({
      query: buildAlterTtlStatement({
        physicalTable: table,
        retentionDays: params.retentionDays ?? null,
      }),
      tags: { feature: "table-split", kind: "alter-ttl", projectId },
    });
  }

  const mvStatus = await getSplitMvStatus(names.eventsFull, names.mv);
  if (mvStatus === "absent" || mvStatus === "cancelled") {
    logger.info(
      `[table-split] creating MV ${names.mv} (status was ${mvStatus})`,
    );
    await commandDoris({
      query: buildTraceMetricsAggMV({
        mvName: names.mv,
        baseTable: names.eventsFull,
      }),
      tags: { feature: "table-split", kind: "create-mv", projectId },
    });
  } else {
    logger.info(`[table-split] MV ${names.mv} already ${mvStatus}, skipping`);
  }
};

export type SplitTablesReadiness = {
  ready: boolean;
  eventsFullExists: boolean;
  tracesScalarExists: boolean;
  mvStatus: SplitMvStatus;
};

/**
 * Readiness gate (Stage 1.2c): a split project is writable/queryable once BOTH
 * base tables exist and its MV build has FINISHED. The grouper must not cut a
 * project's lane until this is true (MV not ready ⇒ base queries still work via
 * the nested fallback, but the rollup rewrite would miss; writes need the base
 * tables). Callers cache the true result — readiness is monotonic once set.
 */
export const getSplitTablesReadiness = async (
  projectId: string,
): Promise<SplitTablesReadiness> => {
  const names = physical(projectId);
  const [eventsFullExists, tracesScalarExists] = await Promise.all([
    dorisTableExists(names.eventsFull),
    dorisTableExists(names.tracesScalar),
  ]);
  const mvStatus = eventsFullExists
    ? await getSplitMvStatus(names.eventsFull, names.mv)
    : ("absent" as SplitMvStatus);
  return {
    ready: eventsFullExists && tracesScalarExists && mvStatus === "finished",
    eventsFullExists,
    tracesScalarExists,
    mvStatus,
  };
};
