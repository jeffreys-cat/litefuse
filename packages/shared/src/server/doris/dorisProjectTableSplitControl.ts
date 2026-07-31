import { prisma } from "../../db";
import { logger } from "../logger";
import { enqueueDorisSplitTableProvisioning } from "../redis/dorisSplitTableProvisioningQueue";
import { publishSplitCacheInvalidation } from "./tableSplitCache";

/**
 * Minimum per-project retention (days), Stage 1.8 / design §4.5. A split table
 * is dynamic_partition: once a day-partition ages past retention it is DROPPED.
 * Retention below the "retry horizon" — max(label-keep window, DLQ redrive
 * window), ≥7d in practice — would let a partition be dropped while a job
 * targeting it is still redrivable / reconcilable, i.e. silent data loss. A
 * finite retention below this floor is rejected; null (no TTL) is always fine.
 */
export const RETENTION_FLOOR_DAYS = 7;

/**
 * Designate a project for table-split (or update its settings) and trigger
 * provisioning (Stage 1.2b hook). Writes the control row and enqueues the
 * idempotent per-project provisioning job.
 *
 * ORDERING (Stage 1.2 readiness): a project's split must only go LIVE
 * (split=true) once its physical tables exist. So designation defaults to
 * split=false — call this to create the row + kick provisioning, then flip
 * split=true via the readiness gate (Stage 1.2c) once the tables + MV are
 * ready. Passing split=true here is allowed but only safe for a project that is
 * not yet ingesting (provisioned before its first trace).
 *
 * TODO(1.2e): publish a Redis invalidation so every process refreshes its
 * split-cache immediately instead of waiting for the periodic refresh.
 */
export const upsertDorisProjectTableSplit = async (params: {
  projectId: string;
  split?: boolean;
  retentionDays?: number | null;
  note?: string | null;
}): Promise<void> => {
  const { projectId, split, retentionDays, note } = params;
  // Retention floor (Stage 1.8): a finite retention below the retry horizon
  // could TTL-drop a partition while its job is still redrivable → data loss.
  if (
    retentionDays !== undefined &&
    retentionDays !== null &&
    retentionDays < RETENTION_FLOOR_DAYS
  ) {
    throw new Error(
      `doris table-split retentionDays=${retentionDays} for ${projectId} is below the floor of ${RETENTION_FLOOR_DAYS} days (retry horizon) — data could be TTL-dropped before a redrive/reconcile completes`,
    );
  }
  await prisma.dorisProjectTableSplit.upsert({
    where: { projectId },
    update: {
      ...(split !== undefined ? { split } : {}),
      ...(retentionDays !== undefined ? { retentionDays } : {}),
      ...(note !== undefined ? { note } : {}),
    },
    create: {
      projectId,
      split: split ?? false,
      retentionDays: retentionDays ?? null,
      note: note ?? null,
    },
  });
  logger.info(
    `[table-split] designated ${projectId} (split=${split ?? false}, retentionDays=${retentionDays ?? "none"}); enqueuing provisioning`,
  );
  // Eager invalidation so a retention/split change propagates immediately.
  await publishSplitCacheInvalidation();
  await enqueueDorisSplitTableProvisioning(projectId);
};

/** Remove a project's split designation (control row). Does NOT drop the Doris
 * tables — that is the project-deletion / un-split flow's responsibility. */
export const deleteDorisProjectTableSplit = async (
  projectId: string,
): Promise<void> => {
  await prisma.dorisProjectTableSplit
    .delete({ where: { projectId } })
    .catch(() => undefined); // idempotent — already gone is fine
  await publishSplitCacheInvalidation();
};

/**
 * Write-path "table doesn't exist" three-way decision (Stage 1.2d). When a load
 * targets a split project's events_full_<pid> / traces_scalar_<pid> that is
 * absent, the caller (the group-job load path, Stage 1.6) must NOT guess:
 *   - reprovision   : the project still exists → re-enqueue provisioning and
 *                     RETRY the job (tables will exist on the retry);
 *   - skip-tombstoned: the project is gone (deleted) → dead-letter/skip the
 *                     group; recreating its tables would resurrect dead data;
 *   - pg-error      : PG unreachable → cannot decide → RETRY (never guess).
 */
export type MissingSplitTableAction =
  | "reprovision"
  | "skip-tombstoned"
  | "pg-error";

export const classifyMissingSplitTable = async (
  projectId: string,
): Promise<MissingSplitTableAction> => {
  try {
    const project = await prisma.project.findUnique({
      where: { id: projectId },
      select: { id: true, deletedAt: true },
    });
    if (!project || project.deletedAt) return "skip-tombstoned";
    return "reprovision";
  } catch (e) {
    logger.error(
      `[table-split] classifyMissingSplitTable PG lookup failed for ${projectId}`,
      e,
    );
    return "pg-error";
  }
};

/**
 * Act on a missing split table and tell the caller whether to retry or skip.
 * "retry" ⇒ throw/fail the job so BullMQ re-runs it (tables provisioned by
 * then, or PG recovered); "skip" ⇒ the project is tombstoned, drop the group.
 */
export const handleMissingSplitTable = async (
  projectId: string,
): Promise<"retry" | "skip"> => {
  const action = await classifyMissingSplitTable(projectId);
  switch (action) {
    case "reprovision":
      await enqueueDorisSplitTableProvisioning(projectId);
      logger.warn(
        `[table-split] missing tables for live project ${projectId}; re-enqueued provisioning, retrying job`,
      );
      return "retry";
    case "pg-error":
      return "retry";
    case "skip-tombstoned":
      logger.warn(
        `[table-split] missing tables for tombstoned project ${projectId}; skipping group`,
      );
      return "skip";
  }
};
