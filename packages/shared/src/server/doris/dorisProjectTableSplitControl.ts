import { prisma } from "../../db";
import { env } from "../../env";
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

/** Free-tier split-table TTL (days), fixed. Retention is a PAID feature — a free
 * org's projects keep this fixed short window regardless of Project.retentionDays. */
export const FREE_RETENTION_DAYS = 30;

/** Paid default TTL (days) when a paid org has set no explicit Project.retentionDays. */
export const PAID_DEFAULT_RETENTION_DAYS = 3 * 365;

/**
 * The effective split-table TTL (days) for a project, derived from PG at
 * provisioning time. Table split is now UNIVERSAL (every project); retention
 * stays a PAID feature, decoupled:
 *   - free org → FREE_RETENTION_DAYS (fixed 30d; Project.retentionDays ignored)
 *   - paid org → Project.retentionDays (user-set), else PAID_DEFAULT_RETENTION_DAYS
 * Floor-clamped (RETENTION_FLOOR_DAYS). Async (PG read) — call only in async
 * contexts (the provisioning job, the group-load retention filter), NEVER the
 * synchronous isSplitProject hot path.
 */
export const getSplitRetentionDays = async (
  projectId: string,
): Promise<number> => {
  const project = await prisma.project.findUnique({
    where: { id: projectId },
    select: {
      retentionDays: true,
      organization: { select: { cloudConfig: true } },
    },
  });
  if (!project) return FREE_RETENTION_DAYS;
  const raw = isOrgPaid(project.organization.cloudConfig)
    ? (project.retentionDays ?? PAID_DEFAULT_RETENTION_DAYS)
    : FREE_RETENTION_DAYS;
  return raw < RETENTION_FLOOR_DAYS ? RETENTION_FLOOR_DAYS : raw;
};

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
  note?: string | null;
}): Promise<void> => {
  const { projectId, split, note } = params;
  // Retention (split-table TTL) is NOT stored here — it is single-sourced on
  // Project.retentionDays and read (+ floor-clamped) at provisioning time.
  await prisma.dorisProjectTableSplit.upsert({
    where: { projectId },
    update: {
      ...(split !== undefined ? { split } : {}),
      ...(note !== undefined ? { note } : {}),
    },
    create: {
      projectId,
      split: split ?? false,
      note: note ?? null,
    },
  });
  logger.info(
    `[table-split] designated ${projectId} (split=${split ?? false}); enqueuing provisioning`,
  );
  // The control row above IS the designation guarantee (write path resolves it,
  // with a PG fallback, so a designated project never leaks to the shared
  // table). Propagation + the provisioning kick are RECOVERABLE (periodic
  // refresh + grouper self-heal / reconcile re-drive them), so a Redis hiccup
  // here must NOT fail the caller (e.g. project creation).
  try {
    await publishSplitCacheInvalidation();
  } catch (e) {
    logger.error(
      `[table-split] cache invalidation for ${projectId} failed`,
      e,
    );
  }
  try {
    await enqueueDorisSplitTableProvisioning(projectId);
  } catch (e) {
    logger.error(
      `[table-split] provisioning enqueue for ${projectId} failed`,
      e,
    );
  }
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

/** Paying customer iff cloudConfig has an active Stripe subscription with a
 * resolved paid plan — the same test getPlan.ts uses (activeSubscriptionId &&
 * resolvedPlan). Used by getSplitRetentionDays to pick the paid vs free TTL —
 * table split itself is universal (billing-independent). */
const isOrgPaid = (cloudConfig: unknown): boolean => {
  const stripe = (
    cloudConfig as {
      stripe?: {
        activeSubscriptionId?: string | null;
        resolvedPlan?: string | null;
      };
    } | null
  )?.stripe;
  return Boolean(stripe?.activeSubscriptionId && stripe?.resolvedPlan);
};

/**
 * Designate a newly-created project for table split and kick provisioning.
 * Table split is UNIVERSAL — every project gets its own events_full_<pid> /
 * traces_scalar_<pid> tables, independent of billing (retention TTL stays
 * paid-differentiated, derived at provisioning by getSplitRetentionDays).
 * Idempotent (upsert omits `split`, CREATE IF NOT EXISTS, per-project queue
 * de-dups). No-op unless mode = project_id_with_rule.
 */
export const provisionSplitForNewProject = async (
  projectId: string,
): Promise<void> => {
  if (env.LITEFUSE_DORIS_TABLE_SPLIT_MODE !== "project_id_with_rule") return;
  await upsertDorisProjectTableSplit({ projectId });
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
