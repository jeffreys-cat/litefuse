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

/**
 * The effective split-table retention (days) for a project: Project.retentionDays
 * (the SINGLE retention source), floor-clamped so a partition can't be
 * TTL-dropped while a job targeting it is still redrivable. null = no TTL.
 *
 * Async (PG read) — call only in async contexts (the provisioning job, the group
 * load path's retention filter), NEVER the synchronous isSplitProject hot path.
 */
export const getSplitRetentionDays = async (
  projectId: string,
): Promise<number | null> => {
  const project = await prisma.project.findUnique({
    where: { id: projectId },
    select: { retentionDays: true },
  });
  const raw = project?.retentionDays ?? null;
  return raw != null && raw < RETENTION_FLOOR_DAYS ? RETENTION_FLOOR_DAYS : raw;
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

/**
 * Paid-org DEFAULT retention (days) — written to Project.retentionDays (the
 * single retention source) when a paid org's project has none set. Table split
 * IS the paid feature (own long-lived tables); the user can still override this
 * default via the retention setting. 3 years, well above RETENTION_FLOOR_DAYS.
 */
export const PAID_SPLIT_RETENTION_DAYS = 3 * 365;

/** Apply the paid default to Project.retentionDays only when unset (never
 * clobber a user-chosen value). Retention is single-sourced on Project. */
const applyPaidRetentionDefault = async (projectId: string): Promise<void> => {
  const project = await prisma.project.findUnique({
    where: { id: projectId },
    select: { retentionDays: true },
  });
  if (project && project.retentionDays == null) {
    await prisma.project.update({
      where: { id: projectId },
      data: { retentionDays: PAID_SPLIT_RETENTION_DAYS },
    });
  }
};

/** Paying customer iff cloudConfig has an active Stripe subscription with a
 * resolved paid plan — the same test getPlan.ts uses (activeSubscriptionId &&
 * resolvedPlan). */
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
 * Billing-driven split provisioning: designate EVERY project of an organization
 * for table-split and kick provisioning. Called when an org transitions to a
 * paid plan (Stripe webhook). Idempotent — the upsert omits the `split` flag so
 * it PRESERVES an already-live split, and CREATE IF NOT EXISTS / the per-project
 * queue (jobId = projectId) de-dup re-runs.
 *
 * Once designated, a project stays split for good: a later cancellation does
 * NOT un-split it (that would strand its data in events_full_<pid> with no read
 * path — the accepted trade-off). Paid enforcement on cancel is handled
 * separately by the billing ingestion-suspension path. No-op unless
 * mode = project_id_with_rule.
 */
export const provisionSplitForPaidOrganization = async (
  orgId: string,
): Promise<void> => {
  if (env.LITEFUSE_DORIS_TABLE_SPLIT_MODE !== "project_id_with_rule") return;
  const projects = await prisma.project.findMany({
    where: { orgId, deletedAt: null },
    select: { id: true },
  });
  logger.info(
    `[table-split] paid org ${orgId} → provisioning split for ${projects.length} project(s)`,
  );
  for (const { id } of projects) {
    await applyPaidRetentionDefault(id);
    await upsertDorisProjectTableSplit({
      projectId: id,
      note: `billing: paid org ${orgId}`,
    });
  }
};

/**
 * Billing-driven split provisioning for a SINGLE newly-created project: if its
 * org is already a paying customer, designate it for split (a paid org's new
 * projects are split too). Called from the project-create flow. No-op unless
 * mode = project_id_with_rule and the org is paid.
 */
export const provisionSplitForNewProjectIfOrgPaid = async (params: {
  projectId: string;
  orgId: string;
}): Promise<void> => {
  if (env.LITEFUSE_DORIS_TABLE_SPLIT_MODE !== "project_id_with_rule") return;
  const org = await prisma.organization.findUnique({
    where: { id: params.orgId },
    select: { cloudConfig: true },
  });
  if (!org || !isOrgPaid(org.cloudConfig)) return;
  await applyPaidRetentionDefault(params.projectId);
  await upsertDorisProjectTableSplit({
    projectId: params.projectId,
    note: `billing: paid org ${params.orgId}`,
  });
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
