import { Job, Processor } from "bullmq";
import {
  QueueName,
  TQueueJobTypes,
  getCurrentSpan,
  logger,
  provisionSplitTablesForProject,
  getSplitTablesReadiness,
  publishSplitCacheInvalidation,
  SPLIT_SCHEMA_VERSION,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

/**
 * Provision a split project's Doris tables + MV (Stage 1.2b). Idempotent and
 * retry-safe (jobId = projectId serialises per project). Reads the CURRENT
 * retention from the control table (source of truth) rather than the payload,
 * so a retention change re-provisions correctly.
 *
 * The MV build is async; this job returns after CREATE has been issued. It logs
 * the readiness snapshot but does NOT block on the MV finishing — the readiness
 * gate (grouper not-ready skip / getSplitTablesReadiness) handles that.
 */
export const dorisSplitTableProvisioningProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.DorisSplitTableProvisioningQueue]>,
): Promise<void> => {
  const { projectId } = job.data.payload;

  const span = getCurrentSpan();
  span?.setAttribute("messaging.bullmq.job.input.projectId", projectId);

  const control = await prisma.dorisProjectTableSplit.findUnique({
    where: { projectId },
    select: { retentionDays: true },
  });
  if (!control) {
    // No control row → the project is no longer designated to split (e.g. the
    // row was removed before this job ran). Nothing to provision.
    logger.info(
      `[table-split] no control row for ${projectId}; skipping provisioning`,
    );
    return;
  }

  logger.info(
    `[table-split] provisioning tables for ${projectId} (retentionDays=${control.retentionDays ?? "none"})`,
  );
  await provisionSplitTablesForProject({
    projectId,
    retentionDays: control.retentionDays,
  });

  const readiness = await getSplitTablesReadiness(projectId);
  // Go LIVE once the BASE tables exist (provision creates them synchronously).
  // The MV may still be building — that is fine: reads route to the now-existing
  // tables (empty until data), registration goes to the lane, and the grouper's
  // own readiness gate (getSplitTablesReadiness → MV FINISHED) holds cutting
  // until the MV is ready, so no data is written before the rollup is live.
  // Flipping here (not at designation) keeps the not-split window to the few
  // seconds of CREATE TABLE, avoiding both read-errors on a missing table and
  // stranding a new project's first rows in the shared table.
  if (!readiness.eventsFullExists || !readiness.tracesScalarExists) {
    // Should not happen — CREATE TABLE is synchronous. Retry.
    throw new Error(
      `[table-split] base tables missing after provisioning ${projectId} (${JSON.stringify(readiness)})`,
    );
  }
  await prisma.dorisProjectTableSplit.update({
    where: { projectId },
    data: { split: true, schemaVersion: SPLIT_SCHEMA_VERSION },
  });
  await publishSplitCacheInvalidation();
  logger.info(
    `[table-split] ${projectId} live (split=true, schemaVersion=${SPLIT_SCHEMA_VERSION}); mvStatus=${readiness.mvStatus}`,
  );
};
