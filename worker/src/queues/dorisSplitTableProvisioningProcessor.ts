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
  getSplitRetentionDays,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

/**
 * Provision a split project's Doris tables + MV (Stage 1.2b). Idempotent and
 * retry-safe (jobId = projectId serialises per project). The control row's
 * EXISTENCE is the "designated to split" gate; the retention (TTL) is read
 * fresh from Project.retentionDays (single source), so a setRetention / billing
 * change re-provisions the TTL correctly (provisioning ALTERs it).
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
    select: { projectId: true },
  });
  if (!control) {
    // No control row → the project is no longer designated to split (e.g. the
    // row was removed before this job ran). Nothing to provision.
    logger.info(
      `[table-split] no control row for ${projectId}; skipping provisioning`,
    );
    return;
  }

  // Retention (split-table TTL) is single-sourced on Project.retentionDays,
  // floor-clamped (helper).
  const retentionDays = await getSplitRetentionDays(projectId);

  logger.info(
    `[table-split] provisioning tables for ${projectId} (retentionDays=${retentionDays ?? "none"})`,
  );
  await provisionSplitTablesForProject({
    projectId,
    retentionDays,
  });

  const readiness = await getSplitTablesReadiness(projectId);
  // Go LIVE once the BASE tables exist (provision creates them synchronously).
  // The MV may still be building — that is fine: reads route to the now-existing
  // tables (empty until data), registration goes to the lane, and the grouper's
  // own readiness gate (getSplitTablesReadiness → MV FINISHED) holds cutting
  // until the MV is ready, so no data is written before the rollup is live.
  // Flipping here (not at designation) keeps the pending window to the few
  // seconds of CREATE TABLE; project-lane files wait until readiness completes.
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
