import {
  deleteDatasetRunItemsByProjectId,
  deleteDorisProjectTableSplit,
  deleteScoresByProjectId,
  dropSplitTablesForProject,
  logger,
  refreshSplitCache,
} from "@langfuse/shared/src/server";

/** Idempotent Doris-only cleanup shared by queued and batch project deletion. */
export const cleanupDorisProjectData = async (
  projectId: string,
): Promise<void> => {
  await deleteDorisProjectTableSplit(projectId);
  await refreshSplitCache();
  await dropSplitTablesForProject(projectId);

  const [scoreResidue, datasetResidue] = await Promise.all([
    deleteScoresByProjectId(projectId),
    deleteDatasetRunItemsByProjectId(projectId),
  ]);

  logger.info("Completed idempotent Doris project cleanup", {
    projectId,
    scoreResidue,
    datasetResidue,
  });
};
