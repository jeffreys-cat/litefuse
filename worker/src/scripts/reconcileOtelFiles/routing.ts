import { laneFor } from "@langfuse/shared/src/server";

/**
 * Lane-aware grouping-key selection for the otel reconcile backstop.
 *
 * reconcile predates the per-project table split; it only ever knew about the
 * shared shard pool. For a SPLIT project the ingestion write path
 * (OtelIngestionProcessor) registers a file into its dedicated lane
 * `lane-<projectId>`, not a shard — so a lane-blind reconcile (a) misses the
 * split project's registered-key evidence (misclassifying a lost file as
 * `audit` instead of `reinject`) and (b) re-injects into the wrong pool. These
 * two helpers restore lane awareness; both degrade to the original shard-only
 * behaviour when `resolveLane` returns null (project not designated for split).
 *
 * `resolveLane` is injected (defaults to the real `laneFor`) purely so these
 * stay pure/unit-testable without standing up the split-cache + env state.
 */

/**
 * Every grouping key where THIS file's registered key / pending entry could
 * live — used for classification evidence and registered-key cleanup. A split
 * project's dedicated lane comes first, but shards are STILL included so a file
 * that was registered while the project was non-split (before a split flip) is
 * still found under its old shard.
 */
export const candidateGroupingKeys = (
  projectId: string,
  shards: string[],
  resolveLane: (projectId: string) => string | null = laneFor,
): string[] => {
  const lane = resolveLane(projectId);
  return lane ? [lane, ...shards] : shards;
};

/**
 * The single grouping key to re-inject THIS file into: a split project's own
 * lane, else a shard picked by `pickShard`. Mirrors the ingestion write path so
 * a re-injected split-project file lands in the same lane its live traffic does
 * (a group cut from that lane is naturally single-project → targets
 * events_full_<projectId>).
 */
export const reinjectionGroupingKey = (
  projectId: string,
  shards: string[],
  pickShard: (shards: string[]) => string,
  resolveLane: (projectId: string) => string | null = laneFor,
): string => resolveLane(projectId) ?? pickShard(shards);
