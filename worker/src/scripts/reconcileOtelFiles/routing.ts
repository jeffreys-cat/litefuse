import { laneFor } from "@langfuse/shared/src/server";

/**
 * Lane-aware grouping-key selection for the otel reconcile backstop.
 *
 * In the all-split model, every project registers into its dedicated lane
 * `lane-<projectId>`. Reconcile must inspect and re-inject into that same lane;
 * shared pending shards are no longer telemetry write targets.
 *
 * `resolveLane` is injected (defaults to the real `laneFor`) purely so these
 * stay pure/unit-testable without standing up the split-cache + env state.
 */

/**
 * The grouping key where this file's registered key / pending entry lives.
 */
export const candidateGroupingKeys = (
  projectId: string,
  _shards: string[],
  resolveLane: (projectId: string) => string = laneFor,
): string[] => [resolveLane(projectId)];

/**
 * The single grouping key to re-inject this file into. Mirrors the ingestion
 * write path.
 */
export const reinjectionGroupingKey = (
  projectId: string,
  _shards: string[],
  _pickShard: (shards: string[]) => string,
  resolveLane: (projectId: string) => string = laneFor,
): string => resolveLane(projectId);
