import { env } from "../../env";
import { DorisTableName } from "./schema";

/**
 * Per-project table split routing (design docs/project-per-table-*.md).
 *
 * THREE functions, each one concern, none named `shard` (that word stays
 * reserved for BullMQ queue sharding — design §4.2d):
 *   - isSplitProject(pid): does this project get its own tables?
 *   - tableFor(pid, logical): the PHYSICAL table name to read/write.
 *   - laneFor(pid): the grouping-key (Redis pending lane) a file registers into.
 *
 * Stage 0 ships with LITEFUSE_DORIS_TABLE_SPLIT_MODE defaulting to "none", so
 * tableFor is the identity of today's shared-table names and laneFor is a no-op
 * placeholder — the 300+ literal call sites can be migrated to these functions
 * with zero behaviour change. Stage 1 wires the `project_id_with_rule` control
 * table and the actual lane routing.
 */

/**
 * Whether a project's telemetry lives in its own `<logical>_<projectId>`
 * tables (vs the shared tables).
 *
 * - none                 → always false (current behaviour / rollback position)
 * - project_id           → always true (only safe for bounded project counts)
 * - project_id_with_rule → per the PG control table doris_project_table_split
 *   (Stage 1). Stage 0 stub returns false until that table exists.
 */
export const isSplitProject = (_projectId: string): boolean => {
  switch (env.LITEFUSE_DORIS_TABLE_SPLIT_MODE) {
    case "none":
      return false;
    case "project_id":
      return true;
    case "project_id_with_rule":
      // Stage 1: query doris_project_table_split (cached, no negative cache;
      // fail-and-retry on PG error — never guess). Stage 0 stub: not split.
      return false;
    default:
      return false;
  }
};

/**
 * Physical table name for a project's logical table.
 *
 * Split project → `<logical>_<projectId>`; otherwise the shared logical name.
 * Only the two per-project tables (events_full / traces_scalar) and their MV
 * (trace_metrics_agg) are ever split; any other logical name is returned as-is
 * regardless of split status (they are never per-project).
 */
const SPLITTABLE_TABLES: ReadonlySet<string> = new Set<DorisTableName>([
  "events_full",
  "traces_scalar",
]);

export const tableFor = (
  projectId: string,
  logical: DorisTableName,
): string => {
  if (!SPLITTABLE_TABLES.has(logical)) return logical;
  return isSplitProject(projectId) ? `${logical}_${projectId}` : logical;
};

/**
 * Physical name of a split project's synchronous MV (mirrors tableFor).
 * The MV is only split when its base table is.
 */
export const metricsAggTableFor = (projectId: string): string =>
  isSplitProject(projectId)
    ? `trace_metrics_agg_${projectId}`
    : "trace_metrics_agg";

/**
 * The grouping-key (Redis pending lane) a project's uploaded file registers
 * into. Split project → its dedicated lane `lane-<projectId>` (a group cut from
 * it is naturally single-project). Non-split → null, meaning "use the shared
 * shard pool" (the caller keeps its existing random-shard selection).
 *
 * Deliberately returns null (not a shard) for the shared case: shard selection
 * stays where it is today (OtelIngestionProcessor); this function only decides
 * "dedicated lane or not". Stage 1 wires it into registration.
 */
export const laneFor = (projectId: string): string | null =>
  isSplitProject(projectId) ? `lane-${projectId}` : null;
