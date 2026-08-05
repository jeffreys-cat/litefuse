import { env } from "../../env";
import { DorisTableName } from "./schema";
import {
  splitProjectInCache,
  splitProjectDesignatedInCache,
  resolveIngestionSplitState,
} from "./tableSplitCache";

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
export const isSplitProject = (projectId: string): boolean => {
  switch (env.LITEFUSE_DORIS_TABLE_SPLIT_MODE) {
    case "none":
      return false;
    case "project_id":
      return true;
    case "project_id_with_rule":
      // Cached snapshot of doris_project_table_split (tableSplitCache): only
      // split=true projects are held, a miss = not split (no negative cache).
      // Cold cache (never loaded) reads as not-split; the write path gates on
      // isSplitCacheReady() so it never misroutes on a cold/failed cache.
      return splitProjectInCache(projectId);
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
 * Named escape hatch: the SHARED logical table name, on purpose. Returns the
 * logical name verbatim in every split mode — use it at the (few) call sites
 * that legitimately read the shared table with NO single projectId to route on
 * (cross-project scans: telemetry counts, the projectId-less trace redirect).
 *
 * It exists so those sites read as a deliberate `sharedTableFor("events_full")`
 * rather than a bare `events_full` literal the lint rule can't tell apart from
 * an un-migrated one. Under table split these sites still owe a fan-out over
 * every split project's table (design §五); until then this is the shared read.
 */
export const sharedTableFor = (logical: DorisTableName): string => logical;

/**
 * Reverse of tableFor / metricsAggTableFor: map a PHYSICAL table name back to
 * its logical name. `events_full_<pid>` → `events_full`; a shared/unknown name
 * is returned unchanged. Use this before any lookup keyed on the logical table
 * (metadata-format branch, DATE_FIELD_MAPPINGS, etc.) so split projects resolve
 * the same way the shared tables do. Safe for non-split names (identity).
 *
 * The three splittable logicals have no shared prefix, so the `<logical>_`
 * match is unambiguous.
 */
const SPLITTABLE_LOGICALS = [
  "events_full",
  "traces_scalar",
  "trace_metrics_agg",
] as const;

export const toLogicalTable = (physical: string): string => {
  for (const logical of SPLITTABLE_LOGICALS) {
    if (physical === logical || physical.startsWith(`${logical}_`)) {
      return logical;
    }
  }
  return physical;
};

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

/**
 * DESIGNATED-aware lane (live OR pending), synchronous against the loaded
 * snapshot. For the offline reconcile: a PENDING project's files must re-inject
 * into its own lane (held until the tables exist) rather than a shared shard —
 * so reconcile never loads a designated project's data into the shared table.
 * Reconcile primes the cache (refreshSplitCache) up front, so no PG fallback is
 * needed here. NOT for the live write path (use laneForIngestion).
 */
export const laneForDesignated = (projectId: string): string | null =>
  splitProjectDesignatedInCache(projectId) ? `lane-${projectId}` : null;

/**
 * ASYNC lane decision for the WRITE registration path (the only correct one for
 * ingestion). A project that is designated for split — whether LIVE (tables
 * ready) or PENDING (tables still provisioning) — registers into its dedicated
 * `lane-<projectId>`, so its files are captured there and held by the grouper
 * (which cuts a lane only once it is live) instead of leaking to the shared
 * pool. Uses resolveIngestionSplitState, which falls back to a single PG lookup
 * on a cache miss, closing the pub/sub propagation window: a paid org's
 * brand-new project can never route its first rows to the shared table.
 *
 * Only the offline reconcile uses the synchronous laneFor (live-only) above.
 */
export const laneForIngestion = async (
  projectId: string,
): Promise<string | null> => {
  // Mode switch mirrors isSplitProject — never touch the cache / PG in the
  // modes that don't use the control table (else `none`/`project_id`, which
  // don't run the refresh loop, would PG-probe every project's first write).
  switch (env.LITEFUSE_DORIS_TABLE_SPLIT_MODE) {
    case "none":
      return null;
    case "project_id":
      return `lane-${projectId}`;
    case "project_id_with_rule": {
      const state = await resolveIngestionSplitState(projectId);
      return state === "live" || state === "pending"
        ? `lane-${projectId}`
        : null;
    }
    default:
      return null;
  }
};
