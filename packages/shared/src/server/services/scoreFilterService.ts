import { FilterState } from "../../types";
import { FilterList } from "../queries";
import { createDorisFilterFromFilterState } from "../queries/doris-sql/factory";
import { tracesTableUiColumnDefinitionsForDoris } from "../tableMappings/mapTracesTable";
import { queryDoris } from "../repositories/doris";
import { convertDateToAnalyticsDateTime } from "../repositories/analytics";

/**
 * Cap on trace_ids returned from a score filter. Mirrors the comment-filter
 * threshold; keeps the injected `trace_id IN (...)` bounded.
 */
export const SCORE_FILTER_THRESHOLD = 50000;

// A filterState entry is a score filter when its column maps to a scores-table
// column definition (same detection requiresScoresJoin uses).
export const isScoreFilterColumn = (column: string): boolean =>
  tracesTableUiColumnDefinitionsForDoris.some(
    (c) =>
      (c.uiTableName === column || c.uiTableId === column) &&
      c.tableName === "scores",
  );

/**
 * Pre-resolve score filters into a trace_id list so the trace-list query never
 * has to JOIN the scores table (which would pull it off the traces_mv fast path).
 *
 * Scores live in their own table (not events_full), so they are a separate axis:
 * we run the score predicate against `scores`, collect the matching trace_ids,
 * and replace the score filters with a `trace_id IN (...)` filter. The events_full
 * query then stays MV-eligible. Mirrors applyCommentFilters.
 *
 * The resolver reuses the SAME scores_avg aggregation and the SAME predicate
 * generation (createDorisFilterFromFilterState over the scores column defs) as the
 * legacy base-builder scores JOIN, so results are semantically identical.
 *
 * Returns:
 *   - filterState: score filters replaced by an id-IN filter (unchanged if none)
 *   - hasNoMatches: true if score filters matched nothing (caller returns empty)
 */
export async function applyScoreFilters({
  filterState,
  projectId,
}: {
  filterState: FilterState;
  projectId: string;
}): Promise<{ filterState: FilterState; hasNoMatches: boolean }> {
  const scoreFilters = filterState.filter((f) => isScoreFilterColumn(f.column));
  if (scoreFilters.length === 0) {
    return { filterState, hasNoMatches: false };
  }

  // Score time window mirrors the base-builder scores_avg CTE: scope scores to
  // the trace timestamp lower bound minus 1h (so late-arriving scores count).
  const fromFilter = filterState.find(
    (f) => f.type === "datetime" && (f.operator === ">=" || f.operator === ">"),
  );

  // Same predicate the legacy scores JOIN applied (references s.scores_avg /
  // s.score_categories via the scores column defs' select expressions).
  const predicate = new FilterList(
    createDorisFilterFromFilterState(
      scoreFilters,
      tracesTableUiColumnDefinitionsForDoris,
    ),
  ).apply();

  const query = `
    WITH scores_avg AS (
      SELECT
        project_id,
        trace_id,
        collect_list(
          CASE WHEN data_type IN ('NUMERIC', 'BOOLEAN') THEN
            struct(name, avg_value)
          END
        ) AS scores_avg,
        array_except(
          collect_list(
            CASE WHEN data_type = 'CATEGORICAL' AND string_value IS NOT NULL AND string_value != '' THEN
              CONCAT(name, ':', string_value)
            ELSE NULL END
          ),
          [NULL]
        ) AS score_categories
      FROM (
        SELECT
          project_id,
          trace_id,
          name,
          data_type,
          string_value,
          avg(value) as avg_value
        FROM scores s
        WHERE
          project_id = {projectId: String}
          ${fromFilter ? `AND s.timestamp >= DATE_SUB({traceTimestamp: DateTime}, INTERVAL 1 HOUR)` : ""}
        GROUP BY
          project_id,
          trace_id,
          name,
          data_type,
          string_value
      ) tmp
      GROUP BY project_id, trace_id
    )
    SELECT DISTINCT trace_id
    FROM scores_avg s
    ${predicate.query ? `WHERE ${predicate.query}` : ""}
    LIMIT ${SCORE_FILTER_THRESHOLD}
  `;

  const rows = await queryDoris<{ trace_id: string }>({
    query,
    params: {
      projectId,
      ...(fromFilter
        ? {
            traceTimestamp: convertDateToAnalyticsDateTime(
              fromFilter.value as Date,
            ),
          }
        : {}),
    },
    tags: {
      feature: "tracing",
      type: "score-filter",
      kind: "resolve-trace-ids",
      projectId,
    },
  });

  const traceIds = rows.map((r) => r.trace_id);

  // Score filters present but nothing matched → caller should return empty.
  if (traceIds.length === 0) {
    return { filterState: [], hasNoMatches: true };
  }

  const updatedFilterState: FilterState = filterState.filter(
    (f) => !isScoreFilterColumn(f.column),
  );
  updatedFilterState.push({
    type: "stringOptions",
    operator: "any of",
    column: "id",
    value: traceIds,
  });

  return { filterState: updatedFilterState, hasNoMatches: false };
}
