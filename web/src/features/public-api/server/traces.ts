import {
  type TraceRecordReadType,
  deriveFilters,
  createPublicApiTracesColumnMapping,
  tracesTableUiColumnDefinitionsForDoris,
  queryDoris,
  convertDateToAnalyticsDateTime,
  dq,
  convertDorisTracesListToDomain,
  orderByToDorisSQL,
  type DateTimeFilter,
} from "@langfuse/shared/src/server";
import {
  type OrderByState,
  tracesTableCols,
  InvalidRequestError,
} from "@langfuse/shared";
import { type TraceFieldGroup } from "@/src/features/public-api/types/traces";

import type { FilterState } from "@langfuse/shared";
import snakeCase from "lodash/snakeCase";

export type TraceQueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  name?: string;
  type?: string;
  sessionId?: string;
  version?: string;
  release?: string;
  tags?: string | string[];
  environment?: string | string[];
  fromTimestamp?: string;
  toTimestamp?: string;
  fields?: TraceFieldGroup[];
  useEventsTable?: boolean | null;
};

export const generateTracesForPublicApi = async ({
  props,
  advancedFilters,
  orderBy,
}: {
  props: TraceQueryType;
  advancedFilters?: FilterState;
  orderBy: OrderByState;
}) => {
  // Doris implementation - reuse same filter logic
  const filter = deriveFilters(
    props,
    filterParams,
    advancedFilters,
    tracesTableUiColumnDefinitionsForDoris,
    tracesTableCols,
  );
  rejectNonTracesFilters(filter);
  // appliedFilter is serialized BEFORE we strip prefixes below, so the outer
  // WHERE keeps `t.<col>` references where `t` is in scope.
  const appliedFilter = filter.apply();

  const timeFilter = filter.find(
    (f: any) =>
      f.table === "traces" &&
      f.field.includes("timestamp") &&
      (f.operator === ">=" || f.operator === ">"),
  ) as DateTimeFilter | undefined;

  // The environment filter is reused inside the observations/scores CTEs, where
  // alias `t` is not in scope. Drop the prefix on the env filter copies so they
  // reference the bare `environment` column on those tables. Mirrors upstream CK.
  const environmentFilter = filter.filter(
    (f: any) => f.field === "environment",
  );
  environmentFilter.forEach((f: any) => {
    f.tablePrefix = undefined;
  });
  const appliedEnvironmentFilter = environmentFilter.apply();

  // Skip indexes logic still applies to Doris
  const shouldUseSkipIndexes = filter.some(
    (f: any) =>
      f.table === "traces" &&
      ["user_id", "session_id", "metadata"].some((skipIndexCol) =>
        f.field.includes(skipIndexCol),
      ),
  );

  const dorisOrderBy =
    (orderByToDorisSQL(orderBy || [], orderByColumns) ||
      "ORDER BY t.timestamp desc") +
    (shouldUseSkipIndexes ? ", t.event_ts desc" : "");

  const query = `
      WITH observation_stats AS (
        SELECT
          trace_id,
          project_id,
          sum(total_cost) as total_cost,
          milliseconds_diff(
            CASE WHEN max(start_time) > max(end_time) THEN max(start_time) ELSE max(end_time) END,
            CASE WHEN min(start_time) < min(end_time) THEN min(start_time) ELSE min(end_time) END
          ) as latency_milliseconds,
          collect_list(id) as observation_ids
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY id, project_id ORDER BY event_ts DESC) as rn
          FROM observations
          WHERE project_id = {projectId: String}
          ${timeFilter ? `AND start_time >= DATE_SUB({cteTimeFilter: DateTime}, INTERVAL 2 DAY)` : ""}
          ${environmentFilter.length() > 0 ? `AND ${appliedEnvironmentFilter.query}` : ""}
        ) o
        WHERE rn = 1
        GROUP BY project_id, trace_id
      ), score_stats AS (
        SELECT
          trace_id,
          project_id,
          collect_set(id) as score_ids
        FROM scores
        WHERE project_id = {projectId: String}
        AND session_id IS NULL
        AND dataset_run_id IS NULL
        ${timeFilter ? `AND timestamp >= DATE_SUB({cteTimeFilter: DateTime}, INTERVAL 2 DAY)` : ""}
        ${environmentFilter.length() > 0 ? `AND ${appliedEnvironmentFilter.query}` : ""}
        GROUP BY project_id, trace_id
      )

      SELECT
        t.id as id,
        CONCAT('/project/', t.project_id, '/traces/', t.id) as htmlPath,
        t.project_id as project_id,
        t.timestamp as timestamp,
        t.name as name,
        t.environment as environment,
        t.input as input,
        t.output as output,
        t.session_id as session_id,
        t.metadata as metadata,
        t.user_id as user_id,
        t.${dq("release")} as ${dq("release")},
        t.version as version,
        t.bookmarked as bookmarked,
        t.${dq("public")} as ${dq("public")},
        t.tags as tags,
        t.created_at as created_at,
        t.updated_at as updated_at,
        s.score_ids as scores,
        o.observation_ids as observations,
        COALESCE(o.latency_milliseconds / 1000, 0) as latency,
        COALESCE(o.total_cost, 0) as totalCost
      FROM (
            SELECT *
            FROM (
              SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY id, project_id ORDER BY event_ts DESC) as rn
              FROM traces
              WHERE project_id = {projectId: String}
              ${timeFilter ? `AND timestamp >= {cteTimeFilter: DateTime}` : ""}
            ) ranked
            WHERE rn = 1
          ) t
      LEFT JOIN observation_stats o ON t.id = o.trace_id AND t.project_id = o.project_id
      LEFT JOIN score_stats s ON t.id = s.trace_id AND t.project_id = s.project_id
      WHERE t.project_id = {projectId: String}
      ${filter.length() > 0 ? `AND ${appliedFilter.query}` : ""}
      ${dorisOrderBy}
      ${props.limit !== undefined && props.page !== undefined ? `LIMIT {limit: Int32} OFFSET {offset: Int32}` : ""}
    `;

  const result = await queryDoris<
    TraceRecordReadType & {
      observations: string[];
      scores: string[];
      totalCost: number;
      latency: number;
      htmlPath: string;
    }
  >({
    query,
    params: {
      ...appliedEnvironmentFilter.params,
      ...appliedFilter.params,
      projectId: props.projectId,
      ...(props.limit !== undefined ? { limit: props.limit } : {}),
      ...(props.page !== undefined
        ? { offset: (props.page - 1) * props.limit }
        : {}),
      ...(timeFilter
        ? {
            cteTimeFilter: convertDateToAnalyticsDateTime(timeFilter.value),
          }
        : {}),
    },
  });

  return convertDorisTracesListToDomain(
    result.map((trace) => ({
      ...trace,
    })),
    {
      metrics: true,
      scores: true,
      observations: true,
    },
  );
};

export const getTracesCountForPublicApi = async ({
  props,
  advancedFilters,
}: {
  props: TraceQueryType;
  advancedFilters?: FilterState;
}) => {
  // Doris implementation - reuse same filter logic
  const dorisFilter = deriveFilters(
    props,
    filterParams,
    advancedFilters,
    tracesTableUiColumnDefinitionsForDoris,
    tracesTableCols,
  );
  rejectNonTracesFilters(dorisFilter);
  const appliedDorisFilter = dorisFilter.apply();

  const dorisQuery = `
      SELECT count() as count
      FROM traces t
      WHERE project_id = {projectId: String}
      ${dorisFilter.length() > 0 ? `AND ${appliedDorisFilter.query}` : ""}
    `;

  const records = await queryDoris<{ count: string }>({
    query: dorisQuery,
    params: { ...appliedDorisFilter.params, projectId: props.projectId },
  });
  return records.map((record) => Number(record.count)).shift();
};

// Reserved words in Doris (e.g. "release", "public") must be backtick-quoted.
// Backtick-quoting is also safe in ClickHouse, so we apply it unconditionally.
const orderByColumns = [
  "id",
  "timestamp",
  "name",
  "userId",
  "release",
  "version",
  "public",
  "bookmarked",
  "sessionId",
].map((name) => {
  const col = snakeCase(name);
  return {
    uiTableName: name,
    uiTableId: name,
    tableName: "traces",
    select: dq(col),
    queryPrefix: "t",
  };
});

// Use factory functions to create column mappings (eliminates duplication with events table)
const filterParams = createPublicApiTracesColumnMapping("traces", "t");

// The Doris public-traces query joins observation_stats / score_stats CTEs that only
// expose aggregate columns (counts, ids, totals). Filters on observation/score columns
// (e.g. level, latency, totalCost, scores_avg) reference columns those CTEs do not
// produce, and would emit SQL that fails at parse time. Reject them up front so the
// caller gets a clear 400 instead of a Doris parser error.
function rejectNonTracesFilters(filterList: {
  forEach: (cb: (f: { table?: string; field: string }) => void) => void;
}) {
  const offending: { table?: string; field: string }[] = [];
  filterList.forEach((f) => {
    if (f.table && f.table !== "traces") offending.push(f);
  });
  if (offending.length > 0) {
    const desc = offending
      .map((f) => `'${f.field}' (table: ${f.table})`)
      .join(", ");
    throw new InvalidRequestError(
      `Filtering on ${desc} is not supported via the public traces API on the Doris backend. Only columns on the traces table are supported.`,
    );
  }
}
