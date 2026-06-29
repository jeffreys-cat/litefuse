import { OrderByState } from "../../interfaces/orderBy";
import { tracesTableUiColumnDefinitionsForDoris } from "../tableMappings";
import { UiColumnMappings } from "../../tableDefinitions";
import { FilterState } from "../../types";
import { FilterList } from "../queries";

import { TraceRecordReadType } from "../repositories/definitions";
import Decimal from "decimal.js";
import { ScoreAggregate } from "../../features/scores";
import { TracingSearchType } from "../../interfaces/search";
import { ObservationLevelType, TraceDomain } from "../../domain";
// Doris imports
import { convertDateToAnalyticsDateTime, dq } from "../repositories/analytics";
import { queryDoris } from "../repositories/doris";
import { parseDorisUTCDateTimeFormat } from "../repositories/doris";
import {
  createDorisFilterFromFilterState,
  getDorisProjectIdDefaultFilter,
} from "../queries/doris-sql/factory";
import {
  StringFilter as DorisStringFilter,
  StringOptionsFilter as DorisStringOptionsFilter,
  DateTimeFilter as DorisDateTimeFilter,
} from "../queries/doris-sql/doris-filter";
import { orderByToDorisSQL } from "../queries/doris-sql/orderby-factory";
import { dorisSearchCondition } from "../queries/doris-sql/search";

export type TracesTableReturnType = Pick<
  TraceRecordReadType,
  | "project_id"
  | "id"
  | "name"
  | "timestamp"
  | "bookmarked"
  | "release"
  | "version"
  | "user_id"
  | "session_id"
  | "environment"
  | "tags"
  | "public"
>;

export type TracesTableUiReturnType = Pick<
  TraceDomain,
  | "id"
  | "projectId"
  | "timestamp"
  | "tags"
  | "bookmarked"
  | "name"
  | "release"
  | "version"
  | "userId"
  | "environment"
  | "sessionId"
  | "public"
>;

export type TracesMetricsUiReturnType = {
  id: string;
  projectId: string;
  promptTokens: bigint;
  completionTokens: bigint;
  totalTokens: bigint;
  latency: number | null;
  level: ObservationLevelType;
  observationCount: bigint;
  calculatedTotalCost: Decimal | null;
  calculatedInputCost: Decimal | null;
  calculatedOutputCost: Decimal | null;
  scores: ScoreAggregate;
  usageDetails: Record<string, number>;
  costDetails: Record<string, number>;
  errorCount: bigint;
  warningCount: bigint;
  defaultCount: bigint;
  debugCount: bigint;
};

export const convertToUiTableRows = (
  row: TracesTableReturnType,
): TracesTableUiReturnType => {
  // Doris (via mysql2) returns timestamps as Date objects, but some callers
  // (e.g. legacy paths or JSON-roundtripped rows) supply ISO strings; accept
  // both. TypeScript can't narrow Date | string at runtime so we type-assert.
  const timestampValue = row.timestamp as unknown;
  const timestamp =
    timestampValue instanceof Date
      ? (timestampValue as Date)
      : parseDorisUTCDateTimeFormat(row.timestamp as string);

  // tags is a native ARRAY column on both paths, but Doris transmits ARRAY over
  // the MySQL protocol as a JSON array string (mysql2 does not auto-parse it), so
  // it can arrive as either a JS array or a JSON string. Accept both.
  const rawTags = row.tags as unknown;
  let tags: string[] = [];
  if (Array.isArray(rawTags)) {
    tags = rawTags;
  } else if (typeof rawTags === "string" && rawTags.length > 0) {
    try {
      const parsed = JSON.parse(rawTags);
      if (Array.isArray(parsed)) tags = parsed;
    } catch {
      tags = [];
    }
  }

  return {
    id: row.id,
    projectId: row.project_id,
    timestamp: timestamp,
    tags,
    bookmarked: Boolean(row.bookmarked),
    name: row.name ?? null,
    release: row.release ?? null,
    version: row.version ?? null,
    userId: row.user_id ?? null,
    environment: row.environment ?? null,
    sessionId: row.session_id ?? null,
    public: Boolean(row.public),
  };
};

export type TracesTableMetricsDorisReturnType = {
  id: string;
  project_id: string;
  timestamp: Date;
  level: ObservationLevelType;
  observation_count: number | null;
  latency: string | null;
  // Trace-level rollups of the precomputed per-observation scalar columns
  // (SUM of input_tokens_calculated etc., see migration 0037). Doris may return
  // them as strings, so callers coerce with Number()/Decimal.
  total_cost: number | string | null;
  input_tokens: number | string | null;
  output_tokens: number | string | null;
  total_tokens: number | string | null;
  input_cost: number | string | null;
  output_cost: number | string | null;
  scores_avg: Array<{ name: string; avg_value: number }>;
  score_categories: Array<string>;
  error_count: number | null;
  warning_count: number | null;
  default_count: number | null;
  debug_count: number | null;
};

export const convertToUITableMetrics = (
  row: TracesTableMetricsDorisReturnType,
): Omit<TracesMetricsUiReturnType, "scores"> => {
  return {
    id: row.id,
    projectId: row.project_id,
    latency: Number(row.latency),
    promptTokens: BigInt(Number(row.input_tokens ?? 0)),
    completionTokens: BigInt(Number(row.output_tokens ?? 0)),
    totalTokens: BigInt(Number(row.total_tokens ?? 0)),
    // The per-key usage/cost breakdown shown in the list's hover tooltip is
    // lazy-loaded per trace (getTraceUsageBreakdown) instead of being aggregated
    // here, so the list response carries only the scalar totals above.
    usageDetails: {},
    costDetails: {},
    observationCount: BigInt(row.observation_count ?? 0),
    calculatedTotalCost:
      row.total_cost != null ? new Decimal(row.total_cost) : null,
    calculatedInputCost:
      row.input_cost != null ? new Decimal(row.input_cost) : null,
    calculatedOutputCost:
      row.output_cost != null ? new Decimal(row.output_cost) : null,
    level: row.level,
    debugCount: BigInt(row.debug_count ?? 0),
    warningCount: BigInt(row.warning_count ?? 0),
    errorCount: BigInt(row.error_count ?? 0),
    defaultCount: BigInt(row.default_count ?? 0),
  };
};

export type FetchTracesTableProps = {
  select: "count" | "rows" | "metrics" | "identifiers" | "largeFieldStats";
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
  tags?: Record<string, string>;
};

// --- MV fast path for the default rows view -------------------------------
// The common trace-list view (no column filters, no search, timestamp ordering)
// is served as a pure single-table aggregate over events_full that Doris
// TRANSPARENTLY REWRITES onto traces_mv (rolled up across start_time_date so a
// trace is one row even if it spans midnight; stale partitions auto-read base).
// Anything needing WHERE/HAVING routing we haven't migrated yet (column / tag /
// metadata / score / metric filters, non-timestamp ordering, search) falls
// through to the base builder — same results, just not MV-accelerated.

const isTimestampFilterColumn = (column: string): boolean =>
  column === "timestamp" || column === "Timestamp";

const isIdFilterColumn = (column: string): boolean =>
  column === "id" ||
  column === "ID" ||
  column === "traceId" ||
  column === "Trace ID";

const isTagsFilterColumn = (column: string): boolean =>
  column === "tags" || column === "Tags";

const isMetadataFilterColumn = (column: string): boolean =>
  column === "metadata" || column === "Metadata";

// Per-key metadata lookup over the OUTER rollup of the inner CTE's native map
// (trace_by_day.metadata). The inner CTE picks the root span's metadata map per
// (trace, day) via any_value(IF(root, metadata, NULL)); any_value(metadata) in the
// outer rolls it up to one per trace (any_value returns a non-null if one exists,
// so root-less day partitions' NULLs are skipped). element_at(map, key) reads the
// per-key value (NULL when the key is absent), matching base map['key'].
const mvMetadataLookup = (key: string): string => {
  const escapedKey = key.replace(/'/g, "''");
  return `element_at(any_value(metadata), '${escapedKey}')`;
};

// Inner per-(trace, day) aggregate, written to match the traces_mv column
// definitions so Doris transparently rewrites it onto traces_mv. The list fast
// paths nest this as a CTE and roll it up to one row per trace in an OUTER GROUP
// BY. Critically, the inner KEEPS start_time_date in its GROUP BY (= the MV
// partition column): when recent partitions are stale, Doris can union-compensate
// (fresh partitions from the MV + the stale partition read from base). A
// single-layer GROUP BY that drops start_time_date cannot be union-compensated
// (the rollup loses the partition column) and falls fully back to base on any
// live ingestion. See docs/trace-list-materialized-view.md.
const MV_INNER_AGG_SELECT = `
      trace_id AS id,
      project_id,
      MIN(start_time) AS ts,
      any_value(IF(parent_span_id = '', tags, NULL)) AS tags,
      any_value(IF(parent_span_id = '', bookmarked, NULL)) AS bookmarked,
      any_value(IF(parent_span_id = '', IF(trace_name <> '', trace_name, name), NULL)) AS name,
      any_value(IF(parent_span_id = '', NULLIF(\`release\`, ''), NULL)) AS \`release\`,
      any_value(IF(parent_span_id = '', NULLIF(version, ''), NULL)) AS version,
      any_value(IF(parent_span_id = '', NULLIF(user_id, ''), NULL)) AS user_id,
      any_value(IF(parent_span_id = '', NULLIF(environment, ''), NULL)) AS environment,
      any_value(IF(parent_span_id = '', NULLIF(session_id, ''), NULL)) AS session_id,
      MAX(\`public\`) AS \`public\`,
      any_value(IF(parent_span_id = '', metadata, NULL)) AS metadata,
      SUM(IF(parent_span_id <> '', 1, 0)) AS observation_count,
      SUM(total_cost) AS total_cost,
      SUM(input_tokens_calculated) AS input_tokens,
      SUM(output_tokens_calculated) AS output_tokens,
      SUM(total_tokens_calculated) AS total_tokens,
      SUM(input_cost_calculated) AS input_cost,
      SUM(output_cost_calculated) AS output_cost,
      MAX(start_time) AS start_time_max,
      MIN(end_time) AS end_time_min,
      MAX(end_time) AS end_time_max,
      SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
      SUM(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
      SUM(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) AS default_count,
      SUM(CASE WHEN level = 'DEBUG' THEN 1 ELSE 0 END) AS debug_count,
      MAX(event_ts) AS event_ts`;

const buildMvInnerCte = (whereParts: string[]): string => `
    trace_by_day AS (
      SELECT ${MV_INNER_AGG_SELECT}
      FROM events_full
      WHERE ${whereParts.join(" AND ")}
      GROUP BY project_id, trace_id, start_time_date
    )`;

// OUTER rollup of the inner CTE to one row per trace. Components combine as
// MIN(MIN)=MIN, MAX(MAX)=MAX, SUM(SUM)=SUM, so latency / level recompute
// correctly from the per-day pre-aggregates.
const OUTER_LATENCY =
  "milliseconds_diff(CASE WHEN MAX(start_time_max) > MAX(end_time_max) THEN MAX(start_time_max) ELSE MAX(end_time_max) END, CASE WHEN MIN(ts) < MIN(end_time_min) THEN MIN(ts) ELSE MIN(end_time_min) END) / 1000";
const OUTER_LEVEL =
  "CASE WHEN SUM(error_count) > 0 THEN 'ERROR' WHEN SUM(warning_count) > 0 THEN 'WARNING' WHEN SUM(default_count) > 0 THEN 'DEFAULT' ELSE 'DEBUG' END";

// UI column → per-trace aggregate expression, used to route scalar / metric
// filters into the fast path's HAVING (a post-aggregation filter on the trace
// row). Expressions match the fast-path SELECTs and the MV column definitions,
// so adding them keeps the transparent rewrite firing.
// OUTER-query rollup expressions over the inner CTE (trace_by_day) aliases — the
// list fast paths apply scalar/metric filters in the OUTER GROUP BY, so these
// reference the inner pre-aggregates and roll them up (any_value(any_value()) for
// root-pick scalars, SUM(SUM)=SUM for metrics), NOT the base events_full columns.
// Keep in sync with MV_INNER_AGG_SELECT / OUTER_* above.
const tracesTableMvHavingColumns: UiColumnMappings = [
  // root-level scalars
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    tableName: "traces",
    select: "any_value(environment)",
    queryPrefix: "",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    tableName: "traces",
    select: "any_value(user_id)",
    queryPrefix: "",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    tableName: "traces",
    select: "any_value(session_id)",
    queryPrefix: "",
  },
  {
    uiTableName: "Version",
    uiTableId: "version",
    tableName: "traces",
    select: "any_value(version)",
    queryPrefix: "",
  },
  {
    uiTableName: "Release",
    uiTableId: "release",
    tableName: "traces",
    select: `any_value(${dq("release")})`,
    queryPrefix: "",
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    tableName: "traces",
    select: "any_value(name)",
    queryPrefix: "",
  },
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    tableName: "traces",
    select: "any_value(name)",
    queryPrefix: "",
  },
  {
    uiTableName: "⭐️",
    uiTableId: "bookmarked",
    tableName: "traces",
    select: "any_value(bookmarked)",
    queryPrefix: "",
  },
  // observation-level metrics (folded into the per-trace aggregate)
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    tableName: "observations",
    select: "SUM(total_cost)",
    queryPrefix: "",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    tableName: "observations",
    select: "SUM(input_cost)",
    queryPrefix: "",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    tableName: "observations",
    select: "SUM(output_cost)",
    queryPrefix: "",
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    tableName: "observations",
    select: "SUM(input_tokens)",
    queryPrefix: "",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    tableName: "observations",
    select: "SUM(output_tokens)",
    queryPrefix: "",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    tableName: "observations",
    select: "SUM(total_tokens)",
    queryPrefix: "",
  },
  {
    uiTableName: "Tokens",
    uiTableId: "tokens",
    tableName: "observations",
    select: "SUM(total_tokens)",
    queryPrefix: "",
  },
  {
    uiTableName: "Latency (s)",
    uiTableId: "latency",
    tableName: "observations",
    select: OUTER_LATENCY,
    queryPrefix: "",
  },
  {
    uiTableName: "Error Level Count",
    uiTableId: "errorCount",
    tableName: "observations",
    select: "SUM(error_count)",
    queryPrefix: "",
  },
  {
    uiTableName: "Warning Level Count",
    uiTableId: "warningCount",
    tableName: "observations",
    select: "SUM(warning_count)",
    queryPrefix: "",
  },
  {
    uiTableName: "Default Level Count",
    uiTableId: "defaultCount",
    tableName: "observations",
    select: "SUM(default_count)",
    queryPrefix: "",
  },
  {
    uiTableName: "Debug Level Count",
    uiTableId: "debugCount",
    tableName: "observations",
    select: "SUM(debug_count)",
    queryPrefix: "",
  },
  {
    uiTableName: "Level",
    uiTableId: "level",
    tableName: "observations",
    select: OUTER_LEVEL,
    queryPrefix: "",
  },
];

const mvHavingColumnTokens = new Set(
  tracesTableMvHavingColumns.flatMap((c) => [c.uiTableName, c.uiTableId]),
);

// A filter is MV-fast-path-routable when it maps to the aggregate: the timestamp
// range (partition prune + MIN(start_time)), a trace_id list, tags (array_contains
// on the rolled-up native array), per-key metadata (element_at over the rolled-up
// parallel arrays), or any scalar/metric column with an aggregate expression
// (HAVING). NOT routable: scores (separate table) or content search — those fall
// back to the base builder.
const canUseMvListFastPath = (params: {
  filter: FilterState;
  orderBy?: OrderByState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
}): boolean => {
  const { filter, orderBy, searchQuery, searchType } = params;
  // ID search (trace_id / user_id / name) maps to the aggregate, so it can run
  // on the fast path. CONTENT search needs the full input/output (FTS index),
  // which the MV doesn't carry (only the 200-char trim) — fall back to base.
  if (searchQuery && (!searchType || searchType.some((t) => t !== "id")))
    return false;
  // The fast paths order by the trace timestamp; non-timestamp ordering falls
  // back to the base builder (which maps order columns to the JOIN aliases).
  if (orderBy && orderBy.column !== "timestamp") return false;
  return filter.every(
    (f) =>
      (f.type === "datetime" && isTimestampFilterColumn(f.column)) ||
      ((f.type === "stringOptions" || f.type === "string") &&
        isIdFilterColumn(f.column)) ||
      (f.type === "arrayOptions" && isTagsFilterColumn(f.column)) ||
      (f.type === "stringObject" && isMetadataFilterColumn(f.column)) ||
      mvHavingColumnTokens.has(f.column),
  );
};

// Shared WHERE/HAVING fragments for the MV list fast paths. WHERE applies to the
// inner CTE (project_id, partition prune on start_time_date, optional trace_id
// list); HAVING applies to the OUTER rollup over trace_by_day (precise MIN(ts)
// bounds, tags array_contains, per-key metadata, and scalar/metric column filters
// via the OUTER aggregate expressions in tracesTableMvHavingColumns).
const buildMvListWhereHaving = (
  filter: FilterState,
  searchQuery?: string,
): {
  whereParts: string[];
  havingParts: string[];
  params: Record<string, unknown>;
} => {
  const whereParts: string[] = ["project_id = {projectId: String}"];
  const havingParts: string[] = [];
  const params: Record<string, unknown> = {};

  const tsFilters = filter.filter((f) => f.type === "datetime");
  const fromFilter = tsFilters.find(
    (f) => f.operator === ">=" || f.operator === ">",
  );
  const toFilter = tsFilters.find(
    (f) => f.operator === "<=" || f.operator === "<",
  );
  if (fromFilter) {
    params.fromTs = convertDateToAnalyticsDateTime(fromFilter.value as Date);
    whereParts.push("start_time_date >= DATE({fromTs: DateTime})");
    havingParts.push(`MIN(ts) ${fromFilter.operator} {fromTs: DateTime}`);
  }
  if (toFilter) {
    params.toTs = convertDateToAnalyticsDateTime(toFilter.value as Date);
    havingParts.push(`MIN(ts) ${toFilter.operator} {toTs: DateTime}`);
  }

  const idFilter = filter.find(
    (f) =>
      isIdFilterColumn(f.column) &&
      (f.type === "stringOptions" || f.type === "string"),
  );
  if (idFilter) {
    const ids =
      idFilter.type === "stringOptions"
        ? (idFilter.value as string[])
        : [idFilter.value as string];
    params.traceIds = ids;
    whereParts.push("trace_id IN ({traceIds: Array(String)})");
  }

  // tags: the inner CTE carries the root span's native ARRAY per (trace, day);
  // any_value() rolls it up to one per trace in the outer. Match membership with
  // array_contains — exact element matching (no false substring hits).
  const tagsExpr = "any_value(tags)";
  for (const f of filter) {
    if (f.type !== "arrayOptions" || !isTagsFilterColumn(f.column)) continue;
    const contains = (f.value as string[]).map((v) => {
      const escaped = v.replace(/'/g, "''");
      return `array_contains(${tagsExpr}, '${escaped}')`;
    });
    if (contains.length === 0) continue;
    if (f.operator === "all of")
      havingParts.push(`(${contains.join(" AND ")})`);
    else if (f.operator === "none of")
      havingParts.push(`NOT (${contains.join(" OR ")})`);
    else havingParts.push(`(${contains.join(" OR ")})`); // "any of"
  }

  // metadata: per-key filter over the rolled-up parallel arrays (mvMetadataLookup),
  // mirroring the base StringObjectFilter operators on map['key']. Transparently
  // rewrites onto traces_mv (metadata_names/metadata_values columns).
  for (const f of filter) {
    if (f.type !== "stringObject" || !isMetadataFilterColumn(f.column))
      continue;
    const lookup = mvMetadataLookup(f.key);
    const escapedValue = f.value.replace(/'/g, "''");
    switch (f.operator) {
      case "=":
        havingParts.push(`${lookup} = '${escapedValue}'`);
        break;
      case "contains":
        havingParts.push(`INSTR(${lookup}, '${escapedValue}') > 0`);
        break;
      case "does not contain":
        havingParts.push(`INSTR(${lookup}, '${escapedValue}') = 0`);
        break;
      case "starts with":
        havingParts.push(`STARTS_WITH(${lookup}, '${escapedValue}')`);
        break;
      case "ends with":
        havingParts.push(`ENDS_WITH(${lookup}, '${escapedValue}')`);
        break;
    }
  }

  // scalar / metric column filters → HAVING via the aggregate expressions. The
  // Doris filter classes emit inline-escaped values (no bind params), so the
  // produced SQL is self-contained.
  const havingColumnFilters = filter.filter((f) =>
    mvHavingColumnTokens.has(f.column),
  );
  if (havingColumnFilters.length > 0) {
    const res = new FilterList(
      createDorisFilterFromFilterState(
        havingColumnFilters,
        tracesTableMvHavingColumns,
      ),
    ).apply();
    if (res.query) havingParts.push(res.query);
  }

  // ID search (searchType ["id"]): trace_id (grouping key, exposed as id) + root
  // user_id / name via the outer rollup, OR-ed, in HAVING. Content search is gated
  // out upstream (needs full I/O). searchQuery is interpolated as a parameter.
  if (searchQuery) {
    params.searchLike = `%${searchQuery}%`;
    havingParts.push(
      `(id LIKE {searchLike: String}` +
        ` OR any_value(user_id) LIKE {searchLike: String}` +
        ` OR any_value(name) LIKE {searchLike: String})`,
    );
  }

  return { whereParts, havingParts, params };
};

const runMvRowsFastPath = async (params: {
  projectId: string;
  filter: FilterState;
  orderByDesc: boolean;
  searchQuery?: string;
  limit?: number;
  page?: number;
  tags?: Record<string, string>;
}): Promise<TracesTableReturnType[]> => {
  const { projectId, filter, orderByDesc, limit, page, tags, searchQuery } =
    params;

  const {
    whereParts,
    havingParts,
    params: filterParams,
  } = buildMvListWhereHaving(filter, searchQuery);
  const queryParams: Record<string, unknown> = { projectId, ...filterParams };

  const order = orderByDesc ? "DESC" : "ASC";
  const pagination =
    limit !== undefined && page !== undefined
      ? "LIMIT {limit: Int32} OFFSET {offset: Int32}"
      : "";
  if (pagination) {
    queryParams.limit = limit;
    queryParams.offset = (limit ?? 0) * (page ?? 0);
  }

  // Nested two-level aggregate: inner = per (trace, day) at MV granularity (so it
  // transparently rewrites onto traces_mv + union-compensates stale partitions);
  // outer rolls up to one row per trace. See buildMvInnerCte.
  const query = `
    WITH ${buildMvInnerCte(whereParts)}
    SELECT
      id,
      project_id,
      MIN(ts) AS ${dq("timestamp")},
      any_value(tags) AS tags,
      any_value(bookmarked) AS bookmarked,
      any_value(name) AS name,
      any_value(${dq("release")}) AS ${dq("release")},
      any_value(version) AS version,
      any_value(user_id) AS user_id,
      any_value(environment) AS environment,
      any_value(session_id) AS session_id,
      MAX(${dq("public")}) AS ${dq("public")}
    FROM trace_by_day
    GROUP BY project_id, id
    ${havingParts.length > 0 ? `HAVING ${havingParts.join(" AND ")}` : ""}
    ORDER BY DATE(MIN(ts)) ${order}, MIN(ts) ${order}, MAX(event_ts) DESC
    ${pagination}
  `;

  return await queryDoris<TracesTableReturnType>({
    query,
    params: queryParams,
    tags: {
      ...(tags ?? {}),
      feature: "tracing",
      type: "traces-table",
      projectId,
    },
  });
};

// Count of traces = count of (project_id, trace_id) groups. Nested two-level
// aggregate (inner per (trace, day) rewrites onto traces_mv + union-compensates
// stale partitions; outer rolls up to one row per trace), wrapped in count(*).
// Same eligibility as the rows fast path (canUseMvListFastPath).
const runMvCountFastPath = async (params: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  tags?: Record<string, string>;
}): Promise<Array<{ count: string }>> => {
  const { projectId, filter, tags, searchQuery } = params;

  const {
    whereParts,
    havingParts,
    params: filterParams,
  } = buildMvListWhereHaving(filter, searchQuery);
  const queryParams: Record<string, unknown> = { projectId, ...filterParams };

  const query = `
    WITH ${buildMvInnerCte(whereParts)}
    SELECT count(*) AS count FROM (
      SELECT id
      FROM trace_by_day
      GROUP BY project_id, id
      ${havingParts.length > 0 ? `HAVING ${havingParts.join(" AND ")}` : ""}
    ) t
  `;

  return await queryDoris<{ count: string }>({
    query,
    params: queryParams,
    tags: {
      ...(tags ?? {}),
      feature: "tracing",
      type: "traces-table-count",
      projectId,
    },
  });
};

// Metrics fast path: a single-table aggregate over events_full (no t self-JOIN,
// no observations_stats CTE, no scores JOIN) that transparently rewrites onto
// traces_mv. scores are NOT part of this query — the tRPC layer fetches them
// separately (getScoresForTraces) and merges by trace_id, and the metrics SELECT
// of scores_avg was already discarded by convertToUITableMetrics. Only eligible
// when not ordering/filtering by scores or observation columns (canUseMvListFastPath).
const runMvMetricsFastPath = async (params: {
  projectId: string;
  filter: FilterState;
  orderByDesc: boolean;
  searchQuery?: string;
  limit?: number;
  page?: number;
  tags?: Record<string, string>;
}): Promise<TracesTableMetricsDorisReturnType[]> => {
  const { projectId, filter, orderByDesc, limit, page, tags, searchQuery } =
    params;

  const {
    whereParts,
    havingParts,
    params: filterParams,
  } = buildMvListWhereHaving(filter, searchQuery);
  const queryParams: Record<string, unknown> = { projectId, ...filterParams };

  const order = orderByDesc ? "DESC" : "ASC";
  const pagination =
    limit !== undefined && page !== undefined
      ? "LIMIT {limit: Int32} OFFSET {offset: Int32}"
      : "";
  if (pagination) {
    queryParams.limit = limit;
    queryParams.offset = (limit ?? 0) * (page ?? 0);
  }

  // Nested two-level aggregate (see buildMvInnerCte): inner per (trace, day) at MV
  // granularity → rewrites onto traces_mv + union-compensates stale partitions;
  // outer rolls up to one row per trace. latency from the rolled min/max
  // components; level + counts from SUM of the per-day counts. scores are NOT
  // here — the tRPC layer fetches them separately (getScoresForTraces).
  const query = `
    WITH ${buildMvInnerCte(whereParts)}
    SELECT
      id,
      project_id,
      MIN(ts) AS ${dq("timestamp")},
      ${OUTER_LATENCY} AS latency,
      SUM(total_cost) AS total_cost,
      SUM(input_tokens) AS input_tokens,
      SUM(output_tokens) AS output_tokens,
      SUM(total_tokens) AS total_tokens,
      SUM(input_cost) AS input_cost,
      SUM(output_cost) AS output_cost,
      ${OUTER_LEVEL} AS level,
      SUM(error_count) AS error_count,
      SUM(warning_count) AS warning_count,
      SUM(default_count) AS default_count,
      SUM(debug_count) AS debug_count,
      SUM(observation_count) AS observation_count,
      MAX(${dq("public")}) AS ${dq("public")}
    FROM trace_by_day
    GROUP BY project_id, id
    ${havingParts.length > 0 ? `HAVING ${havingParts.join(" AND ")}` : ""}
    ORDER BY DATE(MIN(ts)) ${order}, MIN(ts) ${order}, MAX(event_ts) DESC
    ${pagination}
  `;

  return await queryDoris<TracesTableMetricsDorisReturnType>({
    query,
    params: queryParams,
    tags: {
      ...(tags ?? {}),
      feature: "tracing",
      type: "traces-table",
      projectId,
    },
  });
};

// Define return type mapping for better type safety
type SelectReturnTypeMap = {
  count: { count: string };
  metrics: TracesTableMetricsDorisReturnType;
  rows: TracesTableReturnType;
  identifiers: { id: string; projectId: string; timestamp: string };
  largeFieldStats: {
    avg_input_bytes: string | number | null;
    avg_output_bytes: string | number | null;
    avg_metadata_bytes: string | number | null;
  };
};

// Function overloads for type-safe select-specific returns
async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "count" },
): Promise<Array<SelectReturnTypeMap["count"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "metrics" },
): Promise<Array<SelectReturnTypeMap["metrics"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "rows" },
): Promise<Array<SelectReturnTypeMap["rows"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "identifiers" },
): Promise<Array<SelectReturnTypeMap["identifiers"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "largeFieldStats" },
): Promise<Array<SelectReturnTypeMap["largeFieldStats"]>>;

// Implementation with union type for internal use
async function getTracesTableGeneric(
  props: FetchTracesTableProps,
): Promise<Array<SelectReturnTypeMap[keyof SelectReturnTypeMap]>>;

async function getTracesTableGeneric(props: FetchTracesTableProps) {
  const {
    select,
    projectId,
    filter,
    orderBy,
    limit,
    page,
    searchQuery,
    searchType,
  } = props;

  // MV fast paths (see helpers above): rows / metrics / count are served as a
  // single-table aggregate over events_full that transparently rewrites onto
  // traces_mv. Eligible only when filters / search / ordering are MV-routable
  // (timestamp range + trace_id list); otherwise fall through to the base builder
  // below — same results, just not MV-accelerated.
  const mvEligible = canUseMvListFastPath({
    filter,
    orderBy,
    searchQuery,
    searchType,
  });
  const orderByDesc =
    orderBy?.column === "timestamp" ? orderBy.order !== "ASC" : true;

  if (select === "rows" && mvEligible) {
    return (await runMvRowsFastPath({
      projectId,
      filter,
      orderByDesc,
      limit,
      page,
      tags: props.tags,
      searchQuery,
    })) as Array<SelectReturnTypeMap[keyof SelectReturnTypeMap]>;
  }

  if (select === "metrics" && mvEligible) {
    return (await runMvMetricsFastPath({
      projectId,
      filter,
      orderByDesc,
      limit,
      page,
      tags: props.tags,
      searchQuery,
    })) as Array<SelectReturnTypeMap[keyof SelectReturnTypeMap]>;
  }

  if (select === "count" && mvEligible) {
    return (await runMvCountFastPath({
      projectId,
      filter,
      tags: props.tags,
      searchQuery,
    })) as Array<SelectReturnTypeMap[keyof SelectReturnTypeMap]>;
  }

  // Shared SELECT statement generation logic (used by Doris path)
  let sqlSelect: string;
  switch (select) {
    case "count":
      sqlSelect = "count(*) as count";
      break;
    case "metrics":
      sqlSelect = `
        t.trace_id as id,
        t.project_id as project_id,
        t.start_time as ${dq("timestamp")},
        os.latency_milliseconds / 1000 as latency,
        os.total_cost as total_cost,
        os.input_tokens as input_tokens,
        os.output_tokens as output_tokens,
        os.total_tokens as total_tokens,
        os.input_cost as input_cost,
        os.output_cost as output_cost,
        os.aggregated_level as level,
        os.error_count as error_count,
        os.warning_count as warning_count,
        os.default_count as default_count,
        os.debug_count as debug_count,
        os.observation_count as observation_count,
        s.scores_avg as scores_avg,
        s.score_categories as score_categories,
        t.${dq("public")} as ${dq("public")}`;
      break;
    case "rows":
      // `t` is `events_full` filtered to the root span (parent_span_id =
      // ''). `t.name` is the *root span's own* name, e.g.
      // "advanced-generation-…"; `t.trace_name` is the trace-level name
      // denormalised onto every row by createEventRecord. The trace
      // list UI must show the latter — fall back to `t.name` only when
      // the SDK didn't set a trace name (legacy clients).
      sqlSelect = `
        t.trace_id as id,
        t.project_id as project_id,
        t.start_time as ${dq("timestamp")},
        t.tags as tags,
        t.bookmarked as bookmarked,
        IF(t.trace_name <> '', t.trace_name, t.name) as name,
        t.${dq("release")} as ${dq("release")},
        t.version as version,
        t.user_id as user_id,
        t.environment as environment,
        t.session_id as session_id,
        t.${dq("public")} as ${dq("public")}`;
      break;
    case "identifiers":
      sqlSelect = `
        t.trace_id as id,
        t.project_id as projectId,
        t.start_time as ${dq("timestamp")}`;
      break;
    case "largeFieldStats":
      sqlSelect = `
        AVG(COALESCE(CHAR_LENGTH(CAST(t.input AS STRING)), 0)) as avg_input_bytes,
        AVG(COALESCE(CHAR_LENGTH(CAST(t.output AS STRING)), 0)) as avg_output_bytes,
        AVG(
          COALESCE(CHAR_LENGTH(CAST(t.metadata_names AS STRING)), 0) +
          COALESCE(CHAR_LENGTH(CAST(t.metadata_values AS STRING)), 0)
        ) as avg_metadata_bytes`;
      break;
    default:
      throw new Error(`Unknown select type: ${select}`);
  }

  const { tracesFilter, scoresFilter, observationsFilter } =
    getDorisProjectIdDefaultFilter(projectId, { tracesPrefix: "t" });

  tracesFilter.push(
    ...createDorisFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitionsForDoris,
    ),
  );

  const traceIdFilter = tracesFilter.find(
    (f) => f.table === "traces" && f.field === "id",
  ) as DorisStringFilter | DorisStringOptionsFilter | undefined;

  traceIdFilter
    ? scoresFilter.push(
        new DorisStringOptionsFilter({
          table: "scores",
          field: "trace_id",
          operator: "any of",
          values:
            traceIdFilter instanceof DorisStringFilter
              ? [traceIdFilter.value]
              : traceIdFilter.values,
        }),
      )
    : null;
  traceIdFilter
    ? observationsFilter.push(
        new DorisStringOptionsFilter({
          table: "observations",
          field: "trace_id",
          operator: "any of",
          values:
            traceIdFilter instanceof DorisStringFilter
              ? [traceIdFilter.value]
              : traceIdFilter.values,
        }),
      )
    : null;

  const timeStampFilter = tracesFilter.find(
    (f) =>
      f.field === "start_time" && (f.operator === ">=" || f.operator === ">"),
  ) as DorisDateTimeFilter | undefined;

  const requiresScoresJoin =
    tracesFilter.find((f) => f.table === "scores") !== undefined ||
    tracesTableUiColumnDefinitionsForDoris.find(
      (c) =>
        c.uiTableName === orderBy?.column || c.uiTableId === orderBy?.column,
    )?.tableName === "scores";

  const requiresObservationsJoin =
    tracesFilter.find((f) => f.table === "observations") !== undefined ||
    tracesTableUiColumnDefinitionsForDoris.find(
      (c) =>
        c.uiTableName === orderBy?.column || c.uiTableId === orderBy?.column,
    )?.tableName === "observations";

  const tracesFilterRes = tracesFilter.apply();
  const scoresFilterRes = scoresFilter.apply();
  const observationFilterRes = observationsFilter.apply();

  // Check if any filter references observation-level columns (os.usage_details etc.)
  // to add Doris optimizer hint when needed.
  const hasObsLevelFilter =
    tracesFilter.find((f) => f.table === "observations") !== undefined;

  const search = dorisSearchCondition(searchQuery, searchType, {
    type: "traces",
  });

  const defaultOrder = orderBy?.order && orderBy?.column === "timestamp";
  const orderByCols = [
    ...tracesTableUiColumnDefinitionsForDoris,
    {
      select: "DATE(t.start_time)",
      uiTableName: "timestamp_to_date",
      uiTableId: "timestamp_to_date",
      tableName: "traces",
    },
    {
      select: "t.event_ts",
      uiTableName: "event_ts",
      uiTableId: "event_ts",
      tableName: "traces",
    },
  ];
  const dorisOrderBy = orderByToDorisSQL(
    [
      defaultOrder
        ? [
            {
              column: "timestamp_to_date",
              order: orderBy.order,
            },
            { column: "timestamp", order: orderBy.order },
            { column: "event_ts", order: "DESC" as "DESC" },
          ]
        : null,
      orderBy ?? null,
    ].flat(),
    orderByCols,
  );

  // Doris version of the complex query
  const observations_stats_cte =
    select === "metrics" || requiresObservationsJoin
      ? `
      observations_stats AS (
        -- Trace-level rollup of its observations. Written in the MV-aligned
        -- aggregate shape (single-table, no parent_span_id predicate, level via
        -- SUM(CASE) counts, latency from raw min/max components, partition prune
        -- on start_time_date) so Doris TRANSPARENTLY REWRITES this CTE onto
        -- traces_mv (rolled up across start_time_date) — see migration 0038 and
        -- docs/trace-list-materialized-view.md. Stale/unrefreshed partitions
        -- auto-read base, so results stay correct. Aggregates are all-spans:
        -- the synthetic root span carries no cost/tokens, so those totals are
        -- unchanged; latency becomes the full-trace span and level counts include
        -- the root span. observation_count stays children-only via SUM(IF(...)).
        -- An observation-level filter (rare) adds a non-grouping predicate that
        -- the rewrite can't compensate, so such queries fall back to base — still
        -- correct, just not MV-accelerated.
        SELECT
          trace_id,
          project_id,
          SUM(IF(parent_span_id <> '', 1, 0)) AS observation_count,
          SUM(total_cost) AS total_cost,
          SUM(input_tokens_calculated) AS input_tokens,
          SUM(output_tokens_calculated) AS output_tokens,
          SUM(total_tokens_calculated) AS total_tokens,
          SUM(input_cost_calculated) AS input_cost,
          SUM(output_cost_calculated) AS output_cost,
          -- Calculate millisecond diff in Doris - use CASE WHEN instead of least/greatest
          milliseconds_diff(
          CASE WHEN max(start_time) > max(end_time) THEN max(start_time) ELSE max(end_time) END,
          CASE WHEN min(start_time) < min(end_time) THEN min(start_time) ELSE min(end_time) END
          ) as latency_milliseconds,
          -- Conditional counts
          sum(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as error_count,
          sum(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) as warning_count,
          sum(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) as default_count,
          sum(CASE WHEN level = 'DEBUG' THEN 1 ELSE 0 END) as debug_count,
          -- Level priority derived from the counts above (collect_list does not
          -- transparently rewrite; CASE over SUM(CASE) does).
          CASE
            WHEN sum(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) > 0 THEN 'ERROR'
            WHEN sum(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) > 0 THEN 'WARNING'
            WHEN sum(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) > 0 THEN 'DEFAULT'
            ELSE 'DEBUG'
          END AS aggregated_level
        FROM events_full o
        WHERE project_id = {projectId: String}
        ${timeStampFilter ? `AND start_time_date >= DATE(DATE_SUB({traceTimestamp: DateTime}, INTERVAL 2 DAY))` : ""}
        ${observationFilterRes ? `AND ${observationFilterRes.query}` : ""}
        GROUP BY trace_id, project_id
      )`
      : "";

  const scores_avg_cte =
    select === "metrics" || requiresScoresJoin
      ? `
      scores_avg AS (
        SELECT
          project_id,
          trace_id,
          -- Numeric scores: Array<Struct(name, avg_value)> matching CK's
          -- Array<Tuple> so NumberObjectFilter (size(array_filter(...)) > 0)
          -- OR-matches over all evaluator rows for the same score name.
          -- collect_list skips NULLs automatically (for struct rows).
          collect_list(
            CASE WHEN data_type IN ('NUMERIC', 'BOOLEAN') THEN
              struct(name, avg_value)
            END
          ) AS scores_avg,
          -- Categorical scores: Array<"name:value"> for CategoryOptionsFilter
          -- which uses arrays_overlap(column, array(...)).
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
            ${timeStampFilter ? `AND s.timestamp >= DATE_SUB({traceTimestamp: DateTime}, INTERVAL 1 HOUR)` : ""}
            ${scoresFilterRes ? `AND ${scoresFilterRes.query}` : ""}
          GROUP BY 
            project_id,
            trace_id,
            name,
            data_type,
            string_value
        ) tmp
        GROUP BY project_id, trace_id
      )`
      : "";

  const withClause = [observations_stats_cte, scores_avg_cte]
    .filter(Boolean)
    .join(",\n");

  // Doris Nereids optimizer crashes with "LogicalFilter cannot be cast to
  // LogicalJoin" when complex expressions referencing LEFT JOIN columns
  // (os.usage_details with array_filter/map_keys) appear in the WHERE clause.
  // Disable PUSH_FILTER_INSIDE_JOIN rule via hint when obs-level filters exist.
  const dorisHint = hasObsLevelFilter
    ? `/*+ SET_VAR(disable_nereids_rules='PUSH_FILTER_INSIDE_JOIN') */`
    : "";

  const query = `
      ${withClause ? `WITH ${withClause}` : ""}
      SELECT ${dorisHint} ${sqlSelect}
      FROM events_full t
      ${select === "metrics" || requiresObservationsJoin ? `LEFT JOIN observations_stats os on os.project_id = t.project_id and os.trace_id = t.trace_id` : ""}
      ${select === "metrics" || requiresScoresJoin ? `LEFT JOIN scores_avg s on s.project_id = t.project_id and s.trace_id = t.trace_id` : ""}
      WHERE t.project_id = {projectId: String}
      AND t.parent_span_id = ''
      ${timeStampFilter ? `AND t.start_time_date >= DATE(DATE_SUB({traceTimestamp: DateTime}, INTERVAL 2 DAY))` : ""}
      ${tracesFilterRes ? `AND ${tracesFilterRes.query}` : ""}
      ${search.query}
      ${dorisOrderBy}
      ${limit !== undefined && page !== undefined ? `LIMIT {limit: Int32} OFFSET {offset: Int32}` : ""}
    `;

  // Define Doris-specific return type for metrics
  type DorisMetricsReturnType = Omit<
    TracesTableMetricsDorisReturnType,
    "scores_avg" | "score_categories"
  > & {
    // scores_avg: Array of struct objects ({col1, col2} from Doris struct), or JSON string
    scores_avg: string | Array<Record<string, unknown>>;
    score_categories: string | Array<string>; // Array<"name:value"> or JSON string
  };

  const res = await queryDoris<SelectReturnTypeMap[keyof SelectReturnTypeMap]>({
    query: query,
    params: {
      limit: limit,
      offset: limit && page ? limit * page : 0,
      ...(timeStampFilter
        ? {
            traceTimestamp: convertDateToAnalyticsDateTime(
              timeStampFilter.value,
            ),
          }
        : {}),
      projectId: projectId,
      ...tracesFilterRes.params,
      ...observationFilterRes.params,
      ...scoresFilterRes.params,
      ...search.params,
    },
    tags: {
      ...(props.tags ?? {}),
      feature: "tracing",
      type: "traces-table",
      projectId,
    },
  });

  // Post-process Doris results into the object shape downstream consumers expect.
  if (select === "metrics") {
    const processedRes = (res as unknown as DorisMetricsReturnType[]).map(
      (row) => {
        // Normalize the Array<Struct(name, avg_value)> that Doris returns.
        // The mysql driver serializes struct elements as
        // {"col1": name, "col2": avg_value}; we rename to {name, avg_value}.
        const parsedScoresAvg: Array<{ name: string; avg_value: number }> = [];

        let scoresAvgRaw: unknown[] = [];
        if (typeof row.scores_avg === "string") {
          try {
            scoresAvgRaw = JSON.parse(row.scores_avg);
          } catch {
            scoresAvgRaw = [];
          }
        } else if (Array.isArray(row.scores_avg)) {
          scoresAvgRaw = row.scores_avg;
        }

        scoresAvgRaw.forEach((entry) => {
          if (entry && typeof entry === "object") {
            const e = entry as Record<string, unknown>;
            // Doris struct fields: col1=name, col2=avg_value (positional)
            const name = e.col1 ?? e.name;
            const avg_value = e.col2 ?? e.avg_value;
            if (typeof name === "string" && name.length > 0) {
              parsedScoresAvg.push({
                name,
                avg_value: Number(avg_value) || 0,
              });
            }
          }
        });

        // Handle score_categories - could be string or array
        let scoreCategoriesArray: string[] = [];
        if (typeof row.score_categories === "string") {
          try {
            scoreCategoriesArray = JSON.parse(row.score_categories);
          } catch {
            scoreCategoriesArray = [];
          }
        } else if (Array.isArray(row.score_categories)) {
          scoreCategoriesArray = row.score_categories;
        }

        // Return row with parsed array/object values
        return {
          ...row,
          scores_avg: parsedScoresAvg,
          score_categories: scoreCategoriesArray,
        } as TracesTableMetricsDorisReturnType;
      },
    );

    return processedRes as Array<
      SelectReturnTypeMap[keyof SelectReturnTypeMap]
    >;
  }

  // Post-process Doris results for rows to ensure tags field is properly formatted as array
  if (select === "rows") {
    const processedRes = (res as unknown as TracesTableReturnType[]).map(
      (row) => {
        // Ensure tags is always an array
        let processedTags: string[] = [];

        if (Array.isArray(row.tags)) {
          processedTags = row.tags;
        } else if (typeof row.tags === "string") {
          try {
            // Try to parse as JSON array
            const parsed = JSON.parse(row.tags);
            processedTags = Array.isArray(parsed) ? parsed : [row.tags];
          } catch {
            // If parsing fails, treat as single tag
            processedTags = row.tags ? [row.tags] : [];
          }
        } else if (row.tags == null) {
          processedTags = [];
        } else {
          // Convert any other type to empty array
          processedTags = [];
        }

        return {
          ...row,
          tags: processedTags,
        } as TracesTableReturnType;
      },
    );

    return processedRes as Array<
      SelectReturnTypeMap[keyof SelectReturnTypeMap]
    >;
  }

  return res;
}

export const getTracesTableCount = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}) => {
  const countRows = await getTracesTableGeneric({
    select: "count",
    tags: { kind: "count" },
    ...props,
  });

  const converted = countRows.map((row) => ({
    count: Number(row.count),
  }));

  return converted.length > 0 ? converted[0].count : 0;
};

export const getTracesTableMetrics = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}): Promise<Array<Omit<TracesMetricsUiReturnType, "scores">>> => {
  const countRows = await getTracesTableGeneric({
    select: "metrics",
    tags: { kind: "analytic" },
    ...props,
  });

  return countRows.map(convertToUITableMetrics);
};

export const getTracesTable = async (p: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}) => {
  const { projectId, filter, searchQuery, searchType, orderBy, limit, page } =
    p;
  const rows = await getTracesTableGeneric({
    select: "rows",
    tags: { kind: "list" },
    projectId,
    filter,
    searchQuery,
    searchType,
    orderBy,
    limit,
    page,
  });

  return rows.map(convertToUiTableRows);
};

export const getTraceIdentifiers = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}) => {
  const { projectId, filter, searchQuery, searchType, orderBy, limit, page } =
    props;
  const identifiers = await getTracesTableGeneric({
    select: "identifiers",
    tags: { kind: "list" },
    projectId,
    filter,
    searchQuery,
    searchType,
    orderBy,
    limit,
    page,
  });

  return identifiers.map((row) => ({
    id: row.id,
    projectId: row.projectId,
    // Doris (via mysql2) returns timestamps as Date objects, but some callers
    // (e.g. legacy paths or JSON-roundtripped rows) supply ISO strings; accept
    // both. TypeScript can't narrow Date | string at runtime so we type-assert.
    timestamp:
      (row.timestamp as unknown) instanceof Date
        ? (row.timestamp as unknown as Date)
        : parseDorisUTCDateTimeFormat(row.timestamp),
  }));
};

export const getTracesTableLargeFieldStats = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
}) => {
  const [row] = await getTracesTableGeneric({
    select: "largeFieldStats",
    tags: { kind: "analytic" },
    ...props,
  });

  return {
    avgInputBytes: Number(row?.avg_input_bytes ?? 0),
    avgOutputBytes: Number(row?.avg_output_bytes ?? 0),
    avgMetadataBytes: Number(row?.avg_metadata_bytes ?? 0),
  };
};
