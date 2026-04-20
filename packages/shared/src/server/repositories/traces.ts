import { convertDateToAnalyticsDateTime, dq } from "./analytics";
import {
  createDorisFilterFromFilterState,
  getDorisProjectIdDefaultFilter,
} from "../queries/doris-sql/factory";
import {
  StringFilter as DorisStringFilter,
  DateTimeFilter as DorisDateTimeFilter,
} from "../queries/doris-sql/doris-filter";
import { FilterState } from "../../types";
import { FilterList } from "../queries";
import { tracesTableUiColumnDefinitionsForDoris } from "../tableMappings/mapTracesTable";
import { UiColumnMappings, ColumnDefinition } from "../../tableDefinitions";
import {
  OBSERVATIONS_TO_TRACE_INTERVAL,
  TRACE_TO_OBSERVATIONS_INTERVAL,
} from "./constants";
import { env } from "../../env";
import { recordDistribution } from "../instrumentation";
import { DEFAULT_RENDERING_PROPS, RenderingProps } from "../utils/rendering";
import { parseDorisStringArray } from "../utils/dorisArrays";
import { logger } from "../logger";
import {
  queryDoris,
  upsertDoris,
  commandDoris,
  queryDorisStream,
} from "./doris";
import {
  dorisSearchCondition,
  DorisSearchContext,
} from "../queries/doris-sql/search";
import { TraceRecordReadType } from "./definitions";
import { convertDorisToDomain } from "./traces_converters";

/**
 * Checks if trace exists in Doris.
 * Additionally, give back the timestamp of the trace as metadata.
 *
 * @param {string} projectId - Project ID for the trace
 * @param {string} traceId - ID of the trace to check
 * @param {Date} timestamp - Timestamp for time-based filtering, uses event payload or job timestamp
 * @param {FilterState} filter - Filter for the trace
 * @param {Date} maxTimeStamp - Upper bound on timestamp
 * @param {Date} exactTimestamp - Exact match for the trace
 * @returns {Promise<boolean>} - True if trace exists
 *
 * Notes:
 * • Filters within ±2 day window
 * • Used for validating trace references before eval job creation
 */
export const checkTraceExistsAndGetTimestamp = async ({
  projectId,
  traceId,
  timestamp,
  filter,
  maxTimeStamp,
  exactTimestamp,
}: {
  projectId: string;
  traceId: string;
  timestamp: Date;
  filter: FilterState;
  maxTimeStamp: Date | undefined;
  exactTimestamp?: Date;
}): Promise<{ exists: boolean; timestamp?: Date }> => {
  const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  const timeStampFilter = tracesFilter.find(
    (f) =>
      f.field === "timestamp" && (f.operator === ">=" || f.operator === ">"),
  ) as DorisDateTimeFilter | undefined;

  tracesFilter.push(
    ...createDorisFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitionsForDoris,
    ),
    new DorisStringFilter({
      table: "t",
      field: "id",
      operator: "=",
      value: traceId,
      tablePrefix: "t",
    }),
  );

  const observationFilter = tracesFilter.find(
    (f) => f.table === "observations",
  );
  const tracesFilterRes = tracesFilter.apply();
  const observationFilterRes = observationFilter?.apply();

  // Helper function to convert Date to Doris DateTime string format
  const toDorisDateTime = (date: Date, offsetSeconds: number = 0) => {
    const adjustedDate = new Date(date.getTime() + offsetSeconds * 1000);
    return adjustedDate.toISOString().replace("T", " ").replace("Z", "");
  };

  // Doris version of the complex query
  const query = `
    WITH observations_agg AS (
        SELECT
            CASE
              WHEN COUNT(CASE WHEN level = 'ERROR' THEN 1 END) > 0 THEN 'ERROR'
              WHEN COUNT(CASE WHEN level = 'WARNING' THEN 1 END) > 0 THEN 'WARNING'
              WHEN COUNT(CASE WHEN level = 'DEFAULT' THEN 1 END) > 0 THEN 'DEFAULT'
              ELSE 'DEBUG'
            END AS aggregated_level,
            COUNT(CASE WHEN level = 'ERROR' THEN 1 END) as error_count,
            COUNT(CASE WHEN level = 'WARNING' THEN 1 END) as warning_count,
            COUNT(CASE WHEN level = 'DEFAULT' THEN 1 END) as default_count,
            COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) as debug_count,
            trace_id,
            project_id
        FROM observations o
        WHERE o.project_id = '${projectId}'
        ${timeStampFilter ? `AND o.start_time >= '${toDorisDateTime(timestamp, -172800)}'` : ""}
        AND o.start_time >= '${toDorisDateTime(timestamp, -172800)}'
        GROUP BY trace_id, project_id
    )
    SELECT
      t.id as id,
      t.project_id as project_id
    FROM traces t
    ${observationFilterRes ? `INNER JOIN observations_agg o ON t.id = o.trace_id AND t.project_id = o.project_id` : ""}
    WHERE ${tracesFilterRes.query}
    AND t.project_id = '${projectId}'
    AND timestamp >= '${toDorisDateTime(timestamp, -172800)}'
    ${maxTimeStamp ? `AND timestamp <= '${toDorisDateTime(maxTimeStamp)}'` : ""}
    ${!maxTimeStamp ? `AND timestamp <= '${toDorisDateTime(timestamp, 172800)}'` : ""}
    ${exactTimestamp ? `AND timestamp = '${toDorisDateTime(exactTimestamp)}'` : ""}
    GROUP BY t.id, t.project_id
  `;

  const rows = await queryDoris<{ id: string; project_id: string }>({
    query,
    params: {},
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "exists",
      projectId,
    },
  });
  return { exists: rows.length > 0 };
};

/**
 * Accepts a trace in a Clickhouse-ready format.
 * id, project_id, and timestamp must always be provided.
 */
export const upsertTrace = async (trace: Partial<TraceRecordReadType>) => {
  if (!["id", "project_id", "timestamp"].every((key) => key in trace)) {
    throw new Error("Identifier fields must be provided to upsert Trace.");
  }

  await upsertDoris({
    table: "traces",
    records: [trace as TraceRecordReadType],
    eventBodyMapper: convertDorisToDomain,
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "upsert",
      projectId: trace.project_id ?? "",
    },
  });
};

export const getTracesByIds = async (
  traceIds: string[],
  projectId: string,
  timestamp?: Date,
) => {
  const query = `
    SELECT
      id,
      timestamp,
      name,
      user_id,
      metadata,
      environment,
      ${dq("release")},
      version,
      project_id,
      ${dq("public")},
      bookmarked,
      tags,
      input,
      output,
      session_id,
      created_at,
      updated_at,
      event_ts,
      is_deleted
    FROM traces
    WHERE id IN ({traceIds: Array(String)})
    AND project_id = {projectId: String}
    ${timestamp ? `AND timestamp >= {timestamp: DateTime}` : ""}
    ORDER BY event_ts DESC`;

  const records = await queryDoris<TraceRecordReadType>({
    query,
    params: {
      traceIds,
      projectId,
      timestamp: timestamp ? convertDateToAnalyticsDateTime(timestamp) : null,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "byId",
      projectId,
    },
  });

  return records.map((r) => convertDorisToDomain(r));
};

export const getTracesBySessionId = async (
  projectId: string,
  sessionIds: string[],
  timestamp?: Date,
) => {
  // Doris implementation using window function to achieve LIMIT 1 BY semantics
  const query = `
    SELECT
      id,
      timestamp,
      name,
      user_id,
      metadata,
      environment,
      ${dq("release")},
      version,
      project_id,
      ${dq("public")},
      bookmarked,
      tags,
      input,
      output,
      session_id,
      created_at,
      updated_at,
      event_ts,
      is_deleted
    FROM traces
    WHERE session_id IN ({sessionIds: Array(String)})
    AND project_id = {projectId: String}
    ${timestamp ? `AND timestamp >= {timestamp: DateTime}` : ""}
    ORDER BY event_ts DESC`;

  const records = await queryDoris<TraceRecordReadType>({
    query,
    params: {
      sessionIds,
      projectId,
      timestamp: timestamp ? convertDateToAnalyticsDateTime(timestamp) : null,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "list",
      projectId,
    },
  });

  const traces = records.map((r) => convertDorisToDomain(r));

  traces.forEach((trace) => {
    recordDistribution(
      "langfuse.traces_by_session_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
    );
  });

  return traces;
};

export const hasAnyTrace = async (projectId: string) => {
  const query = `
    SELECT 1
    FROM traces
    WHERE project_id = {projectId: String}
    LIMIT 1
  `;

  const rows = await queryDoris<{ 1: number }>({
    query,
    params: {
      projectId,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "hasAny",
      projectId,
    },
  });

  return rows.length > 0;
};

export const getTraceCountsByProjectInCreationInterval = async ({
  start,
  end,
}: {
  start: Date;
  end: Date;
}) => {
  const query = `
    SELECT
      project_id,
      count(*) as count
    FROM traces
    WHERE created_at >= {start: DateTime}
    AND created_at < {end: DateTime}
    GROUP BY project_id
  `;

  const rows = await queryDoris<{ project_id: string; count: string }>({
    query,
    params: {
      start: convertDateToAnalyticsDateTime(start),
      end: convertDateToAnalyticsDateTime(end),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
    },
  });

  return rows.map((row) => ({
    projectId: row.project_id,
    count: Number(row.count),
  }));
};

export const getTraceCountOfProjectsSinceCreationDate = async ({
  projectIds,
  start,
}: {
  projectIds: string[];
  start: Date;
}) => {
  const query = `
    SELECT
      count(*) as count
    FROM traces
    WHERE project_id IN ({projectIds: Array(String)})
    AND created_at >= {start: DateTime}
  `;

  const rows = await queryDoris<{ count: string }>({
    query,
    params: {
      projectIds,
      start: convertDateToAnalyticsDateTime(start),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
    },
  });

  return Number(rows[0]?.count ?? 0);
};

/**
 * Retrieves a trace record by its ID and associated project ID, with optional filtering by timestamp range.
 * If no timestamp filters are provided, runs two queries in parallel:
 * 1. One with a 7-day fromTimestamp filter (typically faster)
 * 2. One without any timestamp filters (complete but slower)
 * Returns the first non-empty result.
 */
export const getTraceById = async ({
  traceId,
  projectId,
  timestamp,
  fromTimestamp,
  renderingProps = DEFAULT_RENDERING_PROPS,
  excludeInputOutput = false,
}: {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  fromTimestamp?: Date;
  renderingProps?: RenderingProps;
  /** When true, sets input/output columns to empty in the query to reduce database load */
  excludeInputOutput?: boolean;
}) => {
  const query = `
    SELECT
      id,
      timestamp,
      name,
      user_id,
      to_json(metadata) as metadata,
      environment,
      ${dq("release")},
      version,
      project_id,
      ${dq("public")},
      bookmarked,
      tags,
      input,
      output,
      session_id,
      created_at,
      updated_at,
      event_ts,
      is_deleted
    FROM traces
    WHERE id = {traceId: String}
    AND project_id = {projectId: String}
    ${timestamp ? `AND DATE(timestamp) = DATE({timestamp: DateTime})` : ""}
    ${fromTimestamp ? `AND timestamp >= {fromTimestamp: DateTime}` : ""}
    ORDER BY event_ts DESC
    LIMIT 1
  `;

  const records = await queryDoris<TraceRecordReadType>({
    query,
    params: {
      traceId,
      projectId,
      ...(timestamp
        ? { timestamp: convertDateToAnalyticsDateTime(timestamp) }
        : {}),
      ...(fromTimestamp
        ? { fromTimestamp: convertDateToAnalyticsDateTime(fromTimestamp) }
        : {}),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "byId",
      projectId,
    },
  });

  logger.info(`Doris getTraceById records:`, {
    recordsCount: records.length,
    records: records.length > 0 ? records : "No records found",
  });

  const res = records.map((r) => convertDorisToDomain(r));

  res.forEach((trace) => {
    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
      {
        table: "traces",
      },
    );
  });

  return res.shift();
};

export const getTracesGroupedByName = async (
  projectId: string,
  tableDefinitions: UiColumnMappings = tracesTableUiColumnDefinitionsForDoris,
  timestampFilter?: FilterState,
) => {
  const dorisFilter = timestampFilter
    ? createDorisFilterFromFilterState(timestampFilter, tableDefinitions)
    : undefined;

  const timestampFilterRes = dorisFilter
    ? new FilterList(dorisFilter).apply()
    : undefined;

  const query = `
      select
        name as name,
        count(*) as count
      from traces t
      WHERE t.project_id = {projectId: String}
      AND t.name IS NOT NULL
      ${timestampFilterRes?.query ? `AND ${timestampFilterRes.query}` : ""}
      GROUP BY name
      ORDER BY count(*) desc
      LIMIT 1000;
    `;

  const rows = await queryDoris<{
    name: string;
    count: string;
  }>({
    query: query,
    params: {
      projectId: projectId,
      ...(timestampFilterRes ? timestampFilterRes.params : {}),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });

  return rows;
};

export const getTracesGroupedBySessionId = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit?: number,
  offset?: number,
  columns?: UiColumnMappings,
) => {
  const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  tracesFilter.push(
    ...createDorisFilterFromFilterState(
      filter,
      columns ?? tracesTableUiColumnDefinitionsForDoris,
    ),
  );

  const tracesFilterRes = tracesFilter.apply();
  const search = dorisSearchCondition(searchQuery, undefined, {
    type: "traces",
  });

  const query = `
      select
        session_id as session_id,
        count(*) as count
      from traces t
      WHERE t.project_id = {projectId: String}
      AND t.session_id IS NOT NULL
      AND t.session_id != ''
      ${tracesFilterRes?.query ? `AND ${tracesFilterRes.query}` : ""}
      ${search.query}
      GROUP BY session_id
      ORDER BY count desc
      ${limit !== undefined && offset !== undefined ? `LIMIT {limit: Int32} OFFSET {offset: Int32}` : ""}
  `;

  const rows = await queryDoris<{
    session_id: string;
    count: string;
  }>({
    query: query,
    params: {
      limit,
      offset,
      projectId,
      ...(tracesFilterRes ? tracesFilterRes.params : {}),
      ...(searchQuery ? search.params : {}),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });

  return rows;
};

export const getTracesGroupedByUsers = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit?: number,
  offset?: number,
  columns?: UiColumnMappings,
) => {
  const dorisFilter = createDorisFilterFromFilterState(
    filter,
    columns ?? tracesTableUiColumnDefinitionsForDoris,
  );

  const filterRes = new FilterList(dorisFilter).apply();
  const search = dorisSearchCondition(searchQuery, undefined, {
    type: "traces",
  });

  const query = `
    select
      user_id as user,
      count(*) as count
    from traces t
    WHERE t.project_id = {projectId: String}
    AND t.user_id IS NOT NULL
    AND t.user_id != ''
    ${filterRes?.query ? `AND ${filterRes.query}` : ""}
    ${search.query}
    GROUP BY user
    ORDER BY count desc
    ${limit !== undefined && offset !== undefined ? `LIMIT {limit: Int32} OFFSET {offset: Int32}` : ""}
  `;

  return queryDoris<{
    user: string;
    count: string;
  }>({
    query: query,
    params: {
      projectId: projectId,
      limit,
      offset,
      ...(filterRes ? filterRes.params : {}),
      ...search.params,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });
};

export type GroupedTracesQueryProp = {
  projectId: string;
  filter: FilterState;
  columns?: UiColumnMappings;
  columnDefinitions?: ColumnDefinition[];
};

export const getTracesGroupedByTags = async (props: GroupedTracesQueryProp) => {
  const { projectId, filter, columns } = props;

  const dorisFilter = createDorisFilterFromFilterState(
    filter,
    columns ?? tracesTableUiColumnDefinitionsForDoris,
  );

  const filterRes = new FilterList(dorisFilter).apply();

  // Doris uses LATERAL VIEW explode to unnest array elements (standard syntax)
  const query = `
    select distinct(tag) as value
    from traces t
    LATERAL VIEW explode(tags) tmp as tag
    WHERE t.project_id = {projectId: String}
    ${filterRes?.query ? `AND ${filterRes.query}` : ""}
    LIMIT 1000;
  `;

  const rows = await queryDoris<{
    value: string;
  }>({
    query: query,
    params: {
      projectId: projectId,
      ...(filterRes ? filterRes.params : {}),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });

  return rows;
};

export const getTracesIdentifierForSession = async (
  projectId: string,
  sessionId: string,
) => {
  // Use window function to achieve LIMIT 1 BY semantics in Doris
  const query = `
    SELECT
      id,
      user_id,
      name,
      timestamp,
      project_id,
      environment
    FROM traces
    WHERE (project_id = {projectId: String})
    AND (session_id = {sessionId: String})
    ORDER BY timestamp ASC;
  `;

  const rows = await queryDoris<{
    id: string;
    user_id: string;
    name: string;
    timestamp: string | Date;
    environment: string;
  }>({
    query: query,
    params: {
      projectId,
      sessionId,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "list",
      projectId,
    },
  });

  return rows.map((row) => ({
    id: row.id,
    userId: row.user_id,
    name: row.name,
    timestamp:
      row.timestamp instanceof Date
        ? row.timestamp
        : new Date(row.timestamp as string),
    environment: row.environment,
  }));
};

export const deleteTraces = async (projectId: string, traceIds: string[]) => {
  const query = `
    DELETE FROM traces
    WHERE project_id = {projectId: String}
    AND id IN ({traceIds: Array(String)});
  `;
  await commandDoris({
    query: query,
    params: {
      projectId,
      traceIds,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "delete",
      projectId,
    },
  });
};

export const hasAnyTraceOlderThan = async (
  projectId: string,
  beforeDate: Date,
) => {
  const query = `
    SELECT 1
    FROM traces
    WHERE project_id = {projectId: String}
    AND timestamp < {cutoffDate: DateTime}
    LIMIT 1
  `;

  const rows = await queryDoris<{ 1: number }>({
    query,
    params: {
      projectId,
      cutoffDate: convertDateToAnalyticsDateTime(beforeDate),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "hasAnyOlderThan",
      projectId,
    },
  });

  return rows.length > 0;
};

export const deleteTracesOlderThanDays = async (
  projectId: string,
  beforeDate: Date,
): Promise<boolean> => {
  const hasData = await hasAnyTraceOlderThan(projectId, beforeDate);
  if (!hasData) {
    return false;
  }

  const query = `
    DELETE FROM traces
    WHERE project_id = {projectId: String}
    AND timestamp < {cutoffDate: DateTime};
  `;
  await commandDoris({
    query: query,
    params: {
      projectId,
      cutoffDate: convertDateToAnalyticsDateTime(beforeDate),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "delete",
      projectId,
    },
  });
  return true;
};

export const deleteTracesByProjectId = async (
  projectId: string,
): Promise<boolean> => {
  const hasData = await hasAnyTrace(projectId);
  if (!hasData) {
    return false;
  }

  const query = `
    DELETE FROM traces
    WHERE project_id = {projectId: String};
  `;
  await commandDoris({
    query: query,
    params: {
      projectId,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "delete",
      projectId,
    },
  });
  return true;
};

export const hasAnyUser = async (projectId: string) => {
  const query = `
    SELECT 1
    FROM traces
    WHERE project_id = {projectId: String}
    AND user_id IS NOT NULL
    AND user_id != ''
    LIMIT 1
  `;

  const rows = await queryDoris<{ 1: number }>({
    query,
    params: {
      projectId,
    },
    tags: {
      feature: "tracing",
      type: "user",
      kind: "hasAny",
      projectId,
    },
  });

  return rows.length > 0;
};

export const getTotalUserCount = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
): Promise<{ totalCount: bigint }[]> => {
  const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  tracesFilter.push(
    ...createDorisFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitionsForDoris,
    ),
  );

  const tracesFilterRes = tracesFilter.apply();
  const search = dorisSearchCondition(searchQuery, undefined, {
    type: "traces",
  });

  const query = `
    SELECT COUNT(DISTINCT t.user_id) AS totalCount
    FROM traces t
    WHERE ${tracesFilterRes.query}
    ${search.query}
    AND t.user_id IS NOT NULL
    AND t.user_id != ''
  `;

  return queryDoris({
    query,
    params: {
      ...tracesFilterRes.params,
      ...search.params,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });
};

export const getUserMetrics = async (
  projectId: string,
  userIds: string[],
  filter: FilterState,
) => {
  if (userIds.length === 0) {
    return [];
  }

  // Helper function to parse timestamps from Doris
  const parseTimestamp = (timestamp: string | Date): Date => {
    if (timestamp instanceof Date) {
      return timestamp;
    }
    return new Date(timestamp);
  };

  // Use the same pattern as other methods - get default filter first
  const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  tracesFilter.push(
    ...createDorisFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitionsForDoris,
    ),
  );

  const tracesFilterRes = tracesFilter.apply();

  const timestampFilter = tracesFilter.find(
    (f) => f.field === "timestamp" && f.operator === ">=",
  ) as DorisDateTimeFilter | undefined;

  // Doris version using map format with proper null handling
  const query = `
      WITH stats as (
        SELECT
            t.user_id as user_id,
            MAX(t.environment) as environment,
            count(distinct o.id) as obs_count,
            sum(o.total_cost) as sum_total_cost,
            max(t.timestamp) as max_timestamp,
            min(t.timestamp) as min_timestamp,
            count(distinct t.id) as trace_count,
            sum(if(MAP_CONTAINS_KEY(o.usage_details,'input'),o.usage_details['input'],0)) as input_usage,
            sum(if(MAP_CONTAINS_KEY(o.usage_details,'output'),o.usage_details['output'],0)) as output_usage,
            sum(if(MAP_CONTAINS_KEY(o.usage_details,'total'),o.usage_details['total'],0)) as total_usage
        FROM
            (
                SELECT
                    o.project_id,
                    o.trace_id,
                    o.usage_details,
                    o.total_cost,
                    o.id
                FROM
                    observations o
                WHERE
                    o.project_id = {projectId: String}
                    ${timestampFilter ? `AND o.start_time >= DATE_SUB({traceTimestamp: DateTime}, ${OBSERVATIONS_TO_TRACE_INTERVAL})` : ""}
                    AND o.trace_id in (
                        SELECT
                            distinct id
                        from
                            traces t
                        where
                            user_id IN ({userIds: Array(String) })
                            AND project_id = {projectId: String}
                            ${tracesFilterRes.query ? `AND ${tracesFilterRes.query}` : ""}
                    )
            ) as o
            JOIN (
                SELECT
                    t.id,
                    t.user_id,
                    t.project_id,
                    t.timestamp,
                    t.environment
                FROM
                    traces t
                WHERE
                    t.user_id IN ({userIds: Array(String) })
                    AND t.project_id = {projectId: String}
                    ${tracesFilterRes.query ? `AND ${tracesFilterRes.query}` : ""}
            ) as t on t.id = o.trace_id
            and t.project_id = o.project_id
        group by
            t.user_id
      )
      SELECT
          input_usage,
          output_usage,
          total_usage,
          obs_count,
          trace_count,
          user_id,
          environment,
          sum_total_cost,
          max_timestamp,
          min_timestamp
      FROM
          stats
    `;

  const rows = await queryDoris<{
    user_id: string;
    environment: string;
    max_timestamp: string | Date;
    min_timestamp: string | Date;
    input_usage: string;
    output_usage: string;
    total_usage: string;
    obs_count: string;
    trace_count: string;
    sum_total_cost: string;
  }>({
    query,
    params: {
      projectId,
      userIds,
      ...(tracesFilterRes ? tracesFilterRes.params : {}),
      ...(timestampFilter
        ? {
            traceTimestamp: convertDateToAnalyticsDateTime(
              timestampFilter.value,
            ),
          }
        : {}),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });

  return rows.map((row) => ({
    userId: row.user_id,
    environment: row.environment,
    maxTimestamp: parseTimestamp(row.max_timestamp),
    minTimestamp: parseTimestamp(row.min_timestamp),
    inputUsage: Number(row.input_usage),
    outputUsage: Number(row.output_usage),
    totalUsage: Number(row.total_usage),
    observationCount: Number(row.obs_count),
    traceCount: Number(row.trace_count),
    totalCost: Number(row.sum_total_cost),
  }));
};

export const getTracesForBlobStorageExport = function (
  projectId: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const query = `
    SELECT
      id,
      timestamp,
      name,
      environment,
      project_id,
      metadata,
      user_id,
      session_id,
      ${dq("release")},
      version,
      ${dq("public")},
      bookmarked,
      tags,
      input,
      output
    FROM traces
    WHERE project_id = {projectId: String}
    AND timestamp >= {minTimestamp: DateTime}
    AND timestamp <= {maxTimestamp: DateTime}
  `;

  const records = queryDorisStream<Record<string, unknown>>({
    query,
    params: {
      projectId,
      minTimestamp: convertDateToAnalyticsDateTime(minTimestamp),
      maxTimestamp: convertDateToAnalyticsDateTime(maxTimestamp),
    },
    tags: {
      feature: "blobstorage",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });

  return records;
};

export const getTracesForAnalyticsIntegrations = async function* (
  projectId: string,
  projectName: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const query = `
    WITH observations_agg AS (
      SELECT o.project_id,
             o.trace_id,
             sum(total_cost) as total_cost,
             count(*) as observation_count,
             milliseconds_diff(
               CASE WHEN max(start_time) > max(end_time) THEN max(start_time) ELSE max(end_time) END,
               CASE WHEN min(start_time) < min(end_time) THEN min(start_time) ELSE min(end_time) END
             ) as latency_milliseconds
      FROM observations o
      WHERE o.project_id = {projectId: String}
      AND o.start_time >= DATE_SUB({minTimestamp: DateTime}, ${TRACE_TO_OBSERVATIONS_INTERVAL})
      GROUP BY o.project_id, o.trace_id
    )

    SELECT
      t.id as id,
      t.timestamp as timestamp,
      t.name as name,
      t.session_id as session_id,
      t.user_id as user_id,
      t.${dq("release")} as ${dq("release")},
      t.version as version,
      t.tags as tags,
      t.metadata['$posthog_session_id'] as posthog_session_id,
      o.total_cost as total_cost,
      o.latency_milliseconds / 1000 as latency,
      o.observation_count as observation_count
    FROM traces t
    LEFT JOIN observations_agg o ON t.id = o.trace_id AND t.project_id = o.project_id
    WHERE t.project_id = {projectId: String}
    AND t.timestamp >= {minTimestamp: DateTime}
    AND t.timestamp <= {maxTimestamp: DateTime}
  `;

  const records = queryDorisStream<Record<string, unknown>>({
    query,
    params: {
      projectId,
      minTimestamp: convertDateToAnalyticsDateTime(minTimestamp),
      maxTimestamp: convertDateToAnalyticsDateTime(maxTimestamp),
    },
    tags: {
      feature: "posthog",
      type: "trace",
      kind: "analytic",
      projectId,
    },
  });

  const baseUrl = env.NEXTAUTH_URL?.replace("/api/auth", "");
  for await (const record of records) {
    yield {
      timestamp: record.timestamp,
      langfuse_id: record.id,
      langfuse_trace_name: record.name,
      langfuse_url: `${baseUrl}/project/${projectId}/traces/${encodeURIComponent(record.id as string)}`,
      langfuse_cost_usd: record.total_cost,
      langfuse_count_observations: record.observation_count,
      langfuse_session_id: record.session_id,
      langfuse_project_id: projectId,
      langfuse_user_id: record.user_id || "langfuse_unknown_user",
      langfuse_latency: record.latency,
      langfuse_release: record.release,
      langfuse_version: record.version,
      langfuse_tags: parseDorisStringArray(record.tags),
      langfuse_event_version: "1.0.0",
      $session_id: record.posthog_session_id ?? null,
      $set: {
        langfuse_user_url: record.user_id
          ? `${baseUrl}/project/${projectId}/users/${encodeURIComponent(record.user_id as string)}`
          : null,
      },
    };
  }
};

/**
 * This query is used only for legacy support of redirects without a projectId.
 * We don't have an index on the traceId so it will be a full table scan.
 * We expect at most 10s of calls per day, so this is acceptable.
 */
export const getTracesByIdsForAnyProject = async (traceIds: string[]) => {
  const query = `
      SELECT id, project_id
      FROM traces
      WHERE id IN ({traceIds: Array(String)})
      ORDER BY event_ts DESC;`;
  const records = await queryDoris<{
    id: string;
    project_id: string;
  }>({
    query,
    params: {
      traceIds,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "list",
    },
  });

  return records.map((record) => ({
    id: record.id,
    projectId: record.project_id,
  }));
};

export const traceWithSessionIdExists = async (
  projectId: string,
  sessionId: string,
) => {
  const query = `
    SELECT id, project_id
    FROM traces
    WHERE session_id = {sessionId: String}
    AND project_id = {projectId: String}
    LIMIT 1
  `;

  const result = await queryDoris<{ id: string; project_id: string }>({
    query,
    params: {
      sessionId,
      projectId,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "exists",
      projectId,
    },
  });

  return result.length > 0;
};

export async function getAgentGraphData(params: {
  projectId: string;
  traceId: string;
  chMinStartTime: string;
  chMaxStartTime: string;
}) {
  const { projectId, traceId, chMinStartTime, chMaxStartTime } = params;

  const query = `
          SELECT
            id,
            parent_observation_id,
            type,
            name,
            CAST(start_time AS STRING) AS start_time,
            CAST(end_time AS STRING) AS end_time,
            metadata['langgraph_node'] AS node,
            metadata['langgraph_step'] AS step
          FROM
            observations
          WHERE
            project_id = {projectId: String}
            AND trace_id = {traceId: String}
            AND start_time >= {chMinStartTime: DateTime}
            AND start_time <= {chMaxStartTime: DateTime}
        `;

  return queryDoris({
    query,
    params: {
      traceId,
      projectId,
      chMinStartTime,
      chMaxStartTime,
    },
  });
}

/**
 * Get trace counts grouped by project and day within a date range.
 *
 * Returns one row per project per day with the count of traces created on that day.
 * Uses half-open interval [startDate, endDate) for filtering.
 *
 * @param startDate - Start of date range (inclusive)
 * @param endDate - End of date range (exclusive)
 * @returns Array of { count, projectId, date } objects
 *
 * @example
 * // Get trace counts for March 1-2, 2024
 * const counts = await getTraceCountsByProjectAndDay({
 *   startDate: new Date('2024-03-01T00:00:00Z'),
 *   endDate: new Date('2024-03-03T00:00:00Z')
 * });
 * // Returns: [
 * //   { count: 1500, projectId: 'proj-123', date: '2024-03-01' },
 * //   { count: 1200, projectId: 'proj-123', date: '2024-03-02' },
 * //   { count: 2300, projectId: 'proj-456', date: '2024-03-01' },
 * //   ...
 * // ]
 *
 * Note: Skips using FINAL (double counting risk) for faster and cheaper
 * queries against Doris. Generous 4x overcompensation before blocking allows
 * for usage aggregation to be meaningful.
 *
 */
export const getTraceCountsByProjectAndDay = async ({
  startDate,
  endDate,
}: {
  startDate: Date;
  endDate: Date;
}) => {
  const query = `
    SELECT
      count(*) as count,
      project_id,
      toDate(timestamp) as date
    FROM traces
    WHERE timestamp >= {startDate: DateTime}
    AND timestamp < {endDate: DateTime}
    GROUP BY project_id, toDate(timestamp)
  `;

  const rows = await queryDoris<{
    count: string;
    project_id: string;
    date: string;
  }>({
    query,
    params: {
      startDate: convertDateToAnalyticsDateTime(startDate),
      endDate: convertDateToAnalyticsDateTime(endDate),
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "analytic",
    },
  });

  return rows.map((row) => ({
    count: Number(row.count),
    projectId: row.project_id,
    date: row.date,
  }));
};
