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
import { queryDoris, commandDoris, queryDorisStream } from "./doris";
import { dorisSearchCondition } from "../queries/doris-sql/search";
import { TraceRecordReadType } from "./definitions";
import { convertDorisToDomain } from "./traces_converters";

/**
 * Trace reads, post async-MV era. The traces_mv-shaped any_value aggregation
 * over events_full is GONE — every trace read is a flat-row lookup:
 *   - Scalars: traces_scalar (one row per trace, migration 0039), also the
 *     AUTHORITATIVE store for the mutable flags (bookmarked/public/tags —
 *     events_full is DUPLICATE-model, no UPDATEs).
 *   - Full input/output (the only trace fields NOT on traces_scalar): a flat
 *     read of the trace's root span row in events_full (is_root = 1).
 * Consequence: a trace is only readable once its ROOT span arrived (OTel
 * exports the root last, so an in-flight trace has no scalar row and no root
 * row). Such traces are absent from decoration/byId until the root lands.
 */

/**
 * Shared SELECT list for flat traces_scalar reads; shape matches
 * TraceRecordReadType / convertDorisToDomain.
 */
const TRACES_SCALAR_SELECT = `
      id,
      project_id,
      start_time AS \`timestamp\`,
      name,
      user_id,
      session_id,
      ${dq("release")},
      version,
      environment,
      bookmarked,
      ${dq("public")},
      tags,
      metadata,
      input_trim,
      output_trim,
      created_at,
      updated_at,
      event_ts`;

/**
 * Full input/output for ONE trace: point read of the root span row.
 * ORDER BY event_ts DESC — events_full is DUPLICATE-model, a re-delivered
 * root span lands as a second row; pick the newest copy deterministically.
 */
const TRACE_ROOT_IO_QUERY = `
    SELECT
      CAST(input AS STRING) AS input,
      CAST(output AS STRING) AS output
    FROM events_full
    WHERE project_id = {projectId: String}
    AND trace_id = {traceId: String}
    AND is_root = 1
    AND DATE(start_time) = DATE({rootDay: DateTime})
    ORDER BY event_ts DESC
    LIMIT 1
  `;

/**
 * Batch variant: full input/output for a SET of scalar rows, merged in TS.
 * The root span's start_time is byte-identical to the scalar row's (both
 * dual-written from the same root record), so the scalar rows' own timestamps
 * bound the partition range exactly. DUPLICATE model: ORDER BY event_ts DESC
 * + first-wins keeps the newest copy of a re-delivered root.
 */
const attachRootIO = async (params: {
  projectId: string;
  records: TraceRecordReadType[];
  tags: Record<string, string>;
}): Promise<TraceRecordReadType[]> => {
  const { projectId, records, tags } = params;
  if (records.length === 0) return records;
  const timestamps = records.map((r) => String(r.timestamp));
  const ioRows = await queryDoris<{
    id: string;
    input: string | null;
    output: string | null;
  }>({
    query: `
      SELECT
        trace_id AS id,
        CAST(input AS STRING) AS input,
        CAST(output AS STRING) AS output
      FROM events_full
      WHERE project_id = {projectId: String}
      AND trace_id IN ({ioTraceIds: Array(String)})
      AND is_root = 1
      AND DATE(start_time) >= DATE({ioMinTs: DateTime})
      AND DATE(start_time) <= DATE({ioMaxTs: DateTime})
      ORDER BY event_ts DESC
    `,
    params: {
      projectId,
      ioTraceIds: records.map((r) => r.id),
      ioMinTs: timestamps.reduce((a, b) => (a < b ? a : b)),
      ioMaxTs: timestamps.reduce((a, b) => (a > b ? a : b)),
    },
    tags,
  });
  const ioById = new Map<
    string,
    { input: string | null; output: string | null }
  >();
  for (const row of ioRows) {
    if (!ioById.has(row.id)) ioById.set(row.id, row);
  }
  return records.map((r) => ({
    ...r,
    input: ioById.get(r.id)?.input ?? null,
    output: ioById.get(r.id)?.output ?? null,
  }));
};

/**
 * byId FALLBACK: the trace read flat off its root span row in events_full —
 * used when the traces_scalar row is missing (scalar stream load lagging the
 * events_full one, or data predating the dual-write). Mutable flags
 * (bookmarked/public/tags) read here are the ingestion-time snapshot.
 */
const buildRootSpanTraceQuery = (params: {
  whereSql: string;
  /** Skip the full input/output Variant; only input_trim/output_trim are
   * returned. Used for verbosity "compact". */
  excludeFullIO?: boolean;
}): string => {
  const { whereSql, excludeFullIO = false } = params;
  const fullIO = excludeFullIO
    ? ""
    : `
      CAST(input AS STRING) AS input,
      CAST(output AS STRING) AS output,`;
  return `
    SELECT
      trace_id AS id,
      project_id,
      start_time AS \`timestamp\`,
      IF(trace_name <> '', trace_name, name) AS name,
      NULLIF(user_id, '') AS user_id,
      NULLIF(session_id, '') AS session_id,
      NULLIF(${dq("release")}, '') AS ${dq("release")},
      NULLIF(version, '') AS version,
      NULLIF(environment, '') AS environment,
      bookmarked,
      ${dq("public")},
      created_at,
      updated_at,
      event_ts,
      is_deleted,
      tags,
      input_trim,
      output_trim,${fullIO}
      metadata
    FROM events_full
    WHERE ${whereSql}
    AND is_root = 1
    ORDER BY event_ts DESC
    LIMIT 1
  `;
};

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
      field: "trace_id",
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

  // Phase C: is_root = 1 identifies the root span of a trace
  // (one row per trace). Trace-level fields are denormalized onto root
  // spans by createEventRecord, so trace identity / existence is a
  // single-row filter without aggregation.
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
        FROM events_full o
        WHERE o.project_id = '${projectId}'
        ${timeStampFilter ? `AND o.start_time >= '${toDorisDateTime(timestamp, -172800)}'` : ""}
        AND o.start_time >= '${toDorisDateTime(timestamp, -172800)}'
        GROUP BY trace_id, project_id
    )
    SELECT
      t.trace_id as id,
      t.project_id as project_id
    FROM events_full t
    ${observationFilterRes ? `INNER JOIN observations_agg o ON t.trace_id = o.trace_id AND t.project_id = o.project_id` : ""}
    WHERE ${tracesFilterRes.query}
    AND t.project_id = '${projectId}'
    AND t.is_root = 1
    AND t.start_time >= '${toDorisDateTime(timestamp, -172800)}'
    ${maxTimeStamp ? `AND t.start_time <= '${toDorisDateTime(maxTimeStamp)}'` : ""}
    ${!maxTimeStamp ? `AND t.start_time <= '${toDorisDateTime(timestamp, 172800)}'` : ""}
    ${exactTimestamp ? `AND t.start_time = '${toDorisDateTime(exactTimestamp)}'` : ""}
    GROUP BY t.trace_id, t.project_id
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

export const getTracesByIds = async (
  traceIds: string[],
  projectId: string,
  timestamp?: Date,
  opts?: {
    /**
     * Skip the full input/output Variant columns. Callers that only decorate
     * rows with trace scalars (name/tags/timestamp — e.g. the observations
     * list) must set this: the lookup stays a single flat traces_scalar read.
     * Exports keep the default (they serialize input/output → one extra
     * batch point read of the root span rows).
     */
    excludeFullIO?: boolean;
  },
) => {
  // Flat IN lookup on the (project_id, id) key prefix of traces_scalar — no
  // aggregation. Traces without a scalar row (root span not yet arrived) are
  // simply absent from the result; callers decorate best-effort.
  const scalarRows = await queryDoris<TraceRecordReadType>({
    query: `
      SELECT ${TRACES_SCALAR_SELECT}
      FROM traces_scalar
      WHERE project_id = {projectId: String}
        AND id IN ({traceIds: Array(String)})
        ${timestamp ? `AND start_time >= {timestamp: DateTime}` : ""}
    `,
    params: {
      traceIds,
      projectId,
      timestamp: timestamp ? convertDateToAnalyticsDateTime(timestamp) : null,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "byIds-scalar",
      projectId,
    },
  });

  const records = opts?.excludeFullIO
    ? scalarRows
    : await attachRootIO({
        projectId,
        records: scalarRows,
        tags: {
          feature: "tracing",
          type: "trace",
          kind: "byIds-root-io",
          projectId,
        },
      });

  // metadata comes back as a native MAP (JSONEachRow → JS object);
  // convertDorisToDomain → parseMetadataCHRecordToDomain handles it.
  return records.map((r) => convertDorisToDomain(r));
};

/**
 * Per-trace usage/cost breakdown for the traces-list hover tooltip.
 *
 * The list query (traces-ui-table-service) now returns only the precomputed
 * scalar totals, so the per-key breakdown (input_cache_read, etc.) is fetched
 * lazily, per trace, when the user hovers. Scoped to a single trace_id it hits
 * one bucket and a handful of observation rows, so we just read their
 * usage_details / cost_details maps and sum them by key in TS — no explode_map.
 */
export const getTraceUsageBreakdown = async ({
  projectId,
  traceId,
  timestamp,
}: {
  projectId: string;
  traceId: string;
  timestamp?: Date;
}): Promise<{
  usageDetails: Record<string, number>;
  costDetails: Record<string, number>;
}> => {
  const rows = await queryDoris<{
    usage_details: string | Record<string, number> | null;
    cost_details: string | Record<string, number> | null;
  }>({
    query: `
      SELECT
        to_json(usage_details) AS usage_details,
        to_json(cost_details) AS cost_details
      FROM events_full
      WHERE project_id = {projectId: String}
      AND trace_id = {traceId: String}
      AND is_root = 0
      ${timestamp ? `AND start_time >= DATE_SUB({timestamp: DateTime}, INTERVAL 2 DAY)` : ""}
    `,
    params: {
      projectId,
      traceId,
      timestamp: timestamp ? convertDateToAnalyticsDateTime(timestamp) : null,
    },
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "byId",
      projectId,
    },
  });

  const sumInto = (
    acc: Record<string, number>,
    details: string | Record<string, number> | null,
  ) => {
    if (!details) return;
    let parsed: Record<string, unknown> | null = null;
    if (typeof details === "string") {
      const trimmed = details.trim();
      if (
        !trimmed ||
        trimmed === "null" ||
        trimmed === "NULL" ||
        trimmed === "{}"
      ) {
        return;
      }
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        return;
      }
    } else if (typeof details === "object" && !Array.isArray(details)) {
      parsed = details;
    }
    if (!parsed) return;
    for (const [key, value] of Object.entries(parsed)) {
      const num = Number(value);
      if (!Number.isNaN(num)) acc[key] = (acc[key] ?? 0) + num;
    }
  };

  const usageDetails: Record<string, number> = {};
  const costDetails: Record<string, number> = {};
  for (const row of rows) {
    sumInto(usageDetails, row.usage_details);
    sumInto(costDetails, row.cost_details);
  }
  return { usageDetails, costDetails };
};

export const getTracesBySessionId = async (
  projectId: string,
  sessionIds: string[],
  timestamp?: Date,
) => {
  // Flat traces_scalar read (session_id hits its inverted index; one row per
  // trace, no aggregation), then the full input/output Variants are merged in
  // from a batch point read of the root span rows — the public sessions API
  // returns full trace objects.
  const scalarRows = await queryDoris<TraceRecordReadType>({
    query: `
      SELECT ${TRACES_SCALAR_SELECT}
      FROM traces_scalar
      WHERE project_id = {projectId: String}
        AND session_id IN ({sessionIds: Array(String)})
        ${timestamp ? `AND start_time >= {timestamp: DateTime}` : ""}
      ORDER BY event_ts DESC
    `,
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

  const records = await attachRootIO({
    projectId,
    records: scalarRows,
    tags: {
      feature: "tracing",
      type: "trace",
      kind: "list-root-io",
      projectId,
    },
  });

  // metadata comes back as a native MAP (JSONEachRow → JS object);
  // convertDorisToDomain → parseMetadataCHRecordToDomain handles it.
  const traces = records.map((r) => convertDorisToDomain(r));

  traces.forEach((trace) => {
    recordDistribution(
      "langfuse.traces_by_session_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
    );
  });

  return traces;
};

// traces_scalar (one row per trace — migration 0039) exposed under
// events_full-compatible column names, so the shared filter mappings
// (t.start_time, t.trace_name, ...) and dorisSearchCondition (t.trace_id,
// t.user_id, t.trace_name) apply to it unchanged. Lets the filter-option and
// trace-counting reads below run on a table ~2 orders of magnitude smaller
// than events_full (one row per trace vs one per span) instead of full-project
// is_root = 1 scans across every partition.
const TRACES_SCALAR_AS_T = `(
      SELECT
        project_id,
        id AS trace_id,
        name AS trace_name,
        name,
        user_id,
        session_id,
        environment,
        tags,
        start_time,
        created_at,
        event_ts
      FROM traces_scalar
    ) t`;

export const hasAnyTrace = async (projectId: string) => {
  const query = `
    SELECT 1
    FROM traces_scalar
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
  // traces_scalar: one row per trace, created_at dual-written since migration
  // 0039's trim/audit columns (pre-existing rows need the documented backfill,
  // which carries created_at from events_full).
  const query = `
    SELECT
      project_id,
      count(*) as count
    FROM traces_scalar
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
    FROM traces_scalar
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
 * Retrieves a trace record by its ID and associated project ID, with optional
 * filtering by timestamp range. Hintless calls probe the last 7 days first
 * (partition-pruned) and fall back to an unbounded query only when empty.
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
  // Scalar-first fast path for EVERY verbosity: all trace scalars live on
  // traces_scalar (one row per trace, dual-written at ingestion — migration
  // 0039), which is also the authoritative store for the mutable flags
  // (bookmarked/public/tags — events_full is DUPLICATE-model, no UPDATEs).
  // (project_id, id) is the unique-key prefix and id the distribution column,
  // so this is a single-tablet point lookup. Verbosity full/truncated then
  // fetches the only fields NOT on traces_scalar — the full input/output
  // Variants — via a point read of the root span row, day-pruned by the
  // scalar row's own start_time. Falls back to the events_full aggregate
  // below when the scalar row is missing (a trace still in flight — OTel
  // exports the root span last — or data predating the dual-write).
  const scalarRows = await queryDoris<TraceRecordReadType>({
    query: `
      SELECT ${TRACES_SCALAR_SELECT}
      FROM traces_scalar
      WHERE project_id = {projectId: String}
        AND id = {traceId: String}
        ${timestamp ? `AND start_time_date = DATE({timestamp: DateTime})` : ""}
        ${fromTimestamp ? `AND start_time >= {fromTimestamp: DateTime}` : ""}
      LIMIT 1
    `,
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
      kind: "byId-scalar",
      projectId,
    },
  });
  if (scalarRows.length > 0) {
    const scalarRecord = { ...scalarRows[0] };
    if (!excludeInputOutput) {
      const ioRows = await queryDoris<{
        input: string | null;
        output: string | null;
      }>({
        query: TRACE_ROOT_IO_QUERY,
        params: {
          traceId,
          projectId,
          // The scalar row's own timestamp — exact day partition of the root
          // span, so the Variant point read prunes to one partition.
          rootDay: scalarRecord.timestamp,
        },
        tags: {
          feature: "tracing",
          type: "trace",
          kind: "byId-root-io",
          projectId,
        },
      });
      scalarRecord.input = ioRows[0]?.input ?? null;
      scalarRecord.output = ioRows[0]?.output ?? null;
    }
    const trace = {
      ...convertDorisToDomain(scalarRecord),
      input_trim: scalarRecord.input_trim ?? null,
      output_trim: scalarRecord.output_trim ?? null,
    };
    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
      {
        table: "traces_scalar",
      },
    );
    return trace;
  }

  // Fallback: the scalar row is missing (scalar stream load lagging the
  // events_full one, or data predating the dual-write) — read the trace flat
  // off its root span row in events_full instead.
  // Hintless callers (comments validation, peeks opened from a bare URL, ...)
  // used to fire ONE unbounded query — a scan across every partition of the
  // trace's inverted-index entries. Instead, probe the last 7 days first
  // (partition-pruned natively via the start_time bound) and only fall back
  // to the unbounded query when the trace is genuinely older.
  const attempts: Array<{ fromTs?: Date; unbounded: boolean }> =
    timestamp || fromTimestamp
      ? [{ fromTs: fromTimestamp, unbounded: false }]
      : [
          {
            fromTs: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
            unbounded: false,
          },
          { fromTs: undefined, unbounded: true },
        ];

  let records: TraceRecordReadType[] = [];
  for (const attempt of attempts) {
    const whereSql = `
    trace_id = {traceId: String}
    AND project_id = {projectId: String}
    ${timestamp ? `AND DATE(start_time) = DATE({timestamp: DateTime})` : ""}
    ${attempt.fromTs ? `AND start_time >= {fromTimestamp: DateTime}` : ""}
  `;
    // "compact" (the traces-list cell preview) only needs the trim preview, so
    // skip the full input/output Variant; full/truncated read the Variants
    // straight off the root row.
    const query = buildRootSpanTraceQuery({
      whereSql,
      excludeFullIO: excludeInputOutput,
    });

    // metadata comes back as a native MAP (JSONEachRow → JS object);
    // convertDorisToDomain → parseMetadataCHRecordToDomain handles it.
    records = await queryDoris<TraceRecordReadType>({
      query,
      params: {
        traceId,
        projectId,
        ...(timestamp
          ? { timestamp: convertDateToAnalyticsDateTime(timestamp) }
          : {}),
        ...(attempt.fromTs
          ? { fromTimestamp: convertDateToAnalyticsDateTime(attempt.fromTs) }
          : {}),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: attempt.unbounded ? "byId-unbounded" : "byId",
        projectId,
      },
    });
    if (records.length > 0) break;
  }

  // Carry the precomputed compact preview (root span input_trim/output_trim)
  // alongside the domain object so trace.byId can serve it for verbosity
  // "compact" (the traces-list cell) without parsing the full Variant.
  const res = records.map((r) => ({
    ...convertDorisToDomain(r),
    input_trim: r.input_trim ?? null,
    output_trim: r.output_trim ?? null,
  }));

  res.forEach((trace) => {
    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
      {
        table: "events_full",
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

  // traces_scalar.name is the explicit trace_name — the same value the list's
  // Name column shows and its filters filter on, so the dropdown options now
  // match the list exactly (the old events_full root-span `name` could differ).
  const query = `
      select
        name as name,
        count(*) as count
      from ${TRACES_SCALAR_AS_T}
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
      from ${TRACES_SCALAR_AS_T}
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
    from ${TRACES_SCALAR_AS_T}
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
    from ${TRACES_SCALAR_AS_T}
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
      trace_id AS id,
      user_id,
      name,
      start_time AS timestamp,
      project_id,
      environment
    FROM events_full
    WHERE is_root = 1
    AND project_id = {projectId: String}
    AND session_id = {sessionId: String}
    ORDER BY start_time ASC;
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
    DELETE FROM events_full
    WHERE project_id = {projectId: String}
    AND trace_id IN ({traceIds: Array(String)});
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
  // Mirror into traces_scalar (one row per trace, dual-written at ingestion —
  // migration 0039) so deleted traces don't linger in the flat list fast path.
  await commandDoris({
    query: `
      DELETE FROM traces_scalar
      WHERE project_id = {projectId: String}
      AND id IN ({traceIds: Array(String)});
    `,
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
  // trace_metrics_agg is a sync MV on events_full (migration 0040): the
  // events_full DELETE above maintains it — no mirror statement needed.
};

export const hasAnyTraceOlderThan = async (
  projectId: string,
  beforeDate: Date,
) => {
  const query = `
    SELECT 1
    FROM events_full
    WHERE is_root = 1
    AND project_id = {projectId: String}
    AND start_time < {cutoffDate: DateTime}
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
    DELETE FROM events_full
    WHERE is_root = 1
    AND project_id = {projectId: String}
    AND start_time < {cutoffDate: DateTime};
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
  // Mirror into traces_scalar (root rows only by construction).
  await commandDoris({
    query: `
      DELETE FROM traces_scalar
      WHERE project_id = {projectId: String}
      AND start_time < {cutoffDate: DateTime};
    `,
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
  // trace_metrics_agg is a sync MV on events_full (migration 0040): the
  // events_full DELETE above maintains it — no mirror statement needed.
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
    DELETE FROM events_full
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
  // Mirror into traces_scalar.
  await commandDoris({
    query: `
      DELETE FROM traces_scalar
      WHERE project_id = {projectId: String};
    `,
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
  // trace_metrics_agg is a sync MV on events_full (migration 0040): the
  // events_full DELETE above maintains it — no mirror statement needed.
  return true;
};

export const hasAnyUser = async (projectId: string) => {
  const query = `
    SELECT 1
    FROM events_full
    WHERE is_root = 1
    AND project_id = {projectId: String}
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
    FROM events_full t
    WHERE t.is_root = 1
    AND ${tracesFilterRes.query}
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
    (f) => f.field === "start_time" && f.operator === ">=",
  ) as DorisDateTimeFilter | undefined;

  // Phase C: is_root = 1 selects each trace's root span — one
  // row per trace, with user_id denormalized by createEventRecord.
  // The self-join produces (root span × all spans) groups so we can
  // pick user_id from the root while summing observation totals.
  const query = `
      WITH stats as (
        SELECT
            t.user_id as user_id,
            MAX(t.environment) as environment,
            count(distinct o.span_id) as obs_count,
            sum(o.total_cost) as sum_total_cost,
            max(t.start_time) as max_timestamp,
            min(t.start_time) as min_timestamp,
            count(distinct t.trace_id) as trace_count,
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
                    o.span_id
                FROM
                    events_full o
                WHERE
                    o.project_id = {projectId: String}
                    ${timestampFilter ? `AND o.start_time >= DATE_SUB({traceTimestamp: DateTime}, ${OBSERVATIONS_TO_TRACE_INTERVAL})` : ""}
                    AND o.trace_id in (
                        SELECT
                            distinct trace_id
                        from
                            events_full t
                        where
                            user_id IN ({userIds: Array(String) })
                            AND project_id = {projectId: String}
                            AND is_root = 1
                            ${tracesFilterRes.query ? `AND ${tracesFilterRes.query}` : ""}
                    )
            ) as o
            JOIN (
                SELECT
                    t.trace_id,
                    t.user_id,
                    t.project_id,
                    t.start_time,
                    t.environment
                FROM
                    events_full t
                WHERE
                    t.user_id IN ({userIds: Array(String) })
                    AND t.project_id = {projectId: String}
                    AND t.is_root = 1
                    ${tracesFilterRes.query ? `AND ${tracesFilterRes.query}` : ""}
            ) as t on t.trace_id = o.trace_id
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
      trace_id AS id,
      start_time AS timestamp,
      name,
      environment,
      project_id,
      metadata_names,
      metadata_values,
      user_id,
      session_id,
      ${dq("release")},
      version,
      ${dq("public")},
      bookmarked,
      tags,
      input,
      output
    FROM events_full
    WHERE is_root = 1
    AND project_id = {projectId: String}
    AND start_time >= {minTimestamp: DateTime}
    AND start_time <= {maxTimestamp: DateTime}
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
      FROM events_full o
      WHERE o.project_id = {projectId: String}
      AND o.start_time >= DATE_SUB({minTimestamp: DateTime}, ${TRACE_TO_OBSERVATIONS_INTERVAL})
      GROUP BY o.project_id, o.trace_id
    )

    SELECT
      t.trace_id as id,
      t.start_time as \`timestamp\`,
      t.name as name,
      t.session_id as session_id,
      t.user_id as user_id,
      t.${dq("release")} as ${dq("release")},
      t.version as version,
      t.tags as tags,
      element_at(t.metadata_values, array_position(t.metadata_names, '$posthog_session_id')) as posthog_session_id,
      o.total_cost as total_cost,
      o.latency_milliseconds / 1000 as latency,
      o.observation_count as observation_count
    FROM events_full t
    LEFT JOIN observations_agg o ON t.trace_id = o.trace_id AND t.project_id = o.project_id
    WHERE t.project_id = {projectId: String}
    AND t.is_root = 1
    AND t.start_time >= {minTimestamp: DateTime}
    AND t.start_time <= {maxTimestamp: DateTime}
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
      SELECT trace_id AS id, project_id
      FROM events_full
      WHERE is_root = 1
      AND trace_id IN ({traceIds: Array(String)})
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
    SELECT trace_id AS id, project_id
    FROM events_full
    WHERE is_root = 1
    AND session_id = {sessionId: String}
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
            span_id AS id,
            parent_span_id AS parent_observation_id,
            type,
            name,
            CAST(start_time AS STRING) AS start_time,
            CAST(end_time AS STRING) AS end_time,
            element_at(metadata_values, array_position(metadata_names, 'langgraph_node')) AS node,
            element_at(metadata_values, array_position(metadata_names, 'langgraph_step')) AS step
          FROM
            events_full
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
      DATE(start_time) as date
    FROM events_full
    WHERE is_root = 1
    AND start_time >= {startDate: DateTime}
    AND start_time < {endDate: DateTime}
    GROUP BY project_id, DATE(start_time)
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
