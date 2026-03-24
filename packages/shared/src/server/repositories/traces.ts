import {
  commandClickhouse,
  parseClickhouseUTCDateTimeFormat,
  queryClickhouse,
  queryClickhouseStream,
  upsertClickhouse,
} from "./clickhouse";
import {
  convertDateToAnalyticsDateTime,
  isDorisBackend,
  dq,
} from "./analytics";
import {
  createFilterFromFilterState,
  getProjectIdDefaultFilter,
} from "../queries/clickhouse-sql/factory";
import { FilterState } from "../../types";
import {
  DateTimeFilter,
  StringFilter,
} from "../queries/clickhouse-sql/clickhouse-filter";
import { FilterList } from "../queries";
import { TraceRecordReadType } from "./definitions";
import { tracesTableUiColumnDefinitions, tracesTableUiColumnDefinitionsForDoris } from "../tableMappings/mapTracesTable";
import { UiColumnMappings, ColumnDefinition } from "../../tableDefinitions";
import { tracesTableCols } from "../../tableDefinitions/tracesTable";
import {
  convertDateToClickhouseDateTime,
  PreferredClickhouseService,
} from "../clickhouse/client";
import { convertClickhouseToDomain } from "./traces_converters";
import { clickhouseSearchCondition } from "../queries/clickhouse-sql/search";
import {
  OBSERVATIONS_TO_TRACE_INTERVAL,
  TRACE_TO_OBSERVATIONS_INTERVAL,
} from "./constants";
import { env } from "../../env";
import { ClickHouseClientConfigOptions } from "@clickhouse/client";
import { recordDistribution } from "../instrumentation";
import type { AnalyticsTraceEvent } from "../analytics-integrations/types";
import { measureAndReturn } from "../clickhouse/measureAndReturn";
import { DEFAULT_RENDERING_PROPS, RenderingProps } from "../utils/rendering";
import { logger } from "../logger";
import { traceException } from "../instrumentation";
import { prisma } from "../../db";
import {
  createDorisFilterFromFilterState,
  getDorisProjectIdDefaultFilter,
} from "../queries/doris-sql/factory";
import { queryDoris, upsertDoris, commandDoris, queryDorisStream } from "./doris";
import {
  StringFilter as DorisStringFilter,
  DateTimeFilter as DorisDateTimeFilter,
} from "../queries/doris-sql/doris-filter";
import { dorisSearchCondition, DorisSearchContext } from "../queries/doris-sql/search";

/**
 * Checks if trace exists in clickhouse.
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
  if (isDorisBackend()) {
    const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
      tracesPrefix: "t",
    });

    const timeStampFilter = tracesFilter.find(
      (f) =>
        f.field === "timestamp" && (f.operator === ">=" || f.operator === ">"),
    ) as DorisDateTimeFilter | undefined;

    tracesFilter.push(
      ...createDorisFilterFromFilterState(filter, tracesTableUiColumnDefinitionsForDoris),
      new DorisStringFilter({
        dorisTable: "t",
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
      return adjustedDate.toISOString().replace('T', ' ').replace('Z', '');
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
  }

  const { tracesFilter } = getProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  const timeStampFilter = tracesFilter.find(
    (f) =>
      f.field === "timestamp" && (f.operator === ">=" || f.operator === ">"),
  ) as DateTimeFilter | undefined;

  tracesFilter.push(
    ...createFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitions,
      tracesTableCols,
    ),
    new StringFilter({
      clickhouseTable: "t",
      field: "id",
      operator: "=",
      value: traceId,
    }),
  );

  const observationFilter = tracesFilter.find(
    (f) => f.table === "observations",
  );
  const tracesFilterRes = tracesFilter.apply();
  const observationFilterRes = observationFilter?.apply();

  const observations_cte = `
    WITH observations_agg AS (
      SELECT
        multiIf(
          arrayExists(x -> x = 'ERROR', groupArray(level)), 'ERROR',
          arrayExists(x -> x = 'WARNING', groupArray(level)), 'WARNING',
          arrayExists(x -> x = 'DEFAULT', groupArray(level)), 'DEFAULT',
          'DEBUG'
        ) AS aggregated_level,
        countIf(level = 'ERROR') as error_count,
        countIf(level = 'WARNING') as warning_count,
        countIf(level = 'DEFAULT') as default_count,
        countIf(level = 'DEBUG') as debug_count,
        date_diff('millisecond', least(min(start_time), min(end_time)), greatest(max(start_time), max(end_time))) as latency_milliseconds,
        sumMap(usage_details) as usage_details,
        sumMap(cost_details) as cost_details,
        trace_id,
        project_id
      FROM observations o FINAL
      WHERE o.project_id = {projectId: String}
        ${timeStampFilter ? `AND o.start_time >= {traceTimestamp: DateTime64(3)} - ${OBSERVATIONS_TO_TRACE_INTERVAL}` : ""}
        AND o.start_time >= {timestamp: DateTime64(3)} - ${OBSERVATIONS_TO_TRACE_INTERVAL}
      GROUP BY trace_id, project_id
    )
  `;

  return measureAndReturn({
    operationName: "checkTraceExistsAndGetTimestamp",
    projectId,
    input: {
      params: {
        projectId,
        ...tracesFilterRes.params,
        ...(observationFilterRes ? observationFilterRes.params : {}),
        ...(timestamp
          ? { timestamp: convertDateToClickhouseDateTime(timestamp) }
          : {}),
        ...(maxTimeStamp
          ? { maxTimeStamp: convertDateToClickhouseDateTime(maxTimeStamp) }
          : {}),
        ...(exactTimestamp
          ? { exactTimestamp: convertDateToClickhouseDateTime(exactTimestamp) }
          : {}),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "exists",
        projectId,
        operation_name: "checkTraceExistsAndGetTimestamp",
      },
      timestamp: timestamp ?? exactTimestamp,
    },
    fn: async (input) => {
      const query = `
        ${observations_cte}
        SELECT
          t.id as id,
          t.project_id as project_id,
          t.timestamp as timestamp
        FROM traces t FINAL
        ${observationFilterRes ? `INNER JOIN observations_agg o ON t.id = o.trace_id AND t.project_id = o.project_id` : ""}
        WHERE ${tracesFilterRes.query}
        AND t.project_id = {projectId: String}
        AND t.timestamp >= {timestamp: DateTime64(3)} - ${TRACE_TO_OBSERVATIONS_INTERVAL}
        ${maxTimeStamp ? `AND t.timestamp <= {maxTimeStamp: DateTime64(3)}` : ""}
        ${!maxTimeStamp ? `AND t.timestamp <= {timestamp: DateTime64(3)} + INTERVAL 2 DAY` : ""}
        ${exactTimestamp ? `AND toDate(t.timestamp) = toDate({exactTimestamp: DateTime64(3)})` : ""}
        GROUP BY t.id, t.project_id, t.timestamp
      `;

      const rows = await queryClickhouse<{
        id: string;
        project_id: string;
        timestamp: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });

      return {
        exists: rows.length > 0,
        timestamp:
          rows.length > 0
            ? parseClickhouseUTCDateTimeFormat(rows[0].timestamp)
            : undefined,
      };
    },
  });
};

/**
 * Accepts a trace in a Clickhouse-ready format.
 * id, project_id, and timestamp must always be provided.
 */
export const upsertTrace = async (trace: Partial<TraceRecordReadType>) => {

  if (!["id", "project_id", "timestamp"].every((key) => key in trace)) {
    throw new Error("Identifier fields must be provided to upsert Trace.");
  }

  if (isDorisBackend()) {
    await upsertDoris({
      table: "traces",
      records: [trace as TraceRecordReadType],
      eventBodyMapper: convertClickhouseToDomain,
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "upsert",
        projectId: trace.project_id ?? "",
      },
    });
    return;
  }

  await upsertClickhouse({
    table: "traces",
    records: [trace as TraceRecordReadType],
    eventBodyMapper: convertClickhouseToDomain,
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
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined,
) => {
  if (isDorisBackend()) {
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

    return records.map((r) => convertClickhouseToDomain(r));
  }

  const records = await measureAndReturn({
    operationName: "getTracesByIds",
    projectId,
    input: {
      params: {
        traceIds,
        projectId,
        timestamp: timestamp
          ? convertDateToClickhouseDateTime(timestamp)
          : null,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "byId",
        projectId,
        operation_name: "getTracesByIds",
      },
      clickhouseConfigs,
    },
    fn: (input) => {
      const query = `
        SELECT *
        FROM traces
        WHERE id IN ({traceIds: Array(String)})
        AND project_id = {projectId: String}
        ${timestamp ? `AND timestamp >= {timestamp: DateTime64(3)}` : ""}
        ORDER BY event_ts DESC
        LIMIT 1 by id, project_id;
      `;
      return queryClickhouse<TraceRecordReadType>({
        query,
        params: input.params,
        tags: input.tags,
        clickhouseConfigs: input.clickhouseConfigs,
      });
    },
  });

  return records.map((record) =>
    convertClickhouseToDomain(record, DEFAULT_RENDERING_PROPS),
  );
};

export const getTracesBySessionId = async (
  projectId: string,
  sessionIds: string[],
  timestamp?: Date,
) => {
  if (isDorisBackend()) {
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

    const traces = records.map((r) => convertClickhouseToDomain(r));

    traces.forEach((trace) => {
      recordDistribution(
        "langfuse.traces_by_session_id_age",
        new Date().getTime() - trace.timestamp.getTime(),
      );
    });

    return traces;
  }

  const records = await measureAndReturn({
    operationName: "getTracesBySessionId",
    projectId,
    input: {
      params: {
        sessionIds,
        projectId,
        timestamp: timestamp
          ? convertDateToClickhouseDateTime(timestamp)
          : null,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "list",
        projectId,
        operation_name: "getTracesBySessionId",
      },
      timestamp,
    },
    fn: (input) => {
      const query = `
        SELECT *
        FROM traces
        WHERE session_id IN ({sessionIds: Array(String)})
        AND project_id = {projectId: String}
        ${timestamp ? `AND timestamp >= {timestamp: DateTime64(3)}` : ""}
        ORDER BY event_ts DESC
        LIMIT 1 by id, project_id;
      `;
      return queryClickhouse<TraceRecordReadType>({
        query,
        params: input.params,
        tags: input.tags,
      });
    },
  });

  const traces = records.map((record) =>
    convertClickhouseToDomain(record, DEFAULT_RENDERING_PROPS),
  );

  traces.forEach((trace) => {
    recordDistribution(
      "langfuse.traces_by_session_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
    );
  });

  return traces;
};

export const hasAnyTrace = async (projectId: string) => {
  if (isDorisBackend()) {
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
  }

  // Check PostgreSQL flag first — once set, it's never reverted
  try {
    const project = await prisma.project.findUnique({
      where: { id: projectId },
      select: { hasTraces: true },
    });
    if (project?.hasTraces) {
      return true;
    }
  } catch (error) {
    traceException(error);
    logger.error("Failed to read hasTraces flag from PostgreSQL", {
      projectId,
      error,
    });
  }

  const result = await measureAndReturn({
    operationName: "hasAnyTrace",
    projectId,
    input: {
      projectId,
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "hasAny",
        projectId,
        operation_name: "hasAnyTrace",
      },
    },
    fn: async (input) => {
      const query = `
        SELECT 1
        FROM traces
        WHERE project_id = {projectId: String}
        LIMIT 1
      `;

      const rows = await queryClickhouse<{ 1: number }>({
        query,
        params: {
          projectId: input.projectId,
        },
        tags: input.tags,
        clickhouseSettings: {
          max_threads: 1,
        },
      });

      return rows.length > 0;
    },
  });

  // Persist positive result in PostgreSQL — once a project has traces, it stays true
  // Only update if not already set to avoid unnecessary writes
  if (result) {
    try {
      await prisma.project.updateMany({
        where: { id: projectId, hasTraces: false },
        data: { hasTraces: true },
      });
    } catch (error) {
      traceException(error);
      logger.error("Failed to persist hasTraces flag to PostgreSQL", {
        projectId,
        error,
      });
    }
  }

  return result;
};

export const getTraceCountsByProjectInCreationInterval = async ({
  start,
  end,
}: {
  start: Date;
  end: Date;
}) => {
  if (isDorisBackend()) {
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
  }

  return measureAndReturn({
    operationName: "getTraceCountsByProjectInCreationInterval",
    projectId: "__CROSS_PROJECT__",
    input: {
      params: {
        start: convertDateToClickhouseDateTime(start),
        end: convertDateToClickhouseDateTime(end),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        operation_name: "getTraceCountsByProjectInCreationInterval",
      },
      timestamp: start,
    },
    fn: async (input) => {
      const query = `
        SELECT
          project_id,
          count(*) as count
        FROM traces
        WHERE created_at >= {start: DateTime64(3)}
        AND created_at < {end: DateTime64(3)}
        GROUP BY project_id
      `;

      const rows = await queryClickhouse<{ project_id: string; count: string }>(
        {
          query,
          params: input.params,
          tags: input.tags,
        },
      );

      return rows.map((row) => ({
        projectId: row.project_id,
        count: Number(row.count),
      }));
    },
  });
};

export const getTraceCountOfProjectsSinceCreationDate = async ({
  projectIds,
  start,
}: {
  projectIds: string[];
  start: Date;
}) => {
  if (isDorisBackend()) {
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
  }

  return measureAndReturn({
    operationName: "getTraceCountOfProjectsSinceCreationDate",
    projectId: "__CROSS_PROJECT__",
    input: {
      params: {
        projectIds,
        start: convertDateToClickhouseDateTime(start),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        operation_name: "getTraceCountOfProjectsSinceCreationDate",
      },
      timestamp: start,
    },
    fn: async (input) => {
      const query = `
        SELECT
          count(*) as count
        FROM traces
        WHERE project_id IN ({projectIds: Array(String)})
        AND created_at >= {start: DateTime64(3)}
      `;

      const rows = await queryClickhouse<{ count: string }>({
        query,
        params: input.params,
        tags: input.tags,
      });

      return Number(rows[0]?.count ?? 0);
    },
  });
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
  clickhouseFeatureTag = "tracing",
  preferredClickhouseService,
  excludeInputOutput = false,
}: {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  fromTimestamp?: Date;
  renderingProps?: RenderingProps;
  clickhouseFeatureTag?: string;
  preferredClickhouseService?: PreferredClickhouseService;
  /** When true, sets input/output columns to empty in the query to reduce database load */
  excludeInputOutput?: boolean;
}) => {
  if (isDorisBackend()) {
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

    const res = records.map((r) => convertClickhouseToDomain(r));

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
  }

  const records = await measureAndReturn({
    operationName: "getTraceById",
    projectId,
    input: {
      params: {
        traceId,
        projectId,
        ...(timestamp
          ? { timestamp: convertDateToClickhouseDateTime(timestamp) }
          : {}),
        ...(fromTimestamp
          ? { fromTimestamp: convertDateToClickhouseDateTime(fromTimestamp) }
          : {}),
      },
      tags: {
        feature: clickhouseFeatureTag,
        type: "trace",
        kind: "byId",
        projectId,
        operation_name: "getTraceById",
      },
    },
    fn: (input) => {
      const inputColumn = excludeInputOutput
        ? "''"
        : renderingProps.truncated
          ? `leftUTF8(input, ${env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT})`
          : "input";
      const outputColumn = excludeInputOutput
        ? "''"
        : renderingProps.truncated
          ? `leftUTF8(output, ${env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT})`
          : "output";

      const query = `
        SELECT
          id,
          name as name,
          user_id as user_id,
          metadata as metadata,
          release as release,
          version as version,
          project_id,
          environment,
          public as public,
          bookmarked as bookmarked,
          tags,
          ${inputColumn} as input,
          ${outputColumn} as output,
          session_id as session_id,
          0 as is_deleted,
          timestamp,
          created_at,
          updated_at
        FROM traces
        WHERE id = {traceId: String}
        AND project_id = {projectId: String}
        ${timestamp ? `AND toDate(timestamp) = toDate({timestamp: DateTime64(3)})` : ""}
        ${fromTimestamp ? `AND timestamp >= {fromTimestamp: DateTime64(3)}` : ""}
        ORDER BY event_ts DESC
        LIMIT 1
      `;

      return queryClickhouse<TraceRecordReadType>({
        query,
        params: input.params,
        tags: input.tags,
        preferredClickhouseService,
      });
    },
  });

  const res = records.map((record) =>
    convertClickhouseToDomain(record, renderingProps),
  );

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
  tableDefinitions: UiColumnMappings = tracesTableUiColumnDefinitions,
  timestampFilter?: FilterState,
) => {
  if (isDorisBackend()) {
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
  }

  const chFilter = timestampFilter
    ? createFilterFromFilterState(timestampFilter, tableDefinitions)
    : undefined;

  const timestampFilterRes = chFilter
    ? new FilterList(chFilter).apply()
    : undefined;

  return measureAndReturn({
    operationName: "getTracesGroupedByName",
    projectId,
    input: {
      params: {
        projectId,
        ...(timestampFilterRes ? timestampFilterRes.params : {}),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTracesGroupedByName",
      },
    },
    fn: async (input) => {
      // We mainly use queries like this to retrieve filter options.
      // Therefore, we can skip final as some inaccuracy in count is acceptable.
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

      return queryClickhouse<{
        name: string;
        count: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });
    },
  });
};

export const getTracesGroupedBySessionId = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit?: number,
  offset?: number,
  columns?: UiColumnMappings,
  columnDefinitions?: ColumnDefinition[],
) => {
  if (isDorisBackend()) {
    const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
      tracesPrefix: "t",
    });

    tracesFilter.push(
      ...createDorisFilterFromFilterState(
        filter,
        columns ?? tracesTableUiColumnDefinitions,
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
  }

  const { tracesFilter } = getProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  tracesFilter.push(
    ...createFilterFromFilterState(
      filter,
      columns ?? tracesTableUiColumnDefinitions,
      columnDefinitions ?? tracesTableCols,
    ),
  );

  const tracesFilterRes = tracesFilter.apply();
  const search = clickhouseSearchCondition(searchQuery, undefined, "t");

  return measureAndReturn({
    operationName: "getTracesGroupedBySessionId",
    projectId,
    input: {
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
        operation_name: "getTracesGroupedBySessionId",
      },
    },
    fn: async (input) => {
      // We mainly use queries like this to retrieve filter options.
      // Therefore, we can skip final as some inaccuracy in count is acceptable.
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

      return queryClickhouse<{
        session_id: string;
        count: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });
    },
  });
};

export const getTracesGroupedByUsers = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit?: number,
  offset?: number,
  columns?: UiColumnMappings,
  columnDefinitions?: ColumnDefinition[],
) => {
  if (isDorisBackend()) {
    const dorisFilter = createDorisFilterFromFilterState(
      filter,
      columns ?? tracesTableUiColumnDefinitions,
    );

    const filterRes = new FilterList(dorisFilter).apply();

    const query = `
      select
        user_id as user,
        count(*) as count
      from traces t
      WHERE t.project_id = {projectId: String}
      AND t.user_id IS NOT NULL
      AND t.user_id != ''
      ${filterRes?.query ? `AND ${filterRes.query}` : ""}
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
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
      },
    });
  }

  const { tracesFilter } = getProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  tracesFilter.push(
    ...createFilterFromFilterState(
      filter,
      columns ?? tracesTableUiColumnDefinitions,
      columnDefinitions ?? tracesTableCols,
    ),
  );

  const tracesFilterRes = tracesFilter.apply();
  const search = clickhouseSearchCondition(searchQuery, undefined, "t");

  return measureAndReturn({
    operationName: "getTracesGroupedByUsers",
    projectId,
    input: {
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
        operation_name: "getTracesGroupedByUsers",
      },
    },
    fn: async (input) => {
      // We mainly use queries like this to retrieve filter options.
      // Therefore, we can skip final as some inaccuracy in count is acceptable.
      const query = `
        select
          user_id as user,
          count(*) as count
        from traces t
        WHERE t.project_id = {projectId: String}
        AND t.user_id IS NOT NULL
        AND t.user_id != ''
        ${tracesFilterRes?.query ? `AND ${tracesFilterRes.query}` : ""}
        ${search.query}
        GROUP BY user
        ORDER BY count desc
        ${limit !== undefined && offset !== undefined ? `LIMIT {limit: Int32} OFFSET {offset: Int32}` : ""}
      `;

      return queryClickhouse<{
        user: string;
        count: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });
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
  const { projectId, filter, columns, columnDefinitions } = props;

  if (isDorisBackend()) {
    const dorisFilter = createDorisFilterFromFilterState(
      filter,
      columns ?? tracesTableUiColumnDefinitions,
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
  }

  const chFilter = createFilterFromFilterState(
    filter,
    columns ?? tracesTableUiColumnDefinitions,
    columnDefinitions ?? tracesTableCols,
  );

  const filterRes = new FilterList(chFilter).apply();

  return measureAndReturn({
    operationName: "getTracesGroupedByTags",
    projectId,
    input: {
      params: {
        projectId,
        ...(filterRes ? filterRes.params : {}),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTracesGroupedByTags",
      },
    },
    fn: async (input) => {
      const query = `
        select distinct(arrayJoin(tags)) as value
        from traces t
        WHERE t.project_id = {projectId: String}
        ${filterRes?.query ? `AND ${filterRes.query}` : ""}
        LIMIT 1000;
      `;

      return queryClickhouse<{
        value: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });
    },
  });
};

export const getTracesIdentifierForSession = async (
  projectId: string,
  sessionId: string,
) => {
  if (isDorisBackend()) {
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
        isDorisBackend() && row.timestamp instanceof Date
          ? row.timestamp
          : parseClickhouseUTCDateTimeFormat(row.timestamp as string),
      environment: row.environment,
    }));
  }

  const rows = await measureAndReturn({
    operationName: "getTracesIdentifierForSession",
    projectId,
    input: {
      params: {
        projectId,
        sessionId,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "list",
        projectId,
        operation_name: "getTracesIdentifierForSession",
      },
    },
    fn: (input) => {
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
        ORDER BY timestamp ASC
        LIMIT 1 BY id, project_id;
      `;

      return queryClickhouse<{
        id: string;
        user_id: string;
        name: string;
        timestamp: string;
        environment: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });
    },
  });

  return rows.map((row) => ({
    id: row.id,
    userId: row.user_id,
    name: row.name,
    timestamp: parseClickhouseUTCDateTimeFormat(row.timestamp),
    environment: row.environment,
  }));
};

export const deleteTraces = async (projectId: string, traceIds: string[]) => {
  if (isDorisBackend()) {
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
    return;
  }

  await measureAndReturn({
    operationName: "deleteTraces",
    projectId,
    input: {
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
    },
    fn: async (input) => {
      // Pre-flight query with time bounds computed
      const preflight = await queryClickhouse<{
        min_ts: string;
        max_ts: string;
        cnt: string;
      }>({
        query: `
          SELECT
            min(timestamp) - INTERVAL 1 HOUR as min_ts,
            max(timestamp) + INTERVAL 1 HOUR as max_ts,
            count(*) as cnt
          FROM traces
          WHERE project_id = {projectId: String} AND id IN ({traceIds: Array(String)})
        `,
        params: input.params,
        clickhouseConfigs: {
          request_timeout: env.LANGFUSE_CLICKHOUSE_DELETION_TIMEOUT_MS,
        },
        tags: { ...input.tags, kind: "delete-preflight" },
      });

      const count = Number(preflight[0]?.cnt ?? 0);
      if (count === 0) {
        logger.info(
          `deleteTraces: no rows found for project ${projectId}, skipping DELETE`,
        );
        return;
      }

      await commandClickhouse({
        query: `
          DELETE FROM traces
          WHERE project_id = {projectId: String}
          AND id IN ({traceIds: Array(String)})
          AND timestamp >= {minTs: String}::DateTime64(3)
          AND timestamp <= {maxTs: String}::DateTime64(3)
        `,
        params: {
          ...input.params,
          minTs: preflight[0].min_ts,
          maxTs: preflight[0].max_ts,
        },
        clickhouseConfigs: {
          request_timeout: env.LANGFUSE_CLICKHOUSE_DELETION_TIMEOUT_MS,
        },
        tags: input.tags,
      });
    },
  });
};

export const hasAnyTraceOlderThan = async (
  projectId: string,
  beforeDate: Date,
) => {
  if (isDorisBackend()) {
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
  }

  const query = `
    SELECT 1
    FROM traces
    WHERE project_id = {projectId: String}
    AND timestamp < {cutoffDate: DateTime64(3)}
    LIMIT 1
  `;

  const rows = await queryClickhouse<{ 1: number }>({
    query,
    params: {
      projectId,
      cutoffDate: convertDateToClickhouseDateTime(beforeDate),
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
  if (isDorisBackend()) {
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
  }

  const hasData = await hasAnyTraceOlderThan(projectId, beforeDate);
  if (!hasData) {
    return false;
  }

  await measureAndReturn({
    operationName: "deleteTracesOlderThanDays",
    projectId,
    input: {
      params: {
        projectId,
        cutoffDate: convertDateToClickhouseDateTime(beforeDate),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "delete",
        projectId,
      },
    },
    fn: async (input) => {
      const query = `
        DELETE FROM traces
        WHERE project_id = {projectId: String}
        AND timestamp < {cutoffDate: DateTime64(3)};
      `;
      await commandClickhouse({
        query: query,
        params: input.params,
        clickhouseConfigs: {
          request_timeout: env.LANGFUSE_CLICKHOUSE_DELETION_TIMEOUT_MS,
        },
        tags: input.tags,
      });
    },
  });

  return true;
};

export const deleteTracesByProjectId = async (
  projectId: string,
): Promise<boolean> => {
  if (isDorisBackend()) {
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
  }

  const hasData = await hasAnyTrace(projectId);
  if (!hasData) {
    return false;
  }

  await measureAndReturn({
    operationName: "deleteTracesByProjectId",
    projectId,
    input: {
      params: {
        projectId,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "delete",
        projectId,
      },
    },
    fn: async (input) => {
      const query = `
        DELETE FROM traces
        WHERE project_id = {projectId: String};
      `;

      await commandClickhouse({
        query,
        params: input.params,
        clickhouseConfigs: {
          request_timeout: env.LANGFUSE_CLICKHOUSE_DELETION_TIMEOUT_MS,
        },
        tags: input.tags,
      });
    },
  });

  return true;
};

export const hasAnyUser = async (projectId: string) => {
  if (isDorisBackend()) {
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
  }

  return measureAndReturn({
    operationName: "hasAnyUser",
    projectId,
    input: {
      projectId,
      tags: {
        feature: "tracing",
        type: "user",
        kind: "hasAny",
        projectId,
        operation_name: "hasAnyUser",
      },
    },
    fn: async (input) => {
      const query = `
        SELECT 1
        FROM traces
        WHERE project_id = {projectId: String}
        AND user_id IS NOT NULL
        AND user_id != ''
        LIMIT 1
      `;

      const rows = await queryClickhouse<{ 1: number }>({
        query,
        params: {
          projectId: input.projectId,
        },
        tags: input.tags,
      });

      return rows.length > 0;
    },
  });
};

export const getTotalUserCount = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
): Promise<{ totalCount: bigint }[]> => {
  if (isDorisBackend()) {
    const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
      tracesPrefix: "t",
    });

    tracesFilter.push(
      ...createDorisFilterFromFilterState(filter, tracesTableUiColumnDefinitionsForDoris),
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
  }

  const { tracesFilter } = getProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });

  tracesFilter.push(
    ...createFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitions,
      tracesTableCols,
    ),
  );

  const tracesFilterRes = tracesFilter.apply();
  const search = clickhouseSearchCondition(searchQuery, undefined, "t");

  return measureAndReturn({
    operationName: "getTotalUserCount",
    projectId,
    input: {
      params: {
        ...tracesFilterRes.params,
        ...search.params,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTotalUserCount",
      },
    },
    fn: async (input) => {
      const query = `
        SELECT COUNT(DISTINCT t.user_id) AS totalCount
        FROM traces t
        WHERE ${tracesFilterRes.query}
        ${search.query}
        AND t.user_id IS NOT NULL
        AND t.user_id != ''
      `;

      return queryClickhouse({
        query,
        params: input.params,
        tags: input.tags,
      });
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

  // Helper function to parse timestamps from different backends
  const parseTimestamp = (timestamp: string | Date): Date => {
    // Only apply special handling for Doris backend
    if (isDorisBackend() && timestamp instanceof Date) {
      return timestamp;
    }
    
    // Default ClickHouse behavior - always expect string
    if (typeof timestamp === 'string') {
      return parseClickhouseUTCDateTimeFormat(timestamp);
    }
    
    throw new Error(`Invalid timestamp format: ${typeof timestamp}`);
  };

  if (isDorisBackend()) {
    // Use the same pattern as other methods - get default filter first
    const { tracesFilter } = getDorisProjectIdDefaultFilter(projectId, {
      tracesPrefix: "t",
    });

    tracesFilter.push(
      ...createDorisFilterFromFilterState(filter, tracesTableUiColumnDefinitionsForDoris),
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
              traceTimestamp: convertDateToAnalyticsDateTime(timestampFilter.value),
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
  }

  // filter state contains date range filter for traces so far.
  const chFilter = new FilterList(
    createFilterFromFilterState(
      filter,
      tracesTableUiColumnDefinitions,
      tracesTableCols,
    ),
  );
  const chFilterRes = chFilter.apply();

  const timestampFilter = chFilter.find(
    (f) => f.field === "timestamp" && f.operator === ">=",
  );

  // this query uses window functions on observations + traces to always get only the first row and thereby remove deduplicates
  // we filter wherever possible by project id and user id
  const query = `
      WITH stats as (
        SELECT
            t.user_id as user_id,
            anyLast(t.environment) as environment,
            count(distinct o.id) as obs_count,
            sumMap(usage_details) as sum_usage_details,
            sum(total_cost) as sum_total_cost,
            max(t.timestamp) as max_timestamp,
            min(t.timestamp) as min_timestamp,
            count(distinct t.id) as trace_count
        FROM
            (
                SELECT
                    o.project_id,
                    o.trace_id,
                    o.usage_details,
                    o.total_cost,
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY
                            event_ts DESC
                    ) AS rn
                FROM
                    observations o
                WHERE
                    o.project_id = {projectId: String }
                    ${timestampFilter ? `AND o.start_time >= {traceTimestamp: DateTime64(3)} - ${OBSERVATIONS_TO_TRACE_INTERVAL}` : ""}
                    AND o.trace_id in (
                        SELECT distinct id
                        from __TRACE_TABLE__ t
                        where
                            user_id IN ({userIds: Array(String) })
                            AND project_id = {projectId: String }
                            ${filter.length > 0 ? `AND ${chFilterRes.query}` : ""}
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
                    __TRACE_TABLE__ t FINAL
                WHERE
                    t.user_id IN ({userIds: Array(String) })
                    AND t.project_id = {projectId: String }
                    ${filter.length > 0 ? `AND ${chFilterRes.query}` : ""}
            ) as t on t.id = o.trace_id
            and t.project_id = o.project_id
        WHERE o.rn = 1
        group by t.user_id
    )
    SELECT
        arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, sum_usage_details))) as input_usage,
        arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, sum_usage_details))) as output_usage,
        sum_usage_details [ 'total' ] as total_usage,
        obs_count,
        trace_count,
        user_id,
        environment,
        sum_total_cost,
        max_timestamp,
        min_timestamp
    FROM stats`;

  return measureAndReturn({
    operationName: "getUserMetrics",
    projectId,
    input: {
      params: {
        projectId,
        userIds,
        ...chFilterRes.params,
        ...(timestampFilter
          ? {
              traceTimestamp: convertDateToClickhouseDateTime(
                (timestampFilter as DateTimeFilter).value,
              ),
            }
          : {}),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getUserMetrics",
      },
    },
    fn: async (input) => {
      const rows = await queryClickhouse<{
        user_id: string;
        environment: string;
        max_timestamp: string;
        min_timestamp: string;
        input_usage: string;
        output_usage: string;
        total_usage: string;
        obs_count: string;
        trace_count: string;
        sum_total_cost: string;
      }>({
        query: query.replaceAll("__TRACE_TABLE__", "traces"),
        params: input.params,
        tags: input.tags,
      });

      return rows.map((row) => ({
        userId: row.user_id,
        environment: row.environment,
        maxTimestamp: parseClickhouseUTCDateTimeFormat(row.max_timestamp),
        minTimestamp: parseClickhouseUTCDateTimeFormat(row.min_timestamp),
        inputUsage: Number(row.input_usage),
        outputUsage: Number(row.output_usage),
        totalUsage: Number(row.total_usage),
        observationCount: Number(row.obs_count),
        traceCount: Number(row.trace_count),
        totalCost: Number(row.sum_total_cost),
      }));
    },
  });
};

export const getTracesForBlobStorageExport = function (
  projectId: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  if (isDorisBackend()) {
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
  }

  const traceTable = "traces";

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
      release,
      version,
      public as public,
      bookmarked as bookmarked,
      tags,
      input as input,
      output as output
    FROM ${traceTable} FINAL
    WHERE project_id = {projectId: String}
    AND timestamp >= {minTimestamp: DateTime64(3)}
    AND timestamp <= {maxTimestamp: DateTime64(3)}
  `;

  return queryClickhouseStream<Record<string, unknown>>({
    query,
    params: {
      projectId,
      minTimestamp: convertDateToClickhouseDateTime(minTimestamp),
      maxTimestamp: convertDateToClickhouseDateTime(maxTimestamp),
    },
    tags: {
      feature: "blobstorage",
      type: "trace",
      kind: "analytic",
      projectId,
    },
    clickhouseConfigs: {
      request_timeout: env.LANGFUSE_CLICKHOUSE_DATA_EXPORT_REQUEST_TIMEOUT_MS,
    },
  });
};

export const getTracesForAnalyticsIntegrations = async function* (
  projectId: string,
  projectName: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  if (isDorisBackend()) {
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
        langfuse_tags: record.tags,
        langfuse_event_version: "1.0.0",
        $session_id: record.posthog_session_id ?? null,
        $set: {
          langfuse_user_url: record.user_id
            ? `${baseUrl}/project/${projectId}/users/${encodeURIComponent(record.user_id as string)}`
            : null,
        },
      };
    }
    return;
  }

  // Determine which trace table to use based on experiment flag
  const traceTable = "traces";

  const query = `
    WITH observations_agg AS (
      SELECT o.project_id,
             o.trace_id,
             sum(total_cost) as total_cost,
             count(*) as observation_count,
             date_diff('millisecond', least(min(start_time), min(end_time)), greatest(max(start_time), max(end_time))) as latency_milliseconds
      FROM observations o FINAL
      WHERE o.project_id = {projectId: String}
      AND o.start_time >= {minTimestamp: DateTime64(3)} - ${TRACE_TO_OBSERVATIONS_INTERVAL}
      GROUP BY o.project_id, o.trace_id
    )

    SELECT
      t.id as id,
      t.timestamp as timestamp,
      t.name as name,
      t.session_id as session_id,
      t.user_id as user_id,
      t.release as release,
      t.version as version,
      t.tags as tags,
      t.environment as environment,
      t.metadata['$posthog_session_id'] as posthog_session_id,
      t.metadata['$mixpanel_session_id'] as mixpanel_session_id,
      o.total_cost as total_cost,
      o.latency_milliseconds / 1000 as latency,
      o.observation_count as observation_count
    FROM ${traceTable} t FINAL
    LEFT JOIN observations_agg o ON t.id = o.trace_id AND t.project_id = o.project_id
    WHERE t.project_id = {projectId: String}
    AND t.timestamp >= {minTimestamp: DateTime64(3)}
    AND t.timestamp <= {maxTimestamp: DateTime64(3)}
  `;

  const records = queryClickhouseStream<Record<string, unknown>>({
    query,
    params: {
      projectId,
      minTimestamp: convertDateToClickhouseDateTime(minTimestamp),
      maxTimestamp: convertDateToClickhouseDateTime(maxTimestamp),
    },
    tags: {
      feature: "posthog",
      type: "trace",
      kind: "analytic",
      projectId,
    },
    clickhouseConfigs: {
      request_timeout: env.LANGFUSE_CLICKHOUSE_DATA_EXPORT_REQUEST_TIMEOUT_MS,
      clickhouse_settings: {
        join_algorithm: "grace_hash",
        grace_hash_join_initial_buckets: "32",
      },
    },
  });

  const baseUrl = env.NEXTAUTH_URL?.replace("/api/auth", "");

  for await (const record of records) {
    yield {
      timestamp: record.timestamp,
      langfuse_id: record.id,
      langfuse_trace_name: record.name,
      langfuse_url: `${baseUrl}/project/${projectId}/traces/${encodeURIComponent(record.id as string)}`,
      langfuse_user_url: record.user_id
        ? `${baseUrl}/project/${projectId}/users/${encodeURIComponent(record.user_id as string)}`
        : undefined,
      langfuse_cost_usd: record.total_cost,
      langfuse_count_observations: record.observation_count,
      langfuse_session_id: record.session_id,
      langfuse_project_id: projectId,
      langfuse_project_name: projectName,
      langfuse_user_id: record.user_id || null,
      langfuse_latency: record.latency,
      langfuse_release: record.release,
      langfuse_version: record.version,
      langfuse_tags: record.tags,
      langfuse_environment: record.environment,
      langfuse_event_version: "1.0.0",
      posthog_session_id: record.posthog_session_id ?? null,
      mixpanel_session_id: record.mixpanel_session_id ?? null,
    } satisfies AnalyticsTraceEvent;
  }
};

/**
 * This query is used only for legacy support of redirects without a projectId.
 * We don't have an index on the traceId so it will be a full table scan.
 * We expect at most 10s of calls per day, so this is acceptable.
 */
export const getTracesByIdsForAnyProject = async (traceIds: string[]) => {
  if (isDorisBackend()) {
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
  }

  return measureAndReturn({
    operationName: "getTracesByIdsForAnyProject",
    projectId: "__CROSS_PROJECT__",
    input: {
      params: {
        traceIds,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "list",
        operation_name: "getTracesByIdsForAnyProject",
      },
    },
    fn: async (input) => {
      const query = `
          SELECT id, project_id
          FROM traces
          WHERE id IN ({traceIds: Array(String)})
          ORDER BY event_ts DESC
          LIMIT 1 by id, project_id;`;
      const records = await queryClickhouse<{
        id: string;
        project_id: string;
      }>({
        query,
        params: input.params,
        tags: input.tags,
      });

      return records.map((record) => ({
        id: record.id,
        projectId: record.project_id,
      }));
    },
  });
};

export const traceWithSessionIdExists = async (
  projectId: string,
  sessionId: string,
) => {
  if (isDorisBackend()) {
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
  }

  const query = `
    SELECT id, project_id
    FROM traces
    WHERE session_id = {sessionId: String}
    AND project_id = {projectId: String}
    LIMIT 1
  `;

  const result = await queryClickhouse<{ id: string; project_id: string }>({
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

  if (isDorisBackend()) {
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

  const query = `
          SELECT
            id,
            parent_observation_id,
            type,
            name,
            start_time,
            end_time,
            metadata['langgraph_node'] AS node,
            metadata['langgraph_step'] AS step
          FROM
            observations
          WHERE
            project_id = {projectId: String}
            AND trace_id = {traceId: String}
            AND start_time >= {chMinStartTime: DateTime64(3)}
            AND start_time <= {chMaxStartTime: DateTime64(3)}
        `;

  return queryClickhouse({
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
 * queries against clickhouse. Generous 4x overcompensation before blocking allows
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
    WHERE timestamp >= {startDate: DateTime64(3)}
    AND timestamp < {endDate: DateTime64(3)}
    GROUP BY project_id, toDate(timestamp)
  `;

  const rows = await queryClickhouse<{
    count: string;
    project_id: string;
    date: string;
  }>({
    query,
    params: {
      startDate: convertDateToClickhouseDateTime(startDate),
      endDate: convertDateToClickhouseDateTime(endDate),
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
