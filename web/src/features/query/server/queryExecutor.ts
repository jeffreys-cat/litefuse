import {
  queryClickhouse,
  measureAndReturn,
  isDorisBackend,
  queryDoris,
} from "@langfuse/shared/src/server";
import { QueryBuilder } from "@/src/features/query/server/queryBuilder";
import { type QueryType, type ViewVersion } from "@/src/features/query/types";
import { getViewDeclaration } from "@/src/features/query/dataModel";
import { env } from "@/src/env.mjs";

// Re-export validation logic (shared between server and client)
export {
  validateQuery,
  type QueryValidationResult,
} from "@/src/features/query/validateQuery";

/**
 * Execute a query using the QueryBuilder.
 *
 * @param projectId - The project ID
 * @param query - The query configuration as defined in QueryType
 * @param version - The view version to use (v1 or v2), defaults to v1
 * @param enableSingleLevelOptimization - Enable single-level SELECT optimization (default: false)
 * @returns The query result data
 */
export async function executeQuery(
  projectId: string,
  query: QueryType,
  version: ViewVersion = "v1",
  enableSingleLevelOptimization: boolean = false,
): Promise<Array<Record<string, unknown>>> {
  // Remap config to chartConfig for public API compatibility
  // Public API uses "config" while internal QueryType uses "chartConfig"
  const chartConfig =
    (query as unknown as { config?: QueryType["chartConfig"] }).config ??
    query.chartConfig;
  const queryBuilder = new QueryBuilder(chartConfig, version);

  // Build the query (with or without optimization based on flag)
  const { query: compiledQuery, parameters } = await queryBuilder.build(
    query,
    projectId,
    enableSingleLevelOptimization,
  );

  const tags = {
    feature: "custom-queries",
    type: query.view,
    kind: "analytic",
    projectId,
  };

  // Route to Doris backend when configured
  if (isDorisBackend()) {
    const rows = await queryDoris<Record<string, unknown>>({
      query: compiledQuery,
      params: parameters,
      tags,
    });

    // Doris mysql2 driver returns Decimal/BigInt as strings and timestamps
    // as Date objects. Convert to match ClickHouse output format for frontend.
    const converted = rows.map((row) => {
      const out: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(row)) {
        if (value instanceof Date) {
          // Convert Date to ClickHouse-style string: "2026-03-20 13:00:00"
          out[key] = value
            .toISOString()
            .replace("T", " ")
            .replace(/\.\d{3}Z$/, "");
        } else if (typeof value === "string" && /^-?\d+(\.\d+)?$/.test(value)) {
          out[key] = Number(value);
        } else {
          out[key] = value;
        }
      }
      return out;
    });

    // Doris doesn't support WITH FILL. Fill time gaps with zeros to match
    // ClickHouse behavior for continuous time series charts.
    if (query.timeDimension && converted.length > 0) {
      return fillTimeSeriesGaps(
        converted,
        query.timeDimension,
        query.fromTimestamp,
        query.toTimestamp,
      );
    }

    return converted;
  }

  // Check if the query contains trace table references
  const usesTraceTable = compiledQuery.includes("traces");

  // Route events_core queries to the dedicated events read replica.
  // Checked via the view declaration's baseCte rather than scanning the compiled SQL.
  const view = getViewDeclaration(query.view, version);
  const preferredClickhouseService = view.baseCte.includes("events_")
    ? ("EventsReadOnly" as const)
    : undefined;

  if (!usesTraceTable) {
    // No trace table placeholders, execute normally
    return queryClickhouse<Record<string, unknown>>({
      query: compiledQuery,
      params: parameters,
      clickhouseConfigs: {
        clickhouse_settings: {
          date_time_output_format: "iso",
          ...(env.CLICKHOUSE_USE_QUERY_CONDITION_CACHE === "true"
            ? { use_query_condition_cache: "true" }
            : {}),
          max_bytes_before_external_group_by: String(
            env.CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY,
          ),
        },
      },
      tags,
      preferredClickhouseService,
    });
  }

  // Use measureAndReturn for trace table queries
  return measureAndReturn({
    operationName: "executeQuery",
    projectId,
    input: {
      query: compiledQuery,
      params: parameters,
      fromTimestamp: query.fromTimestamp,
      tags: {
        ...tags,
        operation_name: "executeQuery",
      },
    },
    fn: async (input) => {
      return queryClickhouse<Record<string, unknown>>({
        query: input.query,
        params: input.params,
        clickhouseConfigs: {
          clickhouse_settings: {
            date_time_output_format: "iso",
            ...(env.CLICKHOUSE_USE_QUERY_CONDITION_CACHE === "true"
              ? { use_query_condition_cache: "true" }
              : {}),
            max_bytes_before_external_group_by: String(
              env.CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY,
            ),
          },
        },
        tags: input.tags,
        preferredClickhouseService,
      });
    },
  });
}

/**
 * Fill time series gaps with zero values for Doris queries.
 * ClickHouse uses WITH FILL natively; Doris needs application-level fill.
 */
function fillTimeSeriesGaps(
  rows: Record<string, unknown>[],
  timeDimension: NonNullable<QueryType["timeDimension"]>,
  fromTimestamp: string,
  toTimestamp: string,
): Record<string, unknown>[] {
  if (rows.length === 0) return rows;

  // Find the time dimension key in the data
  const timeKey = Object.keys(rows[0]!).find((k) => {
    const v = rows[0]![k];
    return (
      typeof v === "string" &&
      /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(v as string)
    );
  });
  if (!timeKey) return rows;

  // Determine granularity
  const granularity =
    timeDimension.granularity === "auto"
      ? determineGranularity(fromTimestamp, toTimestamp)
      : timeDimension.granularity;

  const stepMs: Record<string, number> = {
    minute: 60 * 1000,
    hour: 60 * 60 * 1000,
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000,
    month: 30 * 24 * 60 * 60 * 1000,
  };
  const step = stepMs[granularity];
  if (!step) return rows;

  // Build a set of existing timestamps
  const existingMap = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    existingMap.set(row[timeKey] as string, row);
  }

  // Build zero-fill template from first row
  const zeroTemplate: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rows[0]!)) {
    if (key === timeKey) continue;
    zeroTemplate[key] =
      typeof value === "number"
        ? 0
        : value === null
          ? null
          : typeof value === "string"
            ? ""
            : null;
  }

  // Generate all time buckets
  const truncate = (d: Date): Date => {
    const t = new Date(d);
    switch (granularity) {
      case "minute":
        t.setUTCSeconds(0, 0);
        break;
      case "hour":
        t.setUTCMinutes(0, 0, 0);
        break;
      case "day":
        t.setUTCHours(0, 0, 0, 0);
        break;
      case "week": {
        const day = t.getUTCDay();
        t.setUTCDate(t.getUTCDate() - ((day + 6) % 7));
        t.setUTCHours(0, 0, 0, 0);
        break;
      }
      case "month":
        t.setUTCDate(1);
        t.setUTCHours(0, 0, 0, 0);
        break;
    }
    return t;
  };

  const formatTs = (d: Date): string =>
    d
      .toISOString()
      .replace("T", " ")
      .replace(/\.\d{3}Z$/, "");

  const start = truncate(new Date(fromTimestamp));
  const end = new Date(toTimestamp);
  const result: Record<string, unknown>[] = [];

  for (let t = start; t <= end; t = new Date(t.getTime() + step)) {
    const ts = formatTs(t);
    const existing = existingMap.get(ts);
    if (existing) {
      result.push(existing);
    } else {
      result.push({ ...zeroTemplate, [timeKey]: ts });
    }
  }

  return result;
}

function determineGranularity(
  fromTimestamp: string,
  toTimestamp: string,
): string {
  const diffMs =
    new Date(toTimestamp).getTime() - new Date(fromTimestamp).getTime();
  const diffHours = diffMs / (1000 * 60 * 60);
  if (diffHours < 2) return "minute";
  if (diffHours < 72) return "hour";
  if (diffHours < 1440) return "day";
  if (diffHours < 8760) return "week";
  return "month";
}
