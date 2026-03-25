import { env } from "../../env";
import {
  queryClickhouse,
  queryClickhouseStream,
  commandClickhouse,
  parseClickhouseUTCDateTimeFormat,
} from "./clickhouse";
import {
  queryDoris,
  queryDorisStream,
  commandDoris,
  parseDorisUTCDateTimeFormat,
} from "./doris";
import { logger } from "../logger";

/**
 * Analytics query interface - abstracts between ClickHouse and Doris
 */
export interface AnalyticsQueryOptions {
  query: string;
  params?: Record<string, unknown>;
  tags?: Record<string, string>;
}

/**
 * Query analytics backend (ClickHouse or Doris) based on configuration
 */
export async function queryAnalytics<T>(
  opts: AnalyticsQueryOptions,
): Promise<T[]> {
  const backend = env.LANGFUSE_ANALYTICS_BACKEND;

  switch (backend) {
    case "doris":
      return await queryDoris<T>(opts);
    case "clickhouse":
    default:
      return await queryClickhouse<T>(opts);
  }
}

/**
 * Stream query results from analytics backend
 */
export async function* queryAnalyticsStream<T>(
  opts: AnalyticsQueryOptions,
): AsyncGenerator<T> {
  const backend = env.LANGFUSE_ANALYTICS_BACKEND;

  switch (backend) {
    case "doris":
      yield* queryDorisStream<T>(opts);
      break;
    case "clickhouse":
    default:
      yield* queryClickhouseStream<T>(opts);
      break;
  }
}

/**
 * Parse date format from analytics backend
 */
export function parseAnalyticsDateTimeFormat(dateString: string): Date {
  const backend = env.LANGFUSE_ANALYTICS_BACKEND;

  switch (backend) {
    case "doris":
      return parseDorisUTCDateTimeFormat(dateString);
    case "clickhouse":
    default:
      return parseClickhouseUTCDateTimeFormat(dateString);
  }
}

/**
 * Convert Date to analytics backend DateTime format
 */
export function convertDateToAnalyticsDateTime(date: Date): string {
  const backend = env.LANGFUSE_ANALYTICS_BACKEND;

  // Both Doris and ClickHouse store UTC time
  return date.toISOString().replace("T", " ").replace("Z", "");
}

/**
 * Get the current analytics backend name
 */
export function getAnalyticsBackend(): string {
  return env.LANGFUSE_ANALYTICS_BACKEND || "clickhouse";
}

/**
 * Check if current backend is Doris
 */
export function isDorisBackend(): boolean {
  return getAnalyticsBackend() === "doris";
}

/**
 * Check if current backend is ClickHouse
 */
export function isClickHouseBackend(): boolean {
  return getAnalyticsBackend() === "clickhouse";
}

// Doris reserved words that need backtick quoting
const DORIS_RESERVED = new Set([
  "release",
  "public",
  "user",
  "key",
  "value",
  "index",
  "type",
]);

/**
 * Quote a column name for Doris if it's a reserved word.
 * Returns `col` as-is for non-reserved words, or wraps in backticks.
 */
export function dq(col: string): string {
  return DORIS_RESERVED.has(col.toLowerCase()) ? "`" + col + "`" : col;
}
