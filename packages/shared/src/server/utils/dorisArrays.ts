/**
 * Utilities for normalizing Doris ARRAY column values.
 *
 * Doris transmits ARRAY<T> columns over the MySQL protocol as
 * JSON-formatted strings (e.g. '["a","b"]'), not as native arrays —
 * mysql2 does not auto-parse them. Repositories that declare `values:
 * string[]` but read them directly from a Doris ARRAY column will see a
 * string at runtime and crash downstream (`.map is not a function`,
 * `.length` on string, etc).
 *
 * These helpers normalize the value regardless of whether the driver
 * already returned an array or still has a JSON string.
 */

/**
 * Normalize a Doris ARRAY<STRING> (or equivalent) column value to a
 * plain string[]. Empty strings and non-string elements are dropped,
 * because all current callers (trace_ids, user_ids, trace_tags,
 * categorical score values) have no use for them. Malformed JSON or
 * unexpected shapes degrade to [].
 */
export const parseDorisStringArray = (value: unknown): string[] => {
  if (Array.isArray(value)) {
    return value.filter(
      (v): v is string => typeof v === "string" && v.length > 0,
    );
  }
  if (typeof value === "string" && value.length > 0) {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) {
        return parsed.filter(
          (v): v is string => typeof v === "string" && v.length > 0,
        );
      }
    } catch {
      // fall through
    }
  }
  return [];
};
