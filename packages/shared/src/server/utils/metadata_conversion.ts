import { parseJsonPrioritised } from "../../utils/json";
import { MetadataDomain } from "../../domain";

/**
 * Doris VARIANT stores dotted metadata keys as nested paths (a key "a.b.c" is
 * stored as {a:{b:{c}}}). Turn a raw metadata filter key into the nested
 * subscript chain the query builder appends after the column, e.g.
 *   "a.b.c" -> `['a']['b']['c']`, "user" -> `['user']`.
 * Each segment is single-quote-escaped for SQL. Used by the query builder and
 * the trace-list metadata filter fast path so per-key filters keep matching the
 * old flattened-key semantics on the VARIANT column.
 */
export function variantMetadataSubscript(rawKey: string): string {
  return rawKey
    .split(".")
    .map((seg) => `['${seg.replace(/'/g, "''")}']`)
    .join("");
}

export function parseMetadataCHRecordToDomain(
  metadata: Record<string, string> | string,
): MetadataDomain {
  if (!metadata) return {};

  // Doris returns metadata as a JSON string, ClickHouse returns it as a Record
  let parsed: Record<string, string>;
  if (typeof metadata === "string") {
    try {
      parsed = JSON.parse(metadata);
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        Array.isArray(parsed)
      ) {
        return {};
      }
    } catch {
      return {};
    }
  } else {
    parsed = metadata;
  }

  return Object.fromEntries(
    Object.entries(parsed).map(([key, val]) => [
      key,
      val === null ? null : parseJsonPrioritised(val),
    ]),
  );
}
