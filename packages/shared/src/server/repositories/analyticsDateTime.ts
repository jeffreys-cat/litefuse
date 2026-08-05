/**
 * Leaf module (ZERO imports) for the Doris datetime formatter. Extracted from
 * analytics.ts to break the analytics → repositories/doris →
 * doris/parameterProcessor → analytics require cycle: parameterProcessor (and
 * anyone who only needs this) imports here instead of from analytics, so there
 * is no back-edge into the still-initialising analytics module. analytics.ts
 * re-exports it, so existing `import { convertDateToAnalyticsDateTime } from
 * "./analytics"` sites are unchanged.
 */
export function convertDateToAnalyticsDateTime(date: Date): string {
  // Doris stores UTC time.
  return date.toISOString().replace("T", " ").replace("Z", "");
}

// Doris reserved words that need backtick quoting. Lives here (not analytics.ts)
// because dq() is called at MODULE-INIT in several repositories' template-literal
// consts; if it resolved through analytics (which imports repositories/doris), a
// bad load order would read DORIS_RESERVED before analytics finished initialising
// (TDZ under dd-trace's require hook). This leaf has no imports, so dq is always
// ready.
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
