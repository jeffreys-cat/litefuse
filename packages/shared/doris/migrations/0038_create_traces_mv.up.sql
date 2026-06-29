-- traces_mv: async materialized view that pre-aggregates events_full (one row
-- per span) into one row per trace, for the trace-list page.
--
-- Design & rationale: docs/trace-list-materialized-view.md. Key points:
--   * Single-table GROUP BY (project_id, trace_id, start_time_date) — no JOIN /
--     window, so Doris can partition-track it and TRANSPARENTLY REWRITE the
--     app's list query (which queries events_full in the same aggregate shape)
--     onto this MV; stale/unrefreshed partitions auto-read base for correctness.
--   * root-level fields via any_value(IF(is_root=1, ...)) (root span is unique
--     per group under MoW UNIQUE KEY; is_root is the precomputed root flag).
--   * Composite metrics (latency, level) are NOT baked as single scalars — that
--     breaks rewrite ("roll up fail"). Instead expose RAW rollup components
--     (MIN/MAX of start/end; per-level SUM(CASE) counts) and let the query
--     compute latency / level priority in its projection.
--   * Aggregates are all-spans (root carries no cost/tokens, so cost/token totals
--     are unaffected; latency = full trace window incl. root).
--   * input/output use the ingestion-truncated input_trim/output_trim (preview
--     only; full-text search runs on base events_full, not the MV).
--
-- Depends on events_full precomputed columns (input_tokens_calculated etc.,
-- input_trim/output_trim) from migration 0037.

CREATE MATERIALIZED VIEW IF NOT EXISTS traces_mv
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 HOUR
PARTITION BY (`timestamp_date`)
DISTRIBUTED BY HASH(`id`) BUCKETS 12
PROPERTIES ("replication_num" = "1")
AS
SELECT
  project_id,
  trace_id AS id,
  start_time_date AS timestamp_date,
  MIN(start_time) AS `timestamp`,
  -- Root-scoped fields use any_value(IF(is_root=1, val, NULL)):
  --   * any_value returns a non-null value if one exists, so the non-root NULLs
  --     are skipped and the root span's value is picked (with one root per group
  --     it is effectively deterministic).
  --   * any_value IS roll-up-able (any_value(any_value())) AND accepts every type
  --     incl. ARRAY/MAP — unlike MAX (no MAP support) and max_by (not roll-up-able)
  --     — so the coarser-grained trace-list query (rolled up across start_time_date)
  --     transparently rewrites onto this MV. Used uniformly for all root-pick
  --     fields (scalars, tags ARRAY, metadata MAP, I/O trim) for consistency;
  --     metadata stays a single native MAP column (no parallel-array zip needed).
  -- Genuine aggregates below (MIN(start_time), SUM, latency min/max, level counts,
  -- MAX(public), MAX(event_ts)) stay MIN/MAX/SUM — they are not root-pick.
  any_value(IF(is_root=1, IF(trace_name<>'',trace_name,name), NULL)) AS name,
  any_value(IF(is_root=1, NULLIF(user_id,''), NULL)) AS user_id,
  any_value(IF(is_root=1, NULLIF(session_id,''), NULL)) AS session_id,
  any_value(IF(is_root=1, NULLIF(`release`,''), NULL)) AS `release`,
  any_value(IF(is_root=1, NULLIF(version,''), NULL)) AS version,
  any_value(IF(is_root=1, NULLIF(environment,''), NULL)) AS environment,
  any_value(IF(is_root=1, bookmarked, NULL)) AS bookmarked,
  MAX(`public`) AS `public`,
  any_value(IF(is_root=1, tags, NULL)) AS tags,
  any_value(IF(is_root=1, metadata, NULL)) AS metadata,
  any_value(IF(is_root=1, input_trim, NULL)) AS input,
  any_value(IF(is_root=1, output_trim, NULL)) AS output,
  SUM(IF(is_root=0,1,0)) AS observations,
  SUM(input_tokens_calculated) AS input_tokens,
  SUM(output_tokens_calculated) AS output_tokens,
  SUM(total_tokens_calculated) AS total_tokens,
  SUM(input_cost_calculated) AS input_cost,
  SUM(output_cost_calculated) AS output_cost,
  SUM(total_cost) AS total_cost,
  -- latency raw components (compute milliseconds_diff in the query projection)
  MAX(start_time) AS start_time_max,
  MIN(end_time)   AS end_time_min,
  MAX(end_time)   AS end_time_max,
  -- level raw counts (derive ERROR>WARNING>DEFAULT>DEBUG priority in query/app)
  SUM(CASE WHEN level='ERROR'   THEN 1 ELSE 0 END) AS error_count,
  SUM(CASE WHEN level='WARNING' THEN 1 ELSE 0 END) AS warning_count,
  SUM(CASE WHEN level='DEFAULT' THEN 1 ELSE 0 END) AS default_count,
  SUM(CASE WHEN level='DEBUG'   THEN 1 ELSE 0 END) AS debug_count,
  MIN(created_at) AS created_at,
  MAX(updated_at) AS updated_at,
  MAX(event_ts) AS event_ts,
  MIN(is_deleted) AS is_deleted
FROM events_full
GROUP BY project_id, trace_id, start_time_date;
