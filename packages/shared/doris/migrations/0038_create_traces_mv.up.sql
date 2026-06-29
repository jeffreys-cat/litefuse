-- traces_mv: async materialized view that pre-aggregates events_full (one row
-- per span) into one row per trace, for the trace-list page.
--
-- Design & rationale: docs/trace-list-materialized-view.md. Key points:
--   * Single-table GROUP BY (project_id, trace_id, start_time_date) — no JOIN /
--     window, so Doris can partition-track it and TRANSPARENTLY REWRITE the
--     app's list query (which queries events_full in the same aggregate shape)
--     onto this MV; stale/unrefreshed partitions auto-read base for correctness.
--   * root-level fields via MAX(IF(parent_span_id='', ...)) (root span is unique
--     per group under MoW UNIQUE KEY).
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
  MAX(IF(parent_span_id='', IF(trace_name<>'',trace_name,name), NULL)) AS name,
  MAX(IF(parent_span_id='', NULLIF(user_id,''), NULL)) AS user_id,
  MAX(IF(parent_span_id='', NULLIF(session_id,''), NULL)) AS session_id,
  MAX(IF(parent_span_id='', NULLIF(`release`,''), NULL)) AS `release`,
  MAX(IF(parent_span_id='', NULLIF(version,''), NULL)) AS version,
  MAX(IF(parent_span_id='', NULLIF(environment,''), NULL)) AS environment,
  MAX(IF(parent_span_id='', bookmarked, NULL)) AS bookmarked,
  MAX(`public`) AS `public`,
  -- tags as a native ARRAY<String> (Doris MAX supports array). MAX(IF(root,...))
  -- picks the root span's tags and rolls up across start_time_date partitions
  -- (MAX is roll-up-able), so the trace-list query transparently rewrites onto
  -- this MV for both display AND filtering (array_contains on the rolled-up tags).
  MAX(IF(parent_span_id='', tags, NULL)) AS tags,
  -- Metadata as the parallel arrays, NOT a Map: Doris MAX does not support Map
  -- (only ARRAY), and max_by(map) — which would keep the native map — is NOT
  -- roll-up-able, so it breaks transparent rewrite for the coarser-grained list
  -- query. Rolling up the two ARRAY<String> columns via MAX(IF(root,...)) keeps
  -- both display (consumers zip names+values back to a map) and per-key filtering
  -- (element_at(values, array_position(names, 'k'))) transparently rewritable.
  MAX(IF(parent_span_id='', metadata_names, NULL)) AS metadata_names,
  MAX(IF(parent_span_id='', metadata_values, NULL)) AS metadata_values,
  MAX(IF(parent_span_id='', input_trim, NULL)) AS input,
  MAX(IF(parent_span_id='', output_trim, NULL)) AS output,
  SUM(IF(parent_span_id<>'',1,0)) AS observations,
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
