-- trace_metrics_agg: realtime per-trace metric rollup, now a SYNCHRONOUS
-- materialized view (single-table rollup) on events_full instead of a
-- separately dual-written AGGREGATE KEY table.
--
-- Requires events_full to be a DUPLICATE model table (migration 0037): Doris
-- sync MVs with aggregate functions are only supported on duplicate tables.
-- The MV is maintained atomically by every load transaction on events_full —
-- always fresh, no ingestion dual-write, no stable-label exactly-once
-- machinery, no T+1 repair job. Whatever rows land in events_full are exactly
-- what the rollup reflects (including any duplicate-delivery rows — the
-- duplicate-model tradeoff is owned by the base table, see 0037).
--
-- Query contract: a sync MV cannot be queried by name. Readers aggregate over
-- events_full with EXACTLY the expression shapes below (same aggregate
-- functions over the same expressions, grouped by project_id, trace_id,
-- date_trunc(start_time, 'day')) and Doris transparently rewrites the scan to
-- this rollup. Keep read-side SQL structurally in sync with this definition.
--
-- Column aliases (tm_*) are REQUIRED on Doris 4.0: the FE's create-MV
-- duplicate-name check compares every MV column name against the base
-- table's full schema, and the Nereids CREATE MATERIALIZED VIEW no longer
-- auto-prefixes plain columns (3.x generated mv_* names) — an unaliased key
-- column always fails with "Duplicate column name '<col>'" (verified on
-- 4.0.6-rc02). The alias names are storage-internal only; transparent
-- rewrite matches on expressions, so read queries keep using the base
-- column names.
--
-- Semantics match the previous agg table / traces_mv (all-span aggregates;
-- the root span carries no tokens/cost so SUMs are unaffected by it; level
-- counts include the root's level exactly once; MIN/MAX build the trace time
-- window; observation_count counts non-root spans).
--
-- Note: created on a non-empty table, the rollup build job backfills history
-- automatically in the background (SHOW ALTER TABLE MATERIALIZED VIEW to
-- monitor); transparent rewrite engages once the build finishes.

CREATE MATERIALIZED VIEW trace_metrics_agg AS
SELECT
    project_id AS tm_project_id,
    trace_id AS tm_trace_id,
    date_trunc(start_time, 'day') AS tm_start_day,

    -- SUM metric columns
    SUM(input_tokens_calculated) AS tm_input_tokens,
    SUM(output_tokens_calculated) AS tm_output_tokens,
    SUM(total_tokens_calculated) AS tm_total_tokens,
    SUM(input_cost_calculated) AS tm_input_cost,
    SUM(output_cost_calculated) AS tm_output_cost,
    SUM(total_cost) AS tm_total_cost,
    SUM(CASE WHEN level = 'ERROR'   THEN 1 ELSE 0 END) AS tm_error_count,
    SUM(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) AS tm_warning_count,
    SUM(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) AS tm_default_count,
    SUM(CASE WHEN level = 'DEBUG'   THEN 1 ELSE 0 END) AS tm_debug_count,
    SUM(CASE WHEN is_root = 0       THEN 1 ELSE 0 END) AS tm_observation_count,

    -- MIN/MAX columns (latency & the trace time window derive from these in
    -- the read query)
    MIN(start_time) AS tm_min_start_time,
    MAX(start_time) AS tm_max_start_time,
    MIN(end_time) AS tm_min_end_time,
    MAX(end_time) AS tm_max_end_time,
    MAX(event_ts) AS tm_max_event_ts
FROM events_full
GROUP BY project_id, trace_id, date_trunc(start_time, 'day');
