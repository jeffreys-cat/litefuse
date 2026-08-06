-- trace_metrics_agg: per-project synchronous rollup MV, built per split project.
--
-- TEMPLATE, not a migration: no .up.sql/.down.sql suffix, so scripts/up.sh
-- never applies it (it globs *.up.sql). splitTableTemplates.buildTraceMetricsAggMV
-- reads this file and substitutes:
--   __TABLE__       → trace_metrics_agg_<projectId>   (the MV name)
--   __BASE_TABLE__  → events_full_<projectId>          (the base it aggregates)
-- Keep this in sync with dataModelDoris.traceMetricsAggRelationSql (the read-side
-- transparent-rewrite shape) — the aggregate columns must match for the rewrite
-- to fire.
CREATE MATERIALIZED VIEW __TABLE__ AS
SELECT
    project_id AS tm_project_id,
    trace_id AS tm_trace_id,
    date_trunc(start_time, 'day') AS tm_start_day,
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
    MIN(start_time) AS tm_min_start_time,
    MAX(start_time) AS tm_max_start_time,
    MIN(end_time) AS tm_min_end_time,
    MAX(end_time) AS tm_max_end_time,
    MAX(created_at) AS tm_max_created_at
FROM __BASE_TABLE__
GROUP BY project_id, trace_id, date_trunc(start_time, 'day')
