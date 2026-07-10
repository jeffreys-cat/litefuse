-- trace_metrics_agg: realtime per-trace metric rollup, maintained by the
-- ingestion dual-write (IngestionService.writeEventRecord emits ONE increment
-- row per span; Doris's AGGREGATE KEY model folds them on compaction/read).
-- Replaces the traces_mv path for traces.metrics and metric-filtered list
-- queries: the async MTMV can never be fresh on the live partition (refresh =
-- re-aggregating the whole day, billions of spans), while this table is
-- maintained incrementally by the writes themselves — ~2 orders of magnitude
-- fewer rows than events_full (one folded row per trace-day vs one per span).
--
-- Exactly-once / drift model (decided design):
--   * Writer retries are exactly-once via STABLE stream-load labels
--     (DorisWriter.stableLabelTables — this table is the only member; the FE
--     label registry dedups a retry whose earlier attempt actually committed).
--     Verified against Doris 4.0.6: "Label Already Exists" +
--     ExistingJobStatus FINISHED, and aborted txns free their label.
--   * Residual duplication (BullMQ job replay after a worker crash; OTel
--     client re-delivery — there is NO merge layer on the OTel path) can
--     double-apply SUM columns for the CURRENT day only. Accepted: the T+1
--     repair overwrites closed partitions from events_full (MoW = exact
--     ground truth), so history is always exact; MIN/MAX columns are
--     duplication-immune at all times.
--   * NULL semantics verified on 4.0.6: SUM/MIN/MAX all IGNORE NULL inputs,
--     so spans without end_time/tokens simply contribute nothing.
--
-- Ops contracts:
--   * FE label_keep_max_second (default 3 days) must NOT be lowered below the
--     writer's retry window, or label dedup silently expires.
--   * Never hand-craft stream loads with the `langfuse_` label prefix.
--
-- Semantics match traces_mv / MV_INNER_AGG_SELECT (all-span aggregates; the
-- root span carries no tokens/cost so SUMs are unaffected by it; level counts
-- include the root's level exactly once, same as the MV).

CREATE TABLE IF NOT EXISTS trace_metrics_agg (
    `project_id`        varchar(64) NOT NULL,
    `trace_id`          varchar(64) NOT NULL,
    `start_time_date`   Date        NOT NULL,

    -- SUM columns (duplication-sensitive — see drift model above)
    `input_tokens`      BIGINT          SUM NULL,
    `output_tokens`     BIGINT          SUM NULL,
    `total_tokens`      BIGINT          SUM NULL,
    `input_cost`        Decimal(38,12)  SUM NULL,
    `output_cost`       Decimal(38,12)  SUM NULL,
    `total_cost`        Decimal(38,12)  SUM NULL,
    `error_count`       BIGINT          SUM NULL,
    `warning_count`     BIGINT          SUM NULL,
    `default_count`     BIGINT          SUM NULL,
    `debug_count`       BIGINT          SUM NULL,
    `observation_count` BIGINT          SUM NULL,

    -- MIN/MAX columns (idempotent under any duplication; latency & the trace
    -- time window derive from these in the read query)
    `min_start_time`    DateTime(3)     MIN NULL,
    `max_start_time`    DateTime(3)     MAX NULL,
    `min_end_time`      DateTime(3)     MIN NULL,
    `max_end_time`      DateTime(3)     MAX NULL,
    `event_ts`          DateTime(3)     MAX NULL
) ENGINE = OLAP
AGGREGATE KEY(`project_id`, `trace_id`, `start_time_date`)
AUTO PARTITION BY RANGE (date_trunc(`start_time_date`, 'day')) ()
DISTRIBUTED BY HASH(`trace_id`) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- Backfill / T+1 repair statement (run manually for pre-existing data; the
-- repair job runs the same shape per closed partition via INSERT OVERWRITE).
-- events_full is MoW → exact; this recomputes a partition from ground truth.
--
-- INSERT INTO trace_metrics_agg
-- SELECT
--   project_id,
--   trace_id,
--   DATE(start_time) AS start_time_date,
--   SUM(input_tokens_calculated)  AS input_tokens,
--   SUM(output_tokens_calculated) AS output_tokens,
--   SUM(total_tokens_calculated)  AS total_tokens,
--   SUM(input_cost_calculated)    AS input_cost,
--   SUM(output_cost_calculated)   AS output_cost,
--   SUM(total_cost)               AS total_cost,
--   SUM(CASE WHEN level = 'ERROR'   THEN 1 ELSE 0 END) AS error_count,
--   SUM(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
--   SUM(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) AS default_count,
--   SUM(CASE WHEN level = 'DEBUG'   THEN 1 ELSE 0 END) AS debug_count,
--   SUM(IF(is_root = 0, 1, 0))    AS observation_count,
--   MIN(start_time)               AS min_start_time,
--   MAX(start_time)               AS max_start_time,
--   MIN(end_time)                 AS min_end_time,
--   MAX(end_time)                 AS max_end_time,
--   MAX(event_ts)                 AS event_ts
-- FROM events_full
-- WHERE trace_id IS NOT NULL
-- GROUP BY project_id, trace_id, DATE(start_time);
