-- events_full_view: read-time view over events_full with GENERATION
-- input resolution against content_dict.
--
-- Layout:
--   - Part 1 selects type='GENERATION' rows with non-null input and
--     re-assembles their hash array into the original ordered content
--     JSON via LATERAL VIEW POSEXPLODE + LEFT JOIN content_dict +
--     GROUP_CONCAT.
--   - Part 2 selects everything else (non-GENERATION rows, plus
--     GENERATION rows with null input) and passes `input` through
--     unchanged.
--   The two branches are UNION ALL'd. Each branch carries a NOT-overlapping
--   WHERE predicate so Doris's predicate pushdown picks the right plan
--   per branch and the union is partition-aligned.
--
-- For type='GENERATION' rows, ingestion stores an ARRAY<VARCHAR> of
-- SHA-256 hashes in events_full.input (deduplicateInputContent in
-- createEventRecord; content_dict entries written alongside, see
-- migration 0035). Part 1's POSEXPLODE + GROUP_CONCAT rebuilds the
-- original content array at read time.
--
-- Note: OTel-only Litefuse Lightweight does NOT write synthetic
-- `t-<trace_id>` trace rows — each OTel span produces exactly one
-- events_full row, with trace-level fields (trace_name, user_id,
-- session_id, tags, release, ...) denormalised onto every row by
-- createEventRecord. There is no trace-span LEFT JOIN here for that
-- reason; trace-level lookups go through getTraceById which aggregates
-- via GROUP BY trace_id.

DROP VIEW IF EXISTS events_full_view;

CREATE VIEW events_full_view AS

-- Part 1: GENERATION rows — resolve input hash array to ordered content
-- array via content_dict join.
SELECT
    o.`project_id`,
    o.`start_time_date`,
    o.`span_id`,
    o.`trace_id`,
    o.`parent_span_id`,
    o.`start_time`,
    o.`end_time`,
    o.`completion_start_time`,
    o.`name`,
    o.`type`,
    o.`environment`,
    o.`version`,
    o.`release`,
    o.`level`,
    o.`status_message`,
    o.`trace_name`,
    o.`user_id`,
    o.`session_id`,
    o.`tags`,
    o.`bookmarked`,
    o.`public`,
    o.`prompt_id`,
    o.`prompt_name`,
    o.`prompt_version`,
    o.`model_id`,
    o.`provided_model_name`,
    o.`model_parameters`,
    o.`provided_usage_details`,
    o.`usage_details`,
    o.`provided_cost_details`,
    o.`cost_details`,
    o.`total_cost`,
    o.`usage_pricing_tier_id`,
    o.`usage_pricing_tier_name`,
    o.`tool_definitions`,
    o.`tool_calls`,
    o.`tool_call_names`,
    CAST(t.`input_resolved` AS VARIANT) AS `input`,
    o.`output`,
    o.`metadata_names`,
    o.`metadata_values`,
    o.`experiment_id`,
    o.`experiment_name`,
    o.`experiment_metadata_names`,
    o.`experiment_metadata_values`,
    o.`experiment_description`,
    o.`experiment_dataset_id`,
    o.`experiment_item_id`,
    o.`experiment_item_version`,
    o.`experiment_item_expected_output`,
    o.`experiment_item_metadata_names`,
    o.`experiment_item_metadata_values`,
    o.`experiment_item_root_span_id`,
    o.`source`,
    o.`service_name`,
    o.`service_version`,
    o.`scope_name`,
    o.`scope_version`,
    o.`telemetry_sdk_language`,
    o.`telemetry_sdk_name`,
    o.`telemetry_sdk_version`,
    o.`blob_storage_file_path`,
    o.`event_bytes`,
    o.`created_at`,
    o.`updated_at`,
    o.`event_ts`,
    o.`is_deleted`
FROM events_full o
LEFT JOIN (
    SELECT
        o_inner.`project_id`,
        o_inner.`start_time_date`,
        o_inner.`span_id`,
        CONCAT(
            '[',
            GROUP_CONCAT(CAST(c.`content` AS VARCHAR) ORDER BY t_inner.`pos`),
            ']'
        ) AS `input_resolved`
    FROM events_full o_inner
    LATERAL VIEW POSEXPLODE(CAST(o_inner.`input` AS ARRAY<VARCHAR>)) t_inner AS `pos`, `hash_item`
    LEFT JOIN content_dict c
        ON t_inner.`hash_item` = c.`content_hash`
       AND o_inner.`start_time_date` = c.`date`
    WHERE o_inner.`type` = 'GENERATION'
      AND o_inner.`input` IS NOT NULL
    GROUP BY o_inner.`project_id`, o_inner.`start_time_date`, o_inner.`span_id`
) t
    ON o.`project_id` = t.`project_id`
   AND o.`start_time_date` = t.`start_time_date`
   AND o.`span_id` = t.`span_id`
WHERE o.`type` = 'GENERATION' AND o.`input` IS NOT NULL

UNION ALL

-- Part 2: non-GENERATION rows OR GENERATION with null input — pass
-- input through unchanged.
SELECT
    o.`project_id`,
    o.`start_time_date`,
    o.`span_id`,
    o.`trace_id`,
    o.`parent_span_id`,
    o.`start_time`,
    o.`end_time`,
    o.`completion_start_time`,
    o.`name`,
    o.`type`,
    o.`environment`,
    o.`version`,
    o.`release`,
    o.`level`,
    o.`status_message`,
    o.`trace_name`,
    o.`user_id`,
    o.`session_id`,
    o.`tags`,
    o.`bookmarked`,
    o.`public`,
    o.`prompt_id`,
    o.`prompt_name`,
    o.`prompt_version`,
    o.`model_id`,
    o.`provided_model_name`,
    o.`model_parameters`,
    o.`provided_usage_details`,
    o.`usage_details`,
    o.`provided_cost_details`,
    o.`cost_details`,
    o.`total_cost`,
    o.`usage_pricing_tier_id`,
    o.`usage_pricing_tier_name`,
    o.`tool_definitions`,
    o.`tool_calls`,
    o.`tool_call_names`,
    o.`input`,
    o.`output`,
    o.`metadata_names`,
    o.`metadata_values`,
    o.`experiment_id`,
    o.`experiment_name`,
    o.`experiment_metadata_names`,
    o.`experiment_metadata_values`,
    o.`experiment_description`,
    o.`experiment_dataset_id`,
    o.`experiment_item_id`,
    o.`experiment_item_version`,
    o.`experiment_item_expected_output`,
    o.`experiment_item_metadata_names`,
    o.`experiment_item_metadata_values`,
    o.`experiment_item_root_span_id`,
    o.`source`,
    o.`service_name`,
    o.`service_version`,
    o.`scope_name`,
    o.`scope_version`,
    o.`telemetry_sdk_language`,
    o.`telemetry_sdk_name`,
    o.`telemetry_sdk_version`,
    o.`blob_storage_file_path`,
    o.`event_bytes`,
    o.`created_at`,
    o.`updated_at`,
    o.`event_ts`,
    o.`is_deleted`
FROM events_full o
WHERE NOT (o.`type` = 'GENERATION' AND o.`input` IS NOT NULL);
