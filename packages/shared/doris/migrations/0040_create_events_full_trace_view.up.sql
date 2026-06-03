-- events_full_trace_view: pass-through SELECT over events_full.
--
-- Sibling of events_full_view (migration 0039) — same column set,
-- but does NOT resolve GENERATION input hash arrays against
-- content_dict. For callers that don't need resolved GENERATION
-- input (list views, filter dropdowns, aggregations, ad-hoc Discover
-- SQL on user_id/session_id), this saves the content_dict LEFT JOIN
-- on every events_full scan.
--
-- The `input` column is passed through AS-IS: for GENERATION rows
-- it's an ARRAY<VARCHAR> of content_dict hashes; for other span
-- types it's the raw value.
--
-- Note: OTel-only Litefuse Lightweight does NOT write synthetic
-- `t-<trace_id>` trace rows — each OTel span produces exactly one
-- events_full row, with trace-level fields denormalised onto every
-- row by createEventRecord. Swapping events_full_view →
-- events_full_trace_view at a non-input-reading call site is a pure
-- perf optimisation; both views expose the same column contract.

DROP VIEW IF EXISTS events_full_trace_view;

CREATE VIEW events_full_trace_view AS
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
FROM events_full o;
