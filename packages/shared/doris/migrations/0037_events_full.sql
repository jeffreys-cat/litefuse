-- SPLIT TEMPLATE — events_full_<pid> (per-project). NOT a migration: this file
-- is never applied by scripts/up.sh (it lives outside doris/migrations and has
-- no .up.sql suffix). It is read by buildSplitTableStatements, which substitutes
-- __TABLE__ and appends the dynamic_partition tail (buildDynamicPartitionTail).
--
-- Column body is IDENTICAL to the canonical shared table
-- (doris/migrations/0037_create_events_full.up.sql) — a unit test
-- (splitTableTemplates.drift) asserts the two files declare the same columns and
-- indexes, so they can never silently diverge. The ONLY differences are:
--   * table name is a __TABLE__ placeholder;
--   * DUPLICATE KEY drops project_id — a split table holds ONE project, so
--     project_id is constant and wasted in the sort key. Key = (trace_id,
--     span_id); columns are reordered so the key is an ordered prefix
--     (project_id moves below span_id);
--   * no partition/dist/PROPERTIES tail here — appended per-project at build
--     time (dynamic_partition for retrofittable TTL).
-- See 0037 for the per-column semantics/comments.

CREATE TABLE IF NOT EXISTS __TABLE__ (
    `trace_id` varchar(64),
    `span_id` varchar(64) NOT NULL,
    `project_id` varchar(64) NOT NULL,
    `start_time` DateTime(3) NOT NULL,

    `parent_span_id` String,
    `is_root` TINYINT DEFAULT '0',

    `end_time` DateTime(3),
    `completion_start_time` DateTime(3),

    `name` String,
    `type` varchar(64),
    `environment` String DEFAULT 'default',
    `version` String,
    `release` String,
    `level` String,
    `status_message` String,

    `trace_name` String,
    `user_id` String,
    `session_id` String,
    `tags` ARRAY<String>,
    `bookmarked` Boolean DEFAULT 'false',
    `public` Boolean DEFAULT 'false',

    `prompt_id` String,
    `prompt_name` String,
    `prompt_version` int,

    `model_id` String,
    `provided_model_name` String,
    `model_parameters` String,

    `provided_usage_details` Map<String, BIGINT>,
    `usage_details` Map<String, BIGINT>,
    `provided_cost_details` Map<String, Decimal(38, 12)>,
    `cost_details` Map<String, Decimal(38, 12)>,
    `total_cost` Decimal(38, 12),
    `input_tokens_calculated` BIGINT,
    `output_tokens_calculated` BIGINT,
    `total_tokens_calculated` BIGINT,
    `input_cost_calculated` Decimal(38, 12),
    `output_cost_calculated` Decimal(38, 12),
    `usage_pricing_tier_id` String,
    `usage_pricing_tier_name` String,

    `tool_definitions` Map<String, String>,
    `tool_calls` ARRAY<String>,
    `tool_call_names` ARRAY<String>,

    `input` Variant,
    `output` Variant,
    `input_trim` String,
    `output_trim` String,

    `metadata` Variant,

    `experiment_id` String,
    `experiment_name` String,
    `experiment_metadata` Variant,
    `experiment_description` String,
    `experiment_dataset_id` String,
    `experiment_item_id` String,
    `experiment_item_version` DateTime(3),
    `experiment_item_expected_output` String,
    `experiment_item_metadata` Variant,
    `experiment_item_root_span_id` String,

    `source` String,
    `service_name` String,
    `service_version` String,
    `scope_name` String,
    `scope_version` String,
    `telemetry_sdk_language` String,
    `telemetry_sdk_name` String,
    `telemetry_sdk_version` String,

    `blob_storage_file_path` String,
    `event_bytes` BIGINT,

    `created_at` DateTime(3) DEFAULT CURRENT_TIMESTAMP(3),

    INDEX idx_span_id (`span_id`) USING INVERTED COMMENT 'inverted index for span_id',
    INDEX idx_is_root (`is_root`) USING INVERTED COMMENT 'inverted index for is_root (root-span flag, WHERE is_root=1)',
    INDEX idx_project_id (`project_id`) USING INVERTED COMMENT 'inverted index for project_id',
    INDEX idx_user_id (`user_id`) USING INVERTED COMMENT 'inverted index for user_id',
    INDEX idx_session_id (`session_id`) USING INVERTED COMMENT 'inverted index for session_id',
    INDEX idx_tags (`tags`) USING INVERTED COMMENT 'inverted index for tags',
    INDEX idx_type (`type`) USING INVERTED COMMENT 'inverted index for type',
    INDEX idx_environment (`environment`) USING INVERTED COMMENT 'inverted index for environment',
    INDEX idx_prompt_name (`prompt_name`) USING INVERTED COMMENT 'inverted index for prompt_name',
    INDEX idx_provided_model_name (`provided_model_name`) USING INVERTED COMMENT 'inverted index for provided_model_name',
    INDEX idx_source (`source`) USING INVERTED COMMENT 'inverted index for source (otel/ingestion-api-dual-write)',
    INDEX idx_name (`name`) USING INVERTED COMMENT 'inverted index for name (trace/observation name search)',
    INDEX idx_trace_name (`trace_name`) USING INVERTED COMMENT 'inverted index for trace_name (trace name search)',
    INDEX idx_input (`input`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT 'full-text index for input content search',
    INDEX idx_output (`output`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT 'full-text index for output content search'
) ENGINE=OLAP
DUPLICATE KEY(`trace_id`, `span_id`)
