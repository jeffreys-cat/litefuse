-- events_full: unified single table replacing traces + observations.
-- Aligned with langfuse V4 main schema. Each row is a "span" (OTel terminology):
--   * synthetic trace spans use span_id = 't-' || trace_id and parent_span_id = ''
--   * observation spans use the SDK-provided id and may reference parent_observation_id
--
-- Storage model: Duplicate Key (append-only).
--   The OTel write path is a stateless transform — each delivered span is
--   loaded as one full row, no read-back or merge. Without MoW there is no
--   key-level dedup: a re-delivered/replayed span lands as a second identical
--   row. Accepted for this model: normal OTel delivery is once, and the
--   duplicate-key sort columns below only define sort order / prefix seek.
--   In exchange, synchronous (rollup) materialized views become available —
--   trace_metrics_agg (migration 0040) is now a sync MV maintained
--   atomically by every load instead of an ingestion dual-write.
--
-- All non-key columns are nullable or have DEFAULT so that Stream Load can
-- insert rows from arbitrary event payloads.

CREATE TABLE if not exists events_full (
    -- Sort-key identifiers (must be the leading columns, in DUPLICATE KEY
    -- order: trace_id, project_id, span_id). trace_id leads so a trace's spans
    -- co-locate for prefix seek on the trace-detail read; project_id follows for
    -- tenant locality; span_id disambiguates the row. start_time is NOT part of
    -- the key (DUPLICATE model allows a non-key partition column — verified on
    -- 4.0.6) but is the partition source below.
    -- nullable to match the ingestion schema (trace_id is nullish); a key +
    -- distribution column may be NULL in Doris. In practice OTel spans always
    -- carry a trace_id, so co-location holds; rare NULLs share one bucket.
    `trace_id` varchar(64),
    `project_id` varchar(64) NOT NULL,
    `span_id` varchar(64) NOT NULL,

    -- start_time is the partition source. AUTO PARTITION BY date_trunc(start_time,
    -- 'day') lets the optimizer prune partitions natively from ANY start_time
    -- predicate (plain ranges and even DATE(start_time) = x forms — verified on
    -- 4.0.6), which eliminates the whole class of "forgot the start_time_date
    -- mirror predicate → full-partition scan" bugs. NOT NULL because auto
    -- partition rejects NULL partition values (ingestion always supplies it).
    -- Values are UTC wall-clock, so partitions are UTC days. Duplicate model:
    -- replays are NOT folded — a re-exported span appends an identical row (see
    -- storage-model note above).
    `start_time` DateTime(3) NOT NULL,

    -- Span relationships
    `parent_span_id` String,
    -- Root-span flag, set at ingestion (1 when the span is the trace root, i.e.
    -- parent_span_id = ''). Lets root-pick aggregates use any_value(IF(is_root=1,
    -- …)) and root-only scans use WHERE is_root=1 — an integer/indexed predicate
    -- instead of the empty-string `parent_span_id = ''` convention.
    `is_root` TINYINT DEFAULT '0',

    -- Timestamps
    `end_time` DateTime(3),
    `completion_start_time` DateTime(3),

    -- Core span properties
    `name` String,
    `type` varchar(64),
    `environment` String DEFAULT 'default',
    `version` String,
    `release` String,
    `level` String,
    `status_message` String,

    -- Trace-level updatable properties (only meaningful on synthetic trace spans
    -- but kept on every row for column-locality at query time)
    `trace_name` String,
    `user_id` String,
    `session_id` String,
    `tags` ARRAY<String>,
    `bookmarked` Boolean DEFAULT 'false',
    `public` Boolean DEFAULT 'false',

    -- Prompt linkage
    `prompt_id` String,
    `prompt_name` String,
    `prompt_version` int,

    -- Model
    `model_id` String,
    `provided_model_name` String,
    `model_parameters` String,

    -- Usage & Cost (Map types; pricing tier columns added in 0031)
    `provided_usage_details` Map<String, BIGINT>,
    `usage_details` Map<String, BIGINT>,
    `provided_cost_details` Map<String, Decimal(38, 12)>,
    `cost_details` Map<String, Decimal(38, 12)>,
    `total_cost` Decimal(38, 12),
    -- Precomputed UI metrics, derived at ingestion from usage_details/cost_details
    -- via reduceUsageOrCostDetails (input/output = sum of input_*/output_* keys,
    -- incl. cache; total = the 'total' key). Read directly by the UI / aggregated
    -- with SUM(...) by trace, replacing query-time explode_map + array_filter.
    `input_tokens_calculated` BIGINT,
    `output_tokens_calculated` BIGINT,
    `total_tokens_calculated` BIGINT,
    `input_cost_calculated` Decimal(38, 12),
    `output_cost_calculated` Decimal(38, 12),
    `usage_pricing_tier_id` String,
    `usage_pricing_tier_name` String,

    -- Tools
    `tool_definitions` Map<String, String>,
    `tool_calls` ARRAY<String>,
    `tool_call_names` ARRAY<String>,

    -- I/O (Variant: stores arbitrary JSON; queries can use variant_col['path']).
    `input` Variant,
    `output` Variant,
    -- Precomputed compact preview of input/output for list tables (the same
    -- parseIO(.., "compact") the UI applies — ChatML last-message extraction —
    -- truncated to 200 chars at ingestion). Lets list queries show a preview
    -- without lazy-loading / parsing the full Variant per row. Full I/O stays in
    -- input/output above for the detail view.
    `input_trim` String,
    `output_trim` String,

    -- Metadata as a single VARIANT holding the raw (possibly nested) object.
    -- Doris Variant normalizes dotted keys into nested paths on ingest (a literal
    -- key "a.b.c" is stored as {a:{b:{c}}}), so read the whole map via
    -- json_object_flatten(metadata) (yields dot-path keys) and filter per key via
    -- the nested path metadata['a']['b']['c'] (the query builder splits a dotted
    -- key on '.'). Replaces the old metadata_names/metadata_values arrays + Map.
    `metadata` Variant,

    -- Experiment fields (populated by an async backfill job from dataset_run_items_rmt)
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

    -- Source / instrumentation (OTel-derived)
    `source` String,
    `service_name` String,
    `service_version` String,
    `scope_name` String,
    `scope_version` String,
    `telemetry_sdk_language` String,
    `telemetry_sdk_name` String,
    `telemetry_sdk_version` String,

    -- Storage / housekeeping
    `blob_storage_file_path` String,
    `event_bytes` BIGINT,

    -- Single audit timestamp. Ingestion writes created_at == updated_at ==
    -- event_ts (all the same load-time `now`), so one column serves every
    -- audit/ordering need: the trace_metrics_agg MV keys freshness off
    -- MAX(created_at), and convertDorisToDomain maps both createdAt and updatedAt
    -- from it.
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
    -- Un-tokenized indexes for full-text ID search on the UI tables. These
    -- columns are searched via LIKE '%x%' (search.ts); an un-tokenized
    -- inverted index accelerates that substring match, matching the pattern
    -- of idx_user_id / idx_span_id above.
    INDEX idx_name (`name`) USING INVERTED COMMENT 'inverted index for name (trace/observation name search)',
    INDEX idx_trace_name (`trace_name`) USING INVERTED COMMENT 'inverted index for trace_name (trace name search)',
    -- Tokenized full-text indexes for content (input/output) search. parser=unicode
    -- tokenizes CJK by character and Latin by word; support_phrase enables
    -- MATCH_PHRASE. Content search (search.ts) is full-text retrieval: it queries
    -- these columns with `col MATCH_PHRASE 'q'` directly (phrase/token match), not
    -- LIKE substring. As a consequence token matching does not find embedded-word
    -- substrings (search "open" won't find "openai"); this is the intended
    -- text-retrieval behavior.
    INDEX idx_input (`input`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT 'full-text index for input content search',
    INDEX idx_output (`output`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT 'full-text index for output content search'
) ENGINE=OLAP
-- Key leads with trace_id (a trace's spans co-locate → fast trace detail),
-- project_id second (tenant prefix locality — every query filters project_id),
-- span_id last to disambiguate the row. trace_id is also the distribution hash
-- so a trace lands in one bucket while a project spreads across buckets for scan
-- parallelism, unlike the old HASH(project_id) which put a whole project in one
-- bucket.
DUPLICATE KEY(`trace_id`, `project_id`, `span_id`)
AUTO PARTITION BY RANGE (date_trunc(`start_time`, 'day')) ()
-- BUCKETS AUTO: per-deployment data volume is unknown up front, so each new
-- day-partition gets its bucket count sized from the previous partitions'
-- actual data volume instead of a fixed guess (a fixed count either starves
-- large tenants or wastes tablets on small ones). Verified on 4.0.6 together
-- with AUTO PARTITION. Existing deployments migrate forward via
--   ALTER TABLE events_full MODIFY DISTRIBUTION DISTRIBUTED BY HASH(trace_id) BUCKETS AUTO;
-- (applies to newly created partitions; existing partitions keep their count).
DISTRIBUTED BY HASH(`trace_id`) BUCKETS AUTO
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
);
