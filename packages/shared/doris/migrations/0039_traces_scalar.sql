-- SPLIT TEMPLATE — traces_scalar_<pid> (per-project). NOT a migration: never
-- applied by scripts/up.sh (lives outside doris/migrations, no .up.sql suffix).
-- Read by buildSplitTableStatements, which substitutes __TABLE__ and appends the
-- dynamic_partition tail (buildDynamicPartitionTail; adds MoW for this table).
--
-- Column body is IDENTICAL to the canonical shared table
-- (doris/migrations/0039_create_traces_scalar.up.sql) — the drift unit test
-- asserts parity. The ONLY differences:
--   * table name is a __TABLE__ placeholder;
--   * UNIQUE KEY drops project_id (constant within a split table). Key = (id,
--     start_time); columns reorder so the key is an ordered prefix (project_id
--     moves below start_time). start_time stays in the key (partition column
--     must be a key column under the UNIQUE model);
--   * no partition/dist/PROPERTIES tail here — appended per-project at build time.
-- See 0039 for per-column semantics.

CREATE TABLE IF NOT EXISTS __TABLE__ (
    `id`              varchar(64),
    `start_time`      DateTime(3) NOT NULL,
    `project_id`      varchar(64) NOT NULL,
    `end_time`        DateTime(3),
    `name`            String,
    `user_id`         String,
    `session_id`      String,
    `release`         String,
    `version`         String,
    `environment`     String  DEFAULT 'default',
    `bookmarked`      Boolean DEFAULT 'false',
    `public`          Boolean DEFAULT 'false',
    `tags`            ARRAY<String>,
    `metadata`        Variant,
    `input_trim`      String,
    `output_trim`     String,
    `created_at`      DateTime(3),
    `updated_at`      DateTime(3),

    INDEX idx_user_id (`user_id`) USING INVERTED COMMENT 'inverted index for user_id',
    INDEX idx_session_id (`session_id`) USING INVERTED COMMENT 'inverted index for session_id',
    INDEX idx_name (`name`) USING INVERTED COMMENT 'inverted index for name (trace name search)',
    INDEX idx_tags (`tags`) USING INVERTED COMMENT 'inverted index for tags',
    INDEX idx_environment (`environment`) USING INVERTED COMMENT 'inverted index for environment'
) ENGINE = OLAP
UNIQUE KEY(`id`, `start_time`)
