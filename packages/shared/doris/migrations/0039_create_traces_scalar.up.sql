-- traces_scalar: one row per trace (the root span's scalar fields), dual-written
-- at ingestion alongside events_full (IngestionService.writeEventRecord filters
-- is_root = 1). Serves the trace-list scalar queries (traces.all / countAll) and
-- the verbosity="compact" trace point-read (traces.byId — the trace-list cell
-- preview): every column those return is root-pick, so reads are flat / a
-- single-tablet point lookup — no GROUP BY, no traces_mv aggregate, no
-- MV-staleness compensation.
--
-- Included for the compact byId: input_trim/output_trim — the 200-char
-- ingestion-precomputed previews (~<=800B/row, keeps the table narrow) — and
-- created_at/updated_at (the domain trace object requires them).
--
-- Deliberately NOT here:
--   * full input/output — the giant payloads stay on events_full; content
--     search stays on its full-text indexes; verbosity full/truncated stays on
--     the events_full aggregate.
--   * cost/token/level/observation-count aggregates — those are all-span SUMs
--     that a root-only dual-write cannot know; they remain served by
--     traces_mv / events_full (traces.metrics).
--
-- Semantics aligned with the MV/list expectations at write time (ingestion):
--   * name       ← trace_name (explicit trace name; not the span name)
--   * user_id / session_id / release / version ← NULL when empty string
--     (matches the MV's NULLIF(x, '') so IS NULL / equality filters agree)
--   * latency, when needed, derives from root start_time/end_time (OTel root
--     spans span the whole trace)
--
-- Storage model mirrors events_full: Unique Key + Merge-on-Write. Ingestion
-- always writes the full post-merge root row, so the newest load wins per key.
-- start_time is in the UNIQUE KEY only because the partition column must be a
-- key column; a root span's start_time is fixed by the SDK, so the key is
-- stable across re-writes of the same trace.

CREATE TABLE IF NOT EXISTS traces_scalar (
    `project_id`      varchar(64) NOT NULL,
    `id`              varchar(64),
    `start_time`      DateTime(3) NOT NULL,
    `start_time_date` Date        NOT NULL,
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
    `metadata`        Map<String, String>,
    `input_trim`      String,
    `output_trim`     String,
    `created_at`      DateTime(3),
    `updated_at`      DateTime(3),
    `event_ts`        DateTime(3) NOT NULL,

    INDEX idx_user_id (`user_id`) USING INVERTED COMMENT 'inverted index for user_id',
    INDEX idx_session_id (`session_id`) USING INVERTED COMMENT 'inverted index for session_id',
    INDEX idx_name (`name`) USING INVERTED COMMENT 'inverted index for name (trace name search)',
    INDEX idx_tags (`tags`) USING INVERTED COMMENT 'inverted index for tags',
    INDEX idx_environment (`environment`) USING INVERTED COMMENT 'inverted index for environment'
) ENGINE = OLAP
UNIQUE KEY(`project_id`, `id`, `start_time`)
AUTO PARTITION BY RANGE (date_trunc(`start_time`, 'day')) ()
DISTRIBUTED BY HASH(`id`) BUCKETS 12
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true"
);

-- Backfill for databases that already hold events_full data (new ingestion
-- dual-writes from now on, but historical traces would otherwise be missing
-- from the list). Run manually — deliberately NOT part of the migration, as it
-- scans every root span and can run for minutes on large instances:
--
-- INSERT INTO traces_scalar
-- SELECT
--   project_id,
--   trace_id AS id,
--   start_time,
--   DATE(start_time) AS start_time_date,
--   end_time,
--   NULLIF(trace_name, '') AS name,
--   NULLIF(user_id, '') AS user_id,
--   NULLIF(session_id, '') AS session_id,
--   NULLIF(`release`, '') AS `release`,
--   NULLIF(version, '') AS version,
--   environment,
--   bookmarked,
--   `public`,
--   tags,
--   metadata,
--   input_trim,
--   output_trim,
--   created_at,
--   updated_at,
--   event_ts
-- FROM events_full
-- WHERE is_root = 1 AND trace_id IS NOT NULL;
