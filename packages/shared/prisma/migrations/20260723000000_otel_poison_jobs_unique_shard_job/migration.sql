-- Idempotent poison-ledger writes: the DLQ redrive cycle re-classifies a job
-- every 10 minutes while its job.remove() keeps failing transiently. Without
-- a uniqueness guarantee on (shard_name, job_id) each cycle inserted another
-- ledger row for the same job; the writer is now an upsert keyed on this
-- constraint. Pre-existing duplicates (created before this migration) are
-- collapsed to the earliest row first.
DELETE FROM "otel_poison_jobs" a
USING "otel_poison_jobs" b
WHERE a."shard_name" = b."shard_name"
  AND a."job_id" = b."job_id"
  AND a."created_at" > b."created_at";

DELETE FROM "otel_poison_jobs" a
USING "otel_poison_jobs" b
WHERE a."shard_name" = b."shard_name"
  AND a."job_id" = b."job_id"
  AND a."created_at" = b."created_at"
  AND a."id" > b."id";

CREATE UNIQUE INDEX "otel_poison_jobs_shard_name_job_id_key" ON "otel_poison_jobs"("shard_name", "job_id");
