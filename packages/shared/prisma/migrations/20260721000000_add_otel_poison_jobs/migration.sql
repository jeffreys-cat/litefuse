-- Poison ledger for the otel exactly-once pipeline (design §3.4 M6): a failed
-- group/file job leaves automatic redrive ONLY after its fileKeys are
-- durably recorded here — otherwise removeOnFail's age-based eviction would
-- eventually evaporate the only pointer to the unprocessed S3 files.
-- Deliberately in Postgres, not Doris: poisoning is most often CAUSED by a
-- Doris outage; the ledger must not share that fate.
CREATE TABLE "otel_poison_jobs" (
    "id" TEXT NOT NULL,
    "shard_name" TEXT NOT NULL,
    "job_id" TEXT NOT NULL,
    "group_id" TEXT,
    "file_keys" JSONB NOT NULL,
    "reason" TEXT NOT NULL,
    "redrives" INTEGER NOT NULL DEFAULT 0,
    "first_seen_at" TIMESTAMP(3) NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "otel_poison_jobs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "otel_poison_jobs_created_at_idx" ON "otel_poison_jobs"("created_at");
