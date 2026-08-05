-- EO completion ledger, moved from Doris blob_storage_file_log to PG.
-- The Doris incarnation was a per-group 2-row stream load: tablet versions
-- concentrated on the tiny table's few tablets and tripped
-- max_tablet_version_num (E-235) whenever compaction lagged — the trigger of
-- the 2026-07-28 duplicate-data incident. PG absorbs small frequent inserts
-- natively, needs no label (ON CONFLICT semantics via skipDuplicates), and is
-- already on the group job's critical path (trace_sessions upsert).
CREATE TABLE "otel_file_ledger" (
    "id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "file_key" TEXT NOT NULL,
    "group_id" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "otel_file_ledger_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "otel_file_ledger_file_key_group_id_key" ON "otel_file_ledger"("file_key", "group_id");
CREATE INDEX "otel_file_ledger_group_id_idx" ON "otel_file_ledger"("group_id");
CREATE INDEX "otel_file_ledger_created_at_idx" ON "otel_file_ledger"("created_at");
