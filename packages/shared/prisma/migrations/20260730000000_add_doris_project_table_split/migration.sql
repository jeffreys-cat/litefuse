-- Per-project Doris table-split control table (docs/project-per-table-*.md).
-- One row per project routed to its own events_full_<pid> / traces_scalar_<pid>
-- / trace_metrics_agg_<pid> tables. isSplitProject consults a cached snapshot of
-- this table when LITEFUSE_DORIS_TABLE_SPLIT_MODE = project_id_with_rule.
CREATE TABLE "doris_project_table_split" (
    "project_id" TEXT NOT NULL,
    "split" BOOLEAN NOT NULL DEFAULT false,
    "retention_days" INTEGER,
    "note" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "doris_project_table_split_pkey" PRIMARY KEY ("project_id")
);
