-- Per-project Doris table-split control table (docs/project-per-table-*.md).
-- One row per project routed to its own events_full_<pid> / traces_scalar_<pid>
-- / trace_metrics_agg_<pid> tables. Table split is universal (every project is
-- designated at creation); isSplitProject consults a cached snapshot of this
-- table and routes a project to its own tables once provisioned (split=true).
CREATE TABLE "doris_project_table_split" (
    "project_id" TEXT NOT NULL,
    "split" BOOLEAN NOT NULL DEFAULT false,
    "note" TEXT,
    "schema_version" INTEGER,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "doris_project_table_split_pkey" PRIMARY KEY ("project_id")
);
