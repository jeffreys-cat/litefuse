-- Retention is now single-sourced on Project.retention_days (read at split-table
-- provisioning / ALTER time). The split control table no longer keeps its own
-- copy — it tracks only provisioned state (split) + schema version.
ALTER TABLE "doris_project_table_split" DROP COLUMN "retention_days";
