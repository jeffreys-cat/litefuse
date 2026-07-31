-- Template/schema version the split tables were provisioned at (Stage 1.2c).
ALTER TABLE "doris_project_table_split" ADD COLUMN "schema_version" INTEGER;
