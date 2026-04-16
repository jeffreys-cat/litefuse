import { UiColumnMappings } from "../../tableDefinitions";

export const scoresColumnsTableUiColumnDefinitions: UiColumnMappings = [
  // scores native columns
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    tableName: "scores",
    select: "timestamp",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    tableName: "scores",
    select: "s.`session_id`",
  },
  {
    uiTableName: "Dataset Run IDs",
    uiTableId: "datasetRunIds",
    tableName: "scores",
    select: "s.`dataset_run_id`",
  },
  {
    uiTableName: "Observation ID",
    uiTableId: "observationId",
    tableName: "scores",
    select: "s.`observation_id`",
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    tableName: "scores",
    select: "s.`trace_id`",
  },
  // require join of scores with dataset_run_items_rmt via trace_id and project_id
  {
    uiTableName: "Dataset Run Item Run IDs",
    uiTableId: "datasetRunItemRunIds",
    tableName: "dataset_run_items_rmt",
    select: "dri.`dataset_run_id`",
  },
  {
    uiTableName: "Dataset ID",
    uiTableId: "datasetId",
    tableName: "dataset_run_items_rmt",
    select: "dri.`dataset_id`",
  },
  {
    uiTableName: "Dataset Item IDs",
    uiTableId: "datasetItemIds",
    tableName: "dataset_run_items_rmt",
    select: "dri.`dataset_item_id`",
  },
];

// Doris-specific column definitions for scores columns table.
// Uses plain column names without double-quoted identifiers (which Doris doesn't support).
// Note: dataset_run_items_rmt columns are mapped to scores table equivalents since
// the rmt table doesn't exist in Doris - use scores.dataset_run_id directly.
export const scoresColumnsTableUiColumnDefinitionsForDoris: UiColumnMappings = [
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    tableName: "scores",
    select: "timestamp",
    queryPrefix: "s",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    tableName: "scores",
    select: "session_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Dataset Run IDs",
    uiTableId: "datasetRunIds",
    tableName: "scores",
    select: "dataset_run_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Dataset Run Item Run IDs",
    uiTableId: "datasetRunItemRunIds",
    tableName: "scores",
    select: "dataset_run_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Dataset ID",
    uiTableId: "datasetId",
    tableName: "scores",
    select: "dataset_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Dataset Item IDs",
    uiTableId: "datasetItemIds",
    tableName: "scores",
    select: "dataset_item_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Observation ID",
    uiTableId: "observationId",
    tableName: "scores",
    select: "observation_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    tableName: "scores",
    select: "trace_id",
    queryPrefix: "s",
  },
];
