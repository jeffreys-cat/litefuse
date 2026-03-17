import { UiColumnMappings } from "../../tableDefinitions";

export const scoresColumnsTableUiColumnDefinitions: UiColumnMappings = [
  // scores native columns
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    clickhouseTableName: "scores",
    clickhouseSelect: "timestamp",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    clickhouseTableName: "scores",
    clickhouseSelect: 's."session_id"',
  },
  {
    uiTableName: "Dataset Run IDs",
    uiTableId: "datasetRunIds",
    clickhouseTableName: "scores",
    clickhouseSelect: 's."dataset_run_id"',
  },
  {
    uiTableName: "Observation ID",
    uiTableId: "observationId",
    clickhouseTableName: "scores",
    clickhouseSelect: 's."observation_id"',
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    clickhouseTableName: "scores",
    clickhouseSelect: 's."trace_id"',
  },
  // require join of scores with dataset_run_items_rmt via trace_id and project_id
  {
    uiTableName: "Dataset Run Item Run IDs",
    uiTableId: "datasetRunItemRunIds",
    clickhouseTableName: "dataset_run_items_rmt",
    clickhouseSelect: 'dri."dataset_run_id"',
  },
  {
    uiTableName: "Dataset ID",
    uiTableId: "datasetId",
    clickhouseTableName: "dataset_run_items_rmt",
    clickhouseSelect: 'dri."dataset_id"',
  },
  {
    uiTableName: "Dataset Item IDs",
    uiTableId: "datasetItemIds",
    clickhouseTableName: "dataset_run_items_rmt",
    clickhouseSelect: 'dri."dataset_item_id"',
  },
];

// Doris-specific column definitions for scores columns table.
// Uses plain column names without double-quoted identifiers (which Doris doesn't support).
// Excludes dataset_run_items_rmt joins since that table doesn't exist in Doris.
export const scoresColumnsTableUiColumnDefinitionsForDoris: UiColumnMappings = [
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    clickhouseTableName: "scores",
    clickhouseSelect: "timestamp",
    queryPrefix: "s",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    clickhouseTableName: "scores",
    clickhouseSelect: "session_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Dataset Run IDs",
    uiTableId: "datasetRunIds",
    clickhouseTableName: "scores",
    clickhouseSelect: "dataset_run_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Observation ID",
    uiTableId: "observationId",
    clickhouseTableName: "scores",
    clickhouseSelect: "observation_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    clickhouseTableName: "scores",
    clickhouseSelect: "trace_id",
    queryPrefix: "s",
  },
];
