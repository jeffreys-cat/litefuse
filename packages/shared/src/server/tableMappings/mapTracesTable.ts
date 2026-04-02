import { UiColumnMappings } from "../../tableDefinitions";

export const tracesTableUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "⭐️",
    uiTableId: "bookmarked",
    clickhouseTableName: "traces",
    clickhouseSelect: "t.bookmarked",
  },
  {
    uiTableName: "Level",
    uiTableId: "level",
    clickhouseTableName: "observations",
    clickhouseSelect: "aggregated_level",
    queryPrefix: "o",
  },
  {
    uiTableName: "ID",
    uiTableId: "id",
    clickhouseTableName: "traces",
    clickhouseSelect: "id",
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    clickhouseTableName: "traces",
    clickhouseSelect: "id",
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    clickhouseTableName: "traces",
    clickhouseSelect: "name",
    queryPrefix: "t",
  },
  {
    // Alias for name - allows traceName filter (used in evals) to work on traces table
    // this happens in the v4 beta if someone filters for traceName in beta mode and then switches back to non-beta
    // TODO: remove after beta v4 is concluded
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    clickhouseTableName: "traces",
    clickhouseSelect: "name",
    queryPrefix: "t",
  },
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    clickhouseTableName: "traces",
    clickhouseSelect: "timestamp",
    queryPrefix: "t",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    clickhouseTableName: "traces",
    clickhouseSelect: "user_id",
    queryPrefix: "t",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    clickhouseTableName: "traces",
    clickhouseSelect: "session_id",
    queryPrefix: "t",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "metadata",
    clickhouseTableName: "traces",
    clickhouseSelect: "metadata",
    queryPrefix: "t",
  },
  {
    uiTableName: "Version",
    uiTableId: "version",
    clickhouseTableName: "traces",
    clickhouseSelect: "version",
    queryPrefix: "t",
  },
  {
    uiTableName: "Release",
    uiTableId: "release",
    clickhouseTableName: "traces",
    clickhouseSelect: "`release`",
    queryPrefix: "t",
  },
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    clickhouseTableName: "traces",
    clickhouseSelect: "environment",
    queryPrefix: "t",
  },
  {
    uiTableName: "Tags",
    uiTableId: "tags",
    clickhouseTableName: "traces",
    clickhouseSelect: "tags",
    queryPrefix: "t",
  },
  {
    uiTableName: "Warning Level Count",
    uiTableId: "warningCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "warning_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Error Level Count",
    uiTableId: "errorCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "error_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Default Level Count",
    uiTableId: "defaultCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "default_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Debug Level Count",
    uiTableId: "debugCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "debug_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, o.usage_details)))",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, o.usage_details)))",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "if(mapExists((k, v) -> (k = 'total'), o.usage_details), o.usage_details['total'], NULL)",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens",
    uiTableId: "tokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "if(mapExists((k, v) -> (k = 'total'), o.usage_details), o.usage_details['total'], NULL)",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  // Scores column duplicated to allow renaming column name. Will be removed once session storage cache is outdated
  // Column names are cached in user sessions - changing them breaks existing filters
  {
    uiTableName: "Scores",
    uiTableId: "scores",
    clickhouseTableName: "scores",
    clickhouseSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "scores_avg",
    clickhouseTableName: "scores",
    clickhouseSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "score_categories",
    clickhouseTableName: "scores",
    clickhouseSelect: "s.score_categories",
  },
  {
    uiTableName: "Latency (s)",
    uiTableId: "latency",
    clickhouseTableName: "observations",
    queryPrefix: "o",
    clickhouseSelect: "latency_milliseconds / 1000",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, o.cost_details)))",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, o.cost_details)))",
  },
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    clickhouseTableName: "observations",
    queryPrefix: "o",
    clickhouseSelect: "cost_details['total']",
  },
];

export const tracesTableUiColumnDefinitionsForDoris: UiColumnMappings = [
  {
    uiTableName: "⭐️",
    uiTableId: "bookmarked",
    clickhouseTableName: "traces",
    clickhouseSelect: "bookmarked",
    queryPrefix: "t",
  },
  {
    uiTableName: "Level",
    uiTableId: "level",
    clickhouseTableName: "observations",
    clickhouseSelect: "aggregated_level",
    queryPrefix: "os",
  },
  {
    uiTableName: "ID",
    uiTableId: "id",
    clickhouseTableName: "traces",
    clickhouseSelect: "id",
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    clickhouseTableName: "traces",
    clickhouseSelect: "name",
  },
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    clickhouseTableName: "traces",
    clickhouseSelect: "timestamp",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    clickhouseTableName: "traces",
    clickhouseSelect: "user_id",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    clickhouseTableName: "traces",
    clickhouseSelect: "session_id",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "metadata",
    clickhouseTableName: "traces",
    clickhouseSelect: "metadata",
  },
  {
    uiTableName: "Version",
    uiTableId: "version",
    clickhouseTableName: "traces",
    clickhouseSelect: "version",
  },
  {
    uiTableName: "Release",
    uiTableId: "release",
    clickhouseTableName: "traces",
    clickhouseSelect: "`release`",
  },
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    clickhouseTableName: "traces",
    clickhouseSelect: "environment",
  },
  {
    uiTableName: "Tags",
    uiTableId: "tags",
    clickhouseTableName: "traces",
    clickhouseSelect: "tags",
  },
  {
    uiTableName: "Warning Level Count",
    uiTableId: "warningCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "warning_count",
    queryPrefix: "os",
  },
  {
    uiTableName: "Error Level Count",
    uiTableId: "errorCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "error_count",
    queryPrefix: "os",
  },
  {
    uiTableName: "Default Level Count",
    uiTableId: "defaultCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "default_count",
    queryPrefix: "os",
  },
  {
    uiTableName: "Debug Level Count",
    uiTableId: "debugCount",
    clickhouseTableName: "observations",
    clickhouseSelect: "debug_count",
    queryPrefix: "os",
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "COALESCE(array_sum(array_filter((v, k) -> lower(k) LIKE '%input%', map_values(os.usage_details), map_keys(os.usage_details))), 0)",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "COALESCE(array_sum(array_filter((v, k) -> lower(k) LIKE '%output%', map_values(os.usage_details), map_keys(os.usage_details))), 0)",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "if(MAP_CONTAINS_KEY(os.usage_details, 'total'), os.usage_details['total'], NULL)",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens",
    uiTableId: "tokens",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "if(MAP_CONTAINS_KEY(os.usage_details, 'total'), os.usage_details['total'], NULL)",
    clickhouseTypeOverwrite: "Decimal64(3)",
  },
  // Scores column duplicated to allow renaming column name. Will be removed once session storage cache is outdated
  // Column names are cached in user sessions - changing them breaks existing filters
  {
    uiTableName: "Scores",
    uiTableId: "scores",
    clickhouseTableName: "scores",
    clickhouseSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "scores",
    clickhouseTableName: "scores",
    clickhouseSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "scores",
    clickhouseTableName: "scores",
    clickhouseSelect: "s.score_categories",
  },
  {
    uiTableName: "Latency (s)",
    uiTableId: "latency",
    clickhouseTableName: "observations",
    clickhouseSelect: "latency_milliseconds / 1000",
    clickhouseTypeOverwrite: "Decimal64(3)",
    queryPrefix: "os",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "COALESCE(array_sum(array_filter((v, k) -> lower(k) LIKE '%input%', map_values(os.cost_details), map_keys(os.cost_details))), 0)",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    clickhouseTableName: "observations",
    clickhouseSelect:
      "COALESCE(array_sum(array_filter((v, k) -> lower(k) LIKE '%output%', map_values(os.cost_details), map_keys(os.cost_details))), 0)",
  },
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    clickhouseTableName: "observations",
    clickhouseSelect: "os.cost_details['total']",
  },
];
