export const DorisTableNames = {
  traces: "traces",
  observations: "observations",
  scores: "scores",
  dataset_run_items_rmt: "dataset_run_items_rmt",
  events_full: "events_full",
  // Root-span scalar mirror + its sync MV (fork additions; both split
  // per-project alongside events_full — see tableRouting).
  traces_scalar: "traces_scalar",
  trace_metrics_agg: "trace_metrics_agg",

  // Virtual tables for dashboards
  // TODO: Check if we can do this more elegantly
  scores_numeric: "scores_numeric",
  scores_categorical: "scores_categorical",
} as const;

export type DorisTableName = keyof typeof DorisTableNames;
