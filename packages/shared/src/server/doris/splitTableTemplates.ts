import { readFileSync, existsSync } from "fs";
import { join } from "path";

/**
 * Per-project Doris table DDL generation (docs/project-per-table-*.md, Stage 1.2).
 *
 * A split project gets its own `events_full_<pid>` / `traces_scalar_<pid>` base
 * tables + a `trace_metrics_agg_<pid>` sync MV. There is no shared telemetry
 * table (all-split model): a split table holds ONE project, so project_id is
 * constant and dropped from the KEY (events_full KEY = (trace_id, span_id);
 * traces_scalar KEY = (id, start_time)). They use dynamic_partition instead of
 * AUTO PARTITION so old day-partitions are auto-dropped at the project's
 * retention (the whole point of the split).
 *
 * SOURCE OF TRUTH = the template files under `doris/migrations` WITHOUT a
 * .up.sql suffix (scripts/up.sh globs *.up.sql, so it never applies them):
 *   - `0037_events_full.sql` / `0039_traces_scalar.sql` — column+index+KEY body
 *     with a __TABLE__ placeholder; the partition/dist/PROPERTIES tail is
 *     appended at build time (buildDynamicPartitionTail).
 *   - `0040_trace_metrics_agg.sql` — the sync MV, __TABLE__ (name) + __BASE_TABLE__
 *     (aggregated base). Keep in sync with the read-side rewrite
 *     (dataModelDoris.traceMetricsAggRelationSql).
 * These are the ONLY schema source — there is no separate shared CREATE
 * migration to drift from.
 */

/** Bump when the generated tail/MV shape changes; recorded in the schema-version gate. */
export const SPLIT_SCHEMA_VERSION = 2;

/** Future day-partitions dynamic_partition pre-creates (clock-skew buffer). */
const DYNAMIC_PARTITION_END = 3;

/**
 * "No TTL" (retentionDays null): a 10-year drop threshold ≈ never drops. We do
 * NOT use AUTO PARTITION for the no-TTL case because you cannot later ALTER a
 * dynamic_partition onto an AUTO table and have it drop the pre-existing
 * partitions (verified on Doris 4.0.6) — so tables are dynamic_partition from
 * creation and TTL is set later via buildAlterTtlStatement.
 */
export const NO_TTL_START_DAYS = 3650;

/**
 * History day-partitions pre-created at table creation (the late-data window).
 * Capped small so provisioning is light regardless of retention — a fresh
 * project has no historical data, this only buys tolerance for slightly-late
 * spans. Data later than this (but still within retention) has no pre-created
 * partition and is rejected; that is rare for near-real-time OTel and the
 * retention filter (Stage 1.6) drops genuinely-old rows upstream anyway.
 */
export const LATE_DATA_HISTORY_DAYS = 7;

/** Split-table DDL template (column+index+KEY body with a __TABLE__ placeholder,
 * split key/order) per logical table — the SINGLE source the per-project CREATE
 * is built from. Lives in doris/migrations WITHOUT a .up.sql suffix, so
 * scripts/up.sh never applies it (it globs *.up.sql): there is no shared table. */
const SPLIT_TEMPLATE_FILE: Record<string, string> = {
  events_full: "0037_events_full.sql",
  traces_scalar: "0039_traces_scalar.sql",
};

/** The trace_metrics_agg sync-MV template (same migrations dir, no .up.sql). */
const MV_TEMPLATE_FILE = "0040_trace_metrics_agg.sql";

/** Placeholder in the templates, replaced with the per-project table name. */
const SPLIT_TABLE_PLACEHOLDER = "__TABLE__";

/** MV-only placeholder for the aggregated base table (events_full_<pid>). */
const MV_BASE_TABLE_PLACEHOLDER = "__BASE_TABLE__";

/** Distribution column + storage model per shared logical table (stable). */
export const SPLIT_BASE_TABLE_SHAPES = {
  events_full: { distributionColumn: "trace_id", mergeOnWrite: false },
  traces_scalar: { distributionColumn: "id", mergeOnWrite: true },
} as const;

export type SplitTailOpts = {
  /** Distribution (bucket) column — trace_id for events_full, id for traces_scalar. */
  distributionColumn: string;
  /** UNIQUE KEY + Merge-on-Write table (traces_scalar) vs DUPLICATE (events_full). */
  mergeOnWrite: boolean;
  /** Days of data to keep (dynamic_partition.start = -retentionDays); null/undefined
   * = no TTL (a 10-year threshold ≈ never drops). Set/changed later via
   * buildAlterTtlStatement. */
  retentionDays?: number | null;
  /** Replication factor (tag.location.default). */
  replication: number;
};

/** dynamic_partition.start (days) + history count for a given retention. */
const partitionWindow = (
  retentionDays: number | null | undefined,
): { startDays: number; historyNum: number } => {
  const startDays = retentionDays ?? NO_TTL_START_DAYS;
  // history_partition_num must not exceed the drop threshold, and stays capped
  // small so provisioning a fresh project is light.
  const historyNum = Math.min(LATE_DATA_HISTORY_DAYS, startDays);
  return { startDays, historyNum };
};

/**
 * The dynamic_partition tail that replaces the shared table's `AUTO PARTITION BY
 * … DISTRIBUTED … PROPERTIES(…)`. Tables are ALWAYS dynamic_partition (never
 * AUTO) so TTL can be applied/changed later by altering dynamic_partition.start;
 * an AUTO table cannot have TTL retro-fitted (its pre-existing partitions are
 * not managed by a later-added dynamic_partition).
 *
 * Bucketing is DELEGATED to Doris: emit `DISTRIBUTED BY HASH(col) BUCKETS AUTO`
 * and set NO dynamic_partition.buckets — no magic number in our DDL. Doris Auto
 * Bucket derives the bucket count from estimate_partition_size (default 10GB)
 * and the cluster; a freshly-created (empty) table's partitions land at the
 * 10-bucket default (observed on 4.0.6 via SHOW PARTITIONS — the test partitions
 * held no data). We let Doris own the sizing rather than pin a number.
 */
export const buildDynamicPartitionTail = (opts: SplitTailOpts): string => {
  const { startDays, historyNum } = partitionWindow(opts.retentionDays);
  const props: Array<[string, string]> = [
    ["replication_allocation", `tag.location.default: ${opts.replication}`],
    ...(opts.mergeOnWrite
      ? ([["enable_unique_key_merge_on_write", "true"]] as [string, string][])
      : []),
    ["dynamic_partition.enable", "true"],
    ["dynamic_partition.time_unit", "DAY"],
    ["dynamic_partition.time_zone", "Etc/UTC"],
    ["dynamic_partition.start", `-${startDays}`],
    ["dynamic_partition.end", String(DYNAMIC_PARTITION_END)],
    ["dynamic_partition.prefix", "p"],
    ["dynamic_partition.create_history_partition", "true"],
    ["dynamic_partition.history_partition_num", String(historyNum)],
  ];
  const propsSql = props.map(([k, v]) => `    "${k}" = "${v}"`).join(",\n");
  return [
    `PARTITION BY RANGE(\`start_time\`) ()`,
    `DISTRIBUTED BY HASH(\`${opts.distributionColumn}\`) BUCKETS AUTO`,
    `PROPERTIES (`,
    propsSql,
    `)`,
  ].join("\n");
};

/**
 * ALTER to set or change a split table's TTL (retention). retentionDays null =
 * remove TTL (back to the 10-year no-drop threshold). The drop of now-expired
 * partitions happens on the dynamic_partition scheduler's next tick
 * (dynamic_partition_check_interval_seconds), not synchronously.
 */
export const buildAlterTtlStatement = (params: {
  physicalTable: string;
  retentionDays?: number | null;
}): string => {
  const { startDays, historyNum } = partitionWindow(params.retentionDays);
  return (
    `ALTER TABLE \`${params.physicalTable}\` SET (` +
    `"dynamic_partition.start" = "-${startDays}", ` +
    `"dynamic_partition.history_partition_num" = "${historyNum}")`
  );
};

/**
 * Build a per-project CREATE from the split template: take the split column +
 * index + KEY body (already in split key/order, __TABLE__ placeholder), swap in
 * the physical table name, and append buildDynamicPartitionTail(opts).
 *
 * @param templateSql   the split template `CREATE TABLE IF NOT EXISTS __TABLE__ (...) ... KEY(...)`
 * @param sharedTable   the shared logical table name it describes (e.g. events_full)
 * @param physicalTable the target per-project name (e.g. events_full_<pid>)
 */
export const buildSplitTableFromTemplate = (params: {
  templateSql: string;
  sharedTable: string;
  physicalTable: string;
  tail: SplitTailOpts;
}): string => {
  const { templateSql, sharedTable, physicalTable, tail } = params;
  if (!templateSql.includes(SPLIT_TABLE_PLACEHOLDER)) {
    throw new Error(
      `buildSplitTableFromTemplate: split template for ${sharedTable} has no ${SPLIT_TABLE_PLACEHOLDER} placeholder`,
    );
  }
  // Drop the leading comment banner — start the statement at CREATE TABLE. The
  // template ends at the KEY line (no partition tail, no trailing ';'), so the
  // rest of the file is the CREATE head verbatim.
  const createStart = templateSql.search(/CREATE TABLE/i);
  if (createStart < 0) {
    throw new Error(
      `buildSplitTableFromTemplate: no CREATE TABLE found in split template for ${sharedTable}`,
    );
  }
  const head = templateSql
    .slice(createStart)
    .split(SPLIT_TABLE_PLACEHOLDER)
    .join(`\`${physicalTable}\``);
  return `${head.trim()}\n${buildDynamicPartitionTail(tail)}`;
};

/**
 * The per-project trace_metrics_agg sync MV, built from the 0040 template file
 * (coupled to dataModelDoris.traceMetricsAggRelationSql — the read-side rewrite
 * shape must match for the transparent rewrite to fire). Only the MV name and
 * base table are per-project; the aggregate shape is identical.
 */
export const buildTraceMetricsAggMV = (params: {
  mvName: string;
  baseTable: string;
}): string => {
  const template = readFileSync(
    join(resolveMigrationsDir(), MV_TEMPLATE_FILE),
    "utf8",
  );
  const start = template.search(/CREATE MATERIALIZED VIEW/i);
  if (start < 0) {
    throw new Error(
      `buildTraceMetricsAggMV: no CREATE MATERIALIZED VIEW in ${MV_TEMPLATE_FILE}`,
    );
  }
  return template
    .slice(start)
    .split(SPLIT_TABLE_PLACEHOLDER)
    .join(params.mvName)
    .split(MV_BASE_TABLE_PLACEHOLDER)
    .join(params.baseTable)
    .trim();
};

/**
 * Locate the shared package's doris/migrations dir. Resolved relative to this
 * module so it works whether loaded from src (vitest) or dist (runtime); the
 * two layouts differ in depth, so we probe candidates. The dir ships in the
 * web/worker images (packages/shared/doris).
 */
export const resolveMigrationsDir = (): string => {
  const candidates = [
    join(__dirname, "../../../../doris/migrations"), // dist/src/server/doris → shared/doris
    join(__dirname, "../../../doris/migrations"), // src/server/doris → shared/doris
  ];
  for (const dir of candidates) {
    if (existsSync(join(dir, "0037_events_full.sql"))) return dir;
  }
  throw new Error(
    `resolveMigrationsDir: doris/migrations not found near ${__dirname} (candidates: ${candidates.join(", ")})`,
  );
};

/** Read a split-table DDL template (with __TABLE__ placeholder) for a table.
 * Templates live in doris/migrations without a .up.sql suffix (up.sh ignores
 * them); they are the single source — there is no shared table to drift from. */
export const readSplitTemplate = (sharedTable: string): string => {
  const file = SPLIT_TEMPLATE_FILE[sharedTable];
  if (!file) {
    throw new Error(`readSplitTemplate: unknown shared table ${sharedTable}`);
  }
  return readFileSync(join(resolveMigrationsDir(), file), "utf8");
};

/**
 * All DDL statements to provision a split project, in apply order:
 * events_full_<pid>, traces_scalar_<pid>, then the MV on events_full_<pid>.
 * Base-table DDL is our split template (split key/order) with a
 * dynamic_partition tail.
 */
export const buildSplitTableStatements = (params: {
  projectId: string;
  /** null/undefined = provision with no TTL; set later via buildAlterTtlStatement. */
  retentionDays?: number | null;
  replication: number;
}): { eventsFull: string; tracesScalar: string; mv: string } => {
  const { projectId, retentionDays, replication } = params;
  const tailBase = { retentionDays, replication };
  const eventsFull = buildSplitTableFromTemplate({
    templateSql: readSplitTemplate("events_full"),
    sharedTable: "events_full",
    physicalTable: `events_full_${projectId}`,
    tail: { ...SPLIT_BASE_TABLE_SHAPES.events_full, ...tailBase },
  });
  const tracesScalar = buildSplitTableFromTemplate({
    templateSql: readSplitTemplate("traces_scalar"),
    sharedTable: "traces_scalar",
    physicalTable: `traces_scalar_${projectId}`,
    tail: { ...SPLIT_BASE_TABLE_SHAPES.traces_scalar, ...tailBase },
  });
  const mv = buildTraceMetricsAggMV({
    mvName: `trace_metrics_agg_${projectId}`,
    baseTable: `events_full_${projectId}`,
  });
  return { eventsFull, tracesScalar, mv };
};
