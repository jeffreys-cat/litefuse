import { readFileSync, existsSync } from "fs";
import { join } from "path";

/**
 * Per-project Doris table DDL generation (docs/project-per-table-*.md, Stage 1.2).
 *
 * A split project gets its own `events_full_<pid>` / `traces_scalar_<pid>` base
 * tables + a `trace_metrics_agg_<pid>` sync MV. These mirror the shared tables'
 * COLUMNS exactly, but differ deliberately in the sort key: a split table holds
 * ONE project, so project_id is constant and dropped from the KEY (events_full
 * KEY = (trace_id, span_id); traces_scalar KEY = (id, start_time)). They also
 * use dynamic_partition instead of AUTO PARTITION so old day-partitions are
 * auto-dropped at the project's retention (the whole point of the split).
 *
 * SOURCE OF TRUTH = OUR version-controlled DDL. Because the split key/column
 * order diverges from the shared table, the split column+index+KEY body is a
 * dedicated template file (`doris/split-templates/<table>.sql`) with a __TABLE__
 * placeholder, NOT spliced out of the shared .up.sql. A drift unit test
 * (splitTableTemplates.drift) asserts the split template and the shared
 * `doris/migrations/*_create_*.up.sql` declare the SAME columns + indexes (only
 * order/KEY/tail differ), so the two files can never silently diverge. Only the
 * partition/dist/PROPERTIES tail, which we own, is appended at build time. A
 * separate test asserts no post-CREATE ALTER migration exists for these tables,
 * so the single CREATE file stays the whole schema
 * (splitTableTemplates.migrations.unit.test).
 *
 * The MV shape is embedded here because it is inherently coupled to the
 * read-side rewrite (dataModelDoris.traceMetricsAggRelationSql) and migration
 * 0040; keep all three in sync.
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

/** Canonical CREATE migration per shared logical table (drift-guard source). */
const SHARED_TABLE_MIGRATION: Record<string, string> = {
  events_full: "0037_create_events_full.up.sql",
  traces_scalar: "0039_create_traces_scalar.up.sql",
};

/** Split-table DDL template (column+index+KEY body with a __TABLE__ placeholder,
 * split key/order) per shared logical table. This is the source the per-project
 * CREATE is built from; SHARED_TABLE_MIGRATION only feeds the drift guard. */
const SPLIT_TEMPLATE_FILE: Record<string, string> = {
  events_full: "events_full.sql",
  traces_scalar: "traces_scalar.sql",
};

/** Placeholder in the split templates, replaced with the per-project table name. */
const SPLIT_TABLE_PLACEHOLDER = "__TABLE__";

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
 * The per-project trace_metrics_agg sync MV, embedded (coupled to migration
 * 0040 + dataModelDoris.traceMetricsAggRelationSql — all three must match for
 * the transparent rewrite to fire). Only the MV name and base table are
 * per-project; the aggregate shape is identical.
 */
export const buildTraceMetricsAggMV = (params: {
  mvName: string;
  baseTable: string;
}): string =>
  `CREATE MATERIALIZED VIEW ${params.mvName} AS
SELECT
    project_id AS tm_project_id,
    trace_id AS tm_trace_id,
    date_trunc(start_time, 'day') AS tm_start_day,
    SUM(input_tokens_calculated) AS tm_input_tokens,
    SUM(output_tokens_calculated) AS tm_output_tokens,
    SUM(total_tokens_calculated) AS tm_total_tokens,
    SUM(input_cost_calculated) AS tm_input_cost,
    SUM(output_cost_calculated) AS tm_output_cost,
    SUM(total_cost) AS tm_total_cost,
    SUM(CASE WHEN level = 'ERROR'   THEN 1 ELSE 0 END) AS tm_error_count,
    SUM(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) AS tm_warning_count,
    SUM(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) AS tm_default_count,
    SUM(CASE WHEN level = 'DEBUG'   THEN 1 ELSE 0 END) AS tm_debug_count,
    SUM(CASE WHEN is_root = 0       THEN 1 ELSE 0 END) AS tm_observation_count,
    MIN(start_time) AS tm_min_start_time,
    MAX(start_time) AS tm_max_start_time,
    MIN(end_time) AS tm_min_end_time,
    MAX(end_time) AS tm_max_end_time,
    MAX(created_at) AS tm_max_created_at
FROM ${params.baseTable}
GROUP BY project_id, trace_id, date_trunc(start_time, 'day')`;

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
    if (existsSync(join(dir, "0037_create_events_full.up.sql"))) return dir;
  }
  throw new Error(
    `resolveMigrationsDir: doris/migrations not found near ${__dirname} (candidates: ${candidates.join(", ")})`,
  );
};

/** Extract the `CREATE TABLE … ;` statement from a migration file (strip the
 * leading comment banner and any trailing commentary after the statement). The
 * terminating `;` is found ignoring `;` inside `--` line comments — the column
 * comments contain prose semicolons (e.g. "trace_id is nullish); a key …"). */
export const extractCreateStatement = (migrationSql: string): string => {
  const start = migrationSql.search(/CREATE TABLE/i);
  if (start < 0)
    throw new Error("extractCreateStatement: no CREATE TABLE found");
  const lines = migrationSql.slice(start).split("\n");
  const out: string[] = [];
  for (const line of lines) {
    // Strip a trailing `--` line comment; a `;` in the remaining code is the
    // real statement terminator (code is a prefix of line, so indices align).
    const code = line.replace(/--.*$/, "");
    const semi = code.indexOf(";");
    if (semi >= 0) {
      out.push(line.slice(0, semi + 1));
      return out.join("\n").trimEnd();
    }
    out.push(line);
  }
  return out.join("\n").trimEnd();
};

/** Read the canonical CREATE statement for a shared table from its migration.
 * Used by the drift guard (compares columns against the split template). */
export const readSharedCreateStatement = (sharedTable: string): string => {
  const file = SHARED_TABLE_MIGRATION[sharedTable];
  if (!file) {
    throw new Error(
      `readSharedCreateStatement: unknown shared table ${sharedTable}`,
    );
  }
  const sql = readFileSync(join(resolveMigrationsDir(), file), "utf8");
  return extractCreateStatement(sql);
};

/**
 * Locate the shared package's doris/split-templates dir. Resolved relative to
 * this module (src via vitest, dist at runtime), same probe strategy as
 * resolveMigrationsDir. The dir ships in the web/worker images alongside
 * doris/migrations (packages/shared/doris).
 */
export const resolveSplitTemplatesDir = (): string => {
  const candidates = [
    join(__dirname, "../../../../doris/split-templates"), // dist/src/server/doris → shared/doris
    join(__dirname, "../../../doris/split-templates"), // src/server/doris → shared/doris
  ];
  for (const dir of candidates) {
    if (existsSync(join(dir, "events_full.sql"))) return dir;
  }
  throw new Error(
    `resolveSplitTemplatesDir: doris/split-templates not found near ${__dirname} (candidates: ${candidates.join(", ")})`,
  );
};

/** Read the split-table DDL template (with __TABLE__ placeholder) for a table. */
export const readSplitTemplate = (sharedTable: string): string => {
  const file = SPLIT_TEMPLATE_FILE[sharedTable];
  if (!file) {
    throw new Error(`readSplitTemplate: unknown shared table ${sharedTable}`);
  }
  return readFileSync(join(resolveSplitTemplatesDir(), file), "utf8");
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
