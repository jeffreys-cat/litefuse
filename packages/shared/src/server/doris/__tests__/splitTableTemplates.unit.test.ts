import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "fs";
import {
  buildDynamicPartitionTail,
  buildAlterTtlStatement,
  spliceSplitTableDDL,
  buildTraceMetricsAggMV,
  SPLIT_BASE_TABLE_SHAPES,
  readSharedCreateStatement,
  buildSplitTableStatements,
  resolveMigrationsDir,
  NO_TTL_START_DAYS,
  LATE_DATA_HISTORY_DAYS,
} from "../splitTableTemplates";

// A trimmed but structurally faithful `SHOW CREATE TABLE events_full` — column
// body + KEY, then the AUTO PARTITION / DISTRIBUTED / PROPERTIES tail we replace.
const SHOW_CREATE_EVENTS_FULL = `CREATE TABLE \`events_full\` (
  \`project_id\` varchar(64) NOT NULL,
  \`trace_id\` varchar(64) NULL,
  \`start_time\` datetime(3) NOT NULL,
  \`span_id\` varchar(64) NOT NULL,
  INDEX idx_trace_id (\`trace_id\`) USING INVERTED
) ENGINE=OLAP
DUPLICATE KEY(\`project_id\`, \`trace_id\`, \`start_time\`, \`span_id\`)
AUTO PARTITION BY RANGE (date_trunc(\`start_time\`, 'day')) (
PARTITION p20260715000000 VALUES [('2026-07-15 00:00:00'), ('2026-07-16 00:00:00')))
DISTRIBUTED BY HASH(\`trace_id\`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false"
);`;

describe("buildDynamicPartitionTail", () => {
  it("emits dynamic_partition with start = -retentionDays and UTC days", () => {
    const tail = buildDynamicPartitionTail({
      distributionColumn: "trace_id",
      mergeOnWrite: false,
      retentionDays: 30,
      replication: 3,
    });
    expect(tail).toContain("PARTITION BY RANGE(`start_time`) ()");
    expect(tail).toContain("DISTRIBUTED BY HASH(`trace_id`) BUCKETS 10");
    expect(tail).toContain('"dynamic_partition.enable" = "true"');
    expect(tail).toContain('"dynamic_partition.start" = "-30"');
    expect(tail).toContain('"dynamic_partition.time_zone" = "Etc/UTC"');
    expect(tail).toContain('"dynamic_partition.create_history_partition" = "true"');
    expect(tail).toContain('"replication_allocation" = "tag.location.default: 3"');
    // DUPLICATE table → no MoW property
    expect(tail).not.toContain("enable_unique_key_merge_on_write");
  });

  it("adds merge-on-write for UNIQUE tables (traces_scalar)", () => {
    const tail = buildDynamicPartitionTail({
      distributionColumn: "id",
      mergeOnWrite: true,
      retentionDays: 7,
      replication: 1,
    });
    expect(tail).toContain("DISTRIBUTED BY HASH(`id`) BUCKETS 10");
    expect(tail).toContain('"enable_unique_key_merge_on_write" = "true"');
    expect(tail).toContain('"dynamic_partition.start" = "-7"');
    // history capped at min(7, retention) — here retention 7 → 7
    expect(tail).toContain('"dynamic_partition.history_partition_num" = "7"');
  });

  it("no TTL (retentionDays null) → 10-year threshold + capped history", () => {
    const tail = buildDynamicPartitionTail({
      distributionColumn: "trace_id",
      mergeOnWrite: false,
      retentionDays: null,
      replication: 1,
    });
    expect(tail).toContain(
      `"dynamic_partition.start" = "-${NO_TTL_START_DAYS}"`,
    );
    // NOT thousands of history partitions — capped at the late-data window
    expect(tail).toContain(
      `"dynamic_partition.history_partition_num" = "${LATE_DATA_HISTORY_DAYS}"`,
    );
  });

  it("short retention caps history at the retention (never > drop threshold)", () => {
    const tail = buildDynamicPartitionTail({
      distributionColumn: "trace_id",
      mergeOnWrite: false,
      retentionDays: 3,
      replication: 1,
    });
    expect(tail).toContain('"dynamic_partition.start" = "-3"');
    expect(tail).toContain('"dynamic_partition.history_partition_num" = "3"');
  });
});

describe("buildAlterTtlStatement (set/change TTL later)", () => {
  it("sets dynamic_partition.start to the new retention", () => {
    expect(
      buildAlterTtlStatement({ physicalTable: "events_full_pid", retentionDays: 14 }),
    ).toBe(
      'ALTER TABLE `events_full_pid` SET ("dynamic_partition.start" = "-14", "dynamic_partition.history_partition_num" = "7")',
    );
  });

  it("null retention removes TTL (back to no-drop threshold)", () => {
    const sql = buildAlterTtlStatement({
      physicalTable: "events_full_pid",
      retentionDays: null,
    });
    expect(sql).toContain(`"dynamic_partition.start" = "-${NO_TTL_START_DAYS}"`);
  });
});

describe("spliceSplitTableDDL", () => {
  it("keeps the column/KEY head, renames with IF NOT EXISTS, swaps the tail", () => {
    const ddl = spliceSplitTableDDL({
      createSql: SHOW_CREATE_EVENTS_FULL,
      sharedTable: "events_full",
      physicalTable: "events_full_cmqpid",
      tail: { ...SPLIT_BASE_TABLE_SHAPES.events_full, retentionDays: 14, replication: 1 },
    });
    // renamed with IF NOT EXISTS
    expect(ddl).toMatch(/^CREATE TABLE IF NOT EXISTS `events_full_cmqpid`/);
    // column body + KEY preserved verbatim
    expect(ddl).toContain("`span_id` varchar(64) NOT NULL");
    expect(ddl).toContain(
      "DUPLICATE KEY(`project_id`, `trace_id`, `start_time`, `span_id`)",
    );
    // original AUTO PARTITION + concrete partitions + auto props are GONE
    expect(ddl).not.toContain("AUTO PARTITION");
    expect(ddl).not.toContain("p20260715000000");
    expect(ddl).not.toContain("is_being_synced");
    expect(ddl).not.toContain("BUCKETS AUTO");
    // new dynamic_partition tail present
    expect(ddl).toContain("PARTITION BY RANGE(`start_time`) ()");
    expect(ddl).toContain('"dynamic_partition.start" = "-14"');
  });

  it("throws if the SHOW CREATE has no PARTITION BY clause", () => {
    expect(() =>
      spliceSplitTableDDL({
        createSql: "CREATE TABLE `events_full` (`x` int) ENGINE=OLAP",
        sharedTable: "events_full",
        physicalTable: "events_full_x",
        tail: { ...SPLIT_BASE_TABLE_SHAPES.events_full, retentionDays: 1, replication: 1 },
      }),
    ).toThrow(/no PARTITION BY clause/);
  });

  it("throws if the CREATE TABLE header names a different table", () => {
    expect(() =>
      spliceSplitTableDDL({
        createSql: SHOW_CREATE_EVENTS_FULL,
        sharedTable: "traces_scalar",
        physicalTable: "traces_scalar_x",
        tail: { ...SPLIT_BASE_TABLE_SHAPES.traces_scalar, retentionDays: 1, replication: 1 },
      }),
    ).toThrow(/unexpected CREATE TABLE header/);
  });
});

describe("buildSplitTableStatements (reads OUR canonical .up.sql)", () => {
  const PID = "cmqiwxsca0006pj070fdkn0vd";

  it("reads events_full/traces_scalar CREATE from the migration files", () => {
    const ef = readSharedCreateStatement("events_full");
    expect(ef).toMatch(/^CREATE TABLE\s+if not exists\s+events_full/i);
    // a column that only exists in OUR schema (proves it's our DDL, not a stub)
    expect(ef).toContain("`input_tokens_calculated`");
    const ts = readSharedCreateStatement("traces_scalar");
    expect(ts).toContain("`start_time_date`");
  });

  it("produces the 3 per-project statements with dynamic_partition + MV", () => {
    const { eventsFull, tracesScalar, mv } = buildSplitTableStatements({
      projectId: PID,
      retentionDays: 30,
      replication: 1,
    });
    expect(eventsFull).toMatch(
      new RegExp("^CREATE TABLE IF NOT EXISTS `events_full_" + PID + "`"),
    );
    expect(eventsFull).toContain('"dynamic_partition.start" = "-30"');
    expect(eventsFull).not.toContain("AUTO PARTITION");
    // the full column schema flowed through (not a truncated stub)
    expect(eventsFull).toContain("`experiment_id`");
    expect(tracesScalar).toMatch(
      new RegExp("^CREATE TABLE IF NOT EXISTS `traces_scalar_" + PID + "`"),
    );
    expect(tracesScalar).toContain('"enable_unique_key_merge_on_write" = "true"');
    expect(mv).toContain(`FROM events_full_${PID}`);
  });
});

// Drift guard: buildSplitTableStatements reads ONLY the CREATE migration. If a
// later migration ALTERs these tables, the single CREATE file would no longer be
// the whole schema and the split tables would silently miss columns. Fail here
// so schema changes are made in a way the split path sees.
describe("split-table schema drift guard", () => {
  it("no migration ALTERs events_full/traces_scalar after CREATE", () => {
    const dir = resolveMigrationsDir();
    const offenders: string[] = [];
    for (const f of readdirSync(dir)) {
      if (!f.endsWith(".up.sql")) continue;
      if (f.includes("create_events_full") || f.includes("create_traces_scalar"))
        continue;
      const sql = readFileSync(`${dir}/${f}`, "utf8");
      // strip line comments so the ALTER example inside a comment doesn't trip it
      const code = sql
        .split("\n")
        .filter((l) => !l.trimStart().startsWith("--"))
        .join("\n");
      if (/ALTER\s+TABLE\s+`?(events_full|traces_scalar)`?\b/i.test(code)) {
        offenders.push(f);
      }
    }
    expect(offenders).toEqual([]);
  });
});

describe("buildTraceMetricsAggMV", () => {
  it("names the MV and points FROM at the per-project base table", () => {
    const mv = buildTraceMetricsAggMV({
      mvName: "trace_metrics_agg_cmqpid",
      baseTable: "events_full_cmqpid",
    });
    expect(mv).toMatch(/^CREATE MATERIALIZED VIEW trace_metrics_agg_cmqpid AS/);
    expect(mv).toContain("FROM events_full_cmqpid");
    // aggregate shape (must match migration 0040 / dataModelDoris)
    expect(mv).toContain(
      "SUM(CASE WHEN is_root = 0       THEN 1 ELSE 0 END) AS tm_observation_count",
    );
    expect(mv).toContain("GROUP BY project_id, trace_id, date_trunc(start_time, 'day')");
  });
});
