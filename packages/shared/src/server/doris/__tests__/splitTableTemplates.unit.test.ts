import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "fs";
import {
  buildDynamicPartitionTail,
  buildAlterTtlStatement,
  buildSplitTableFromTemplate,
  buildTraceMetricsAggMV,
  SPLIT_BASE_TABLE_SHAPES,
  readSplitTemplate,
  buildSplitTableStatements,
  resolveMigrationsDir,
  NO_TTL_START_DAYS,
  LATE_DATA_HISTORY_DAYS,
} from "../splitTableTemplates";

describe("buildDynamicPartitionTail", () => {
  it("emits dynamic_partition with start = -retentionDays and UTC days", () => {
    const tail = buildDynamicPartitionTail({
      distributionColumn: "trace_id",
      mergeOnWrite: false,
      retentionDays: 30,
      replication: 3,
    });
    expect(tail).toContain("PARTITION BY RANGE(`start_time`) ()");
    // Bucketing delegated to Doris: BUCKETS AUTO + no dynamic_partition.buckets.
    expect(tail).toContain("DISTRIBUTED BY HASH(`trace_id`) BUCKETS AUTO");
    expect(tail).not.toContain("dynamic_partition.buckets");
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
    expect(tail).toContain("DISTRIBUTED BY HASH(`id`) BUCKETS AUTO");
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

describe("buildSplitTableFromTemplate", () => {
  it("substitutes __TABLE__ with the physical name and appends the tail", () => {
    const ddl = buildSplitTableFromTemplate({
      templateSql: readSplitTemplate("events_full"),
      sharedTable: "events_full",
      physicalTable: "events_full_cmqpid",
      tail: { ...SPLIT_BASE_TABLE_SHAPES.events_full, retentionDays: 14, replication: 1 },
    });
    // placeholder replaced everywhere; renamed with IF NOT EXISTS
    expect(ddl).toMatch(/^CREATE TABLE IF NOT EXISTS `events_full_cmqpid`/);
    expect(ddl).not.toContain("__TABLE__");
    // split KEY drops project_id (constant within a split table)
    expect(ddl).toContain("DUPLICATE KEY(`trace_id`, `span_id`)");
    // dynamic_partition tail appended (bucketing delegated: BUCKETS AUTO)
    expect(ddl).toContain("PARTITION BY RANGE(`start_time`) ()");
    expect(ddl).toContain("DISTRIBUTED BY HASH(`trace_id`) BUCKETS AUTO");
    expect(ddl).not.toContain("dynamic_partition.buckets");
    expect(ddl).toContain('"dynamic_partition.start" = "-14"');
  });

  it("throws if the template has no __TABLE__ placeholder", () => {
    expect(() =>
      buildSplitTableFromTemplate({
        templateSql: "CREATE TABLE `events_full` (`x` int) ENGINE=OLAP",
        sharedTable: "events_full",
        physicalTable: "events_full_x",
        tail: { ...SPLIT_BASE_TABLE_SHAPES.events_full, retentionDays: 1, replication: 1 },
      }),
    ).toThrow(/no __TABLE__ placeholder/);
  });
});

describe("buildSplitTableStatements (reads OUR split templates)", () => {
  const PID = "cmqiwxsca0006pj070fdkn0vd";

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
    // split key shape + full column schema flowed through (not a truncated stub)
    expect(eventsFull).toContain("DUPLICATE KEY(`trace_id`, `span_id`)");
    expect(eventsFull).toContain("`experiment_id`");
    // removed columns must NOT be present
    expect(eventsFull).not.toContain("`is_deleted`");
    expect(eventsFull).not.toContain("`event_ts`");
    expect(eventsFull).not.toContain("`updated_at`");

    expect(tracesScalar).toMatch(
      new RegExp("^CREATE TABLE IF NOT EXISTS `traces_scalar_" + PID + "`"),
    );
    expect(tracesScalar).toContain("UNIQUE KEY(`id`, `start_time`)");
    expect(tracesScalar).toContain('"enable_unique_key_merge_on_write" = "true"');
    expect(tracesScalar).not.toContain("`start_time_date`");
    expect(tracesScalar).not.toContain("`event_ts`");
    // traces_scalar is MoW-mutated (bookmark/public/tags) → keeps real updated_at
    expect(tracesScalar).toContain("`updated_at`");

    expect(mv).toContain(`FROM events_full_${PID}`);
    expect(mv).toContain("MAX(created_at) AS tm_max_created_at");
    expect(mv).not.toContain("event_ts");
  });
});

// Drift guard: the split path reads ONLY the CREATE migration / split template.
// If a later migration ALTERs these tables, the single CREATE file would no
// longer be the whole schema and the split tables would silently miss columns.
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
    expect(mv).toContain("MAX(created_at) AS tm_max_created_at");
    expect(mv).toContain("GROUP BY project_id, trace_id, date_trunc(start_time, 'day')");
  });
});
