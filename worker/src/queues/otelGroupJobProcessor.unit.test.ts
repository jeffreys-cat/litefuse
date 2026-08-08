import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import { ZodError } from "zod/v4";

vi.mock("../env", () => ({
  env: {
    LITEFUSE_OTEL_LOAD_CONCURRENCY: 1,
    LITEFUSE_OTEL_TRANSFORM_CONCURRENCY: 1,
    LITEFUSE_OTEL_CONTENT_DICT_BATCH_BYTES: 512,
    LITEFUSE_OTEL_CONTENT_DICT_BATCH_ROWS: 100,
  },
}));

import {
  processOtelGroupJob,
  isDeterministicIngestError,
  type GroupJobDeps,
} from "./otelGroupJobProcessor";
import {
  computeGroupId,
  eventsFullLabelForGroup,
  labelForGroupTable,
  __setSplitSnapshotForTest,
  handleMissingSplitTable,
  getSplitRetentionDays,
  recordHistogram,
  recordIncrement,
  type OtelGroupIngestionEventType,
  type OtelPendingEntryType,
  type StreamLoadBodySource,
} from "@langfuse/shared/src/server";
import { ForbiddenError } from "@langfuse/shared";

// Keep the whole shared barrel real (computeGroupId, labels, split routing,
// __setSplitSnapshotForTest all read/mutate the real module state). Stub PG
// routing and metrics so the processor's lifecycle can be asserted directly.
vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@langfuse/shared/src/server")>();
  return {
    ...actual,
    handleMissingSplitTable: vi.fn(),
    getSplitRetentionDays: vi.fn(async () => Number.MAX_SAFE_INTEGER),
    recordHistogram: vi.fn(),
    recordIncrement: vi.fn(),
  };
});

/**
 * Core-orchestration tests for the self-contained group job — the
 * exactly-once semantics (label, scalar delete-protection gate, ledger
 * ordering, error whitelist) with fully faked deps.
 */

const entry = (fileKey: string): OtelPendingEntryType => ({
  v: 1,
  fileKey,
  size: 1000,
  spanCount: 2,
  ts: Date.now(),
  projectId: "p1",
  publicKey: "pk",
});

const eventRecord = (spanId: string, opts: Record<string, unknown> = {}) =>
  ({
    project_id: "p1",
    trace_id: "t1",
    span_id: spanId,
    start_time: Date.now(),
    event_ts: Date.now(),
    is_root: 0,
    ...opts,
  }) as never;

const payloadFor = (fileKeys: string[]): OtelGroupIngestionEventType => ({
  shape: "group-v1",
  groupId: computeGroupId(fileKeys),
  entries: fileKeys.map(entry),
});

type LoadCall = {
  table: string;
  rows: unknown[];
  count: number;
  options: Record<string, unknown>;
};

const decodeBody = (body: StreamLoadBodySource): unknown[] =>
  Buffer.concat([...body.chunks()])
    .toString("utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((l) => JSON.parse(l));

const makeDeps = (
  overrides: Partial<GroupJobDeps> & {
    dedupedByLabel?: boolean;
    ledger?: boolean;
  } = {},
) => {
  const loads: LoadCall[] = [];
  const deps: GroupJobDeps = {
    downloadFile: vi.fn(async (fileKey: string) => `raw:${fileKey}`),
    transformFile: vi.fn(async (e: OtelPendingEntryType) => ({
      eventRecords: [
        eventRecord(`${e.fileKey}-root`, { is_root: 1, session_id: "s1" }),
        eventRecord(`${e.fileKey}-child`),
      ],
      scalarRecords: [
        {
          project_id: "p1",
          id: "t1",
          start_time: Date.now(),
          event_ts: Date.now(),
        } as never,
      ],
      contentRecords: [],
      sessions: new Map([["s1", "default"]]),
    })),
    streamLoadBody: vi.fn(async (table, body, count, options) => {
      loads.push({ table, rows: decodeBody(body), count, options });
      return {
        dedupedByLabel: table.startsWith("events_full")
          ? (overrides.dedupedByLabel ?? false)
          : false,
      };
    }),
    ledgerExists: vi.fn(async () => overrides.ledger ?? false),
    // PG completion ledger (moved off Doris — the tiny per-group stream
    // loads were the tablet-version incident trigger). Recorded into the
    // same timeline as loads so ordering assertions still work.
    persistLedger: vi.fn(async ({ groupId, entries }) => {
      loads.push({
        table: "pg:otel_file_ledger",
        rows: entries.map((e: OtelPendingEntryType) => ({
          fileKey: e.fileKey,
          projectId: e.projectId,
          groupId,
        })),
        count: entries.length,
        options: {},
      });
    }),
    upsertSessions: vi.fn(async () => {}),
    transformConcurrency: 2,
    ...overrides,
  };
  return { deps, loads };
};

describe("processOtelGroupJob (core EO semantics)", () => {
  // Prime a ready, empty snapshot so the cache-readiness gate passes. Table
  // routing is deterministic all-split and no longer depends on this snapshot.
  beforeEach(() => __setSplitSnapshotForTest([]));
  afterEach(() => __setSplitSnapshotForTest(null));

  it("happy path: label derivation, ndjson framing, no filter ratio, ledger LAST", async () => {
    const payload = payloadFor(["f1.json", "f2.json"]);
    const { deps, loads } = makeDeps();

    await processOtelGroupJob(payload, deps);

    const [events, scalar, ledger] = loads;
    expect(events.table).toBe("events_full_p1");
    expect(events.options.label).toBe(eventsFullLabelForGroup(payload.groupId));
    expect(events.options.format).toBe("json");
    // EO hard rule: silent row-dropping is forbidden.
    expect(events.options).not.toHaveProperty("max_filter_ratio");
    expect(events.rows).toHaveLength(4); // 2 files × 2 records

    // Both Doris loads carry deterministic labels — a group's lifetime FE
    // label-registry footprint is exactly 2 slots regardless of retries
    // (random per-attempt labels flooded the registry and evicted events
    // labels: duplicate-data incident 2026-07-28).
    expect(scalar.table).toBe("traces_scalar_p1");
    expect(scalar.options.label).toBe(
      labelForGroupTable(payload.groupId, "traces_scalar"),
    );

    // Ledger (PG) is written LAST — its existence certifies end-to-end
    // completion.
    expect(ledger.table).toBe("pg:otel_file_ledger");
    expect(ledger.rows).toHaveLength(2);
    expect(ledger.rows[0]).toMatchObject({
      fileKey: "f1.json",
      groupId: payload.groupId,
    });
    expect(deps.upsertSessions).toHaveBeenCalledTimes(1);
  });

  it("flushes content_dict in bounded chunks before events_full", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    const contentStart = new Date().toISOString().slice(0, 10);
    const content = (hash: string, value: string) => ({
      start_time: contentStart,
      content_hash: hash,
      content: value,
    });
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [
        eventRecord("root", {
          is_root: 1,
          input: `${"a".repeat(64)} ${"b".repeat(64)} ${"c".repeat(64)}`,
        }),
      ],
      scalarRecords: [],
      contentRecords: [
        content("a".repeat(64), "a".repeat(100)),
        content("b".repeat(64), "b".repeat(100)),
        content("c".repeat(64), "c".repeat(100)),
      ],
      sessions: new Map(),
    });

    await processOtelGroupJob(payload, deps);

    const contentLoads = loads.filter(
      (load) => load.table === "content_dict_p1",
    );
    expect(contentLoads.map((load) => load.count)).toEqual([2, 1]);
    expect(loads.map((load) => load.table)).toEqual([
      "content_dict_p1",
      "content_dict_p1",
      "events_full_p1",
      "traces_scalar_p1",
      "pg:otel_file_ledger",
    ]);
  });

  it("uses deterministic group-batch labels for content_dict batches", async () => {
    const contentLabelFor = async (hash: string, fileKey: string) => {
      const { deps, loads } = makeDeps();
      deps.transformFile = vi.fn(async () => ({
        eventRecords: [eventRecord("root", { input: hash })],
        contentRecords: [
          {
            start_time: "2026-08-08",
            content_hash: hash,
            content: JSON.stringify([{ role: "user", content: "nested" }]),
          },
        ],
        sessions: new Map(),
      }));

      await processOtelGroupJob(payloadFor([fileKey]), deps);
      return loads.find((load) => load.table === "content_dict_p1")?.options
        .label;
    };

    const first = await contentLabelFor("a".repeat(64), "same.json");
    const replay = await contentLabelFor("a".repeat(64), "same.json");
    const differentGroup = await contentLabelFor("b".repeat(64), "other.json");

    expect(first).toEqual(expect.any(String));
    expect(replay).toBe(first);
    expect(differentGroup).not.toBe(first);
  });

  it("keeps content batch labels stable across concurrent transform ordering", async () => {
    const fileKeys = ["first.json", "second.json"];
    const contentLabelForCompletionOrder = async (
      completionOrder: string[],
    ) => {
      const { deps, loads } = makeDeps();
      deps.transformConcurrency = 2;
      const resolvers = new Map<string, () => void>();
      deps.transformFile = vi.fn(
        (
          entry: OtelPendingEntryType,
          _raw: string,
          onContentRecords: Parameters<GroupJobDeps["transformFile"]>[2],
        ) =>
          new Promise((resolve, reject) => {
            const hash = entry.fileKey === "first.json" ? "a" : "b";
            const contentHash = hash.repeat(64);
            const startTime = Date.parse("2026-08-08T00:00:00.000Z");
            resolvers.set(entry.fileKey, () => {
              if (!onContentRecords) {
                reject(new Error("content sink was not provided"));
                return;
              }
              void onContentRecords(
                [
                  {
                    start_time: "2026-08-08",
                    content_hash: contentHash,
                    content: JSON.stringify([
                      { role: "user", content: `nested-${hash}` },
                    ]),
                  },
                ],
                startTime,
              ).then(
                () =>
                  resolve({
                    eventRecords: [
                      eventRecord(entry.fileKey, {
                        input: contentHash,
                        start_time: startTime,
                        event_ts: startTime,
                      }),
                    ],
                    contentRecords: [],
                    sessions: new Map(),
                  }),
                reject,
              );
            });
          }),
      );

      const job = processOtelGroupJob(payloadFor(fileKeys), deps);
      await vi.waitFor(() => expect(resolvers.size).toBe(fileKeys.length));
      for (const fileKey of completionOrder) resolvers.get(fileKey)?.();
      await job;

      return loads
        .filter((load) => load.table === "content_dict_p1")
        .map((load) => load.options.label);
    };

    const forward = await contentLabelForCompletionOrder(fileKeys);
    const reverse = await contentLabelForCompletionOrder(
      [...fileKeys].reverse(),
    );

    expect(forward).toEqual(reverse);
  });

  it("isolates and measures a content row larger than the batch byte limit", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    vi.mocked(recordIncrement).mockClear();
    vi.mocked(recordHistogram).mockClear();
    const oversizedHash = "a".repeat(64);
    const normalHash = "b".repeat(64);
    const oversizedContent = "x".repeat(600);
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [
        eventRecord("root", {
          is_root: 1,
          input: `${oversizedHash} ${normalHash}`,
        }),
      ],
      contentRecords: [
        {
          start_time: new Date().toISOString().slice(0, 10),
          content_hash: oversizedHash,
          content: oversizedContent,
        },
        {
          start_time: new Date().toISOString().slice(0, 10),
          content_hash: normalHash,
          content: '"small"',
        },
      ],
      sessions: new Map(),
    });

    await processOtelGroupJob(payload, deps);

    const contentLoads = loads.filter(
      (load) => load.table === "content_dict_p1",
    );
    expect(contentLoads.map((load) => load.count)).toEqual([1, 1]);
    expect(contentLoads[0].rows).toEqual([
      expect.objectContaining({ content: oversizedContent }),
    ]);
    expect(recordIncrement).toHaveBeenCalledWith(
      "langfuse.otel_group.oversize_content_entry",
      1,
    );
    expect(recordHistogram).toHaveBeenCalledWith(
      "langfuse.otel_group.oversize_content_entry_bytes",
      expect.any(Number),
    );
  });

  it("deduplicates identical content keys within a batch", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    const hash = "a".repeat(64);
    const startTime = new Date().toISOString().slice(0, 10);
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [eventRecord("root", { input: `${hash} ${hash}` })],
      contentRecords: [
        { start_time: startTime, content_hash: hash, content: '"same"' },
        { start_time: startTime, content_hash: hash, content: '"same"' },
      ],
      sessions: new Map(),
    });

    await processOtelGroupJob(payload, deps);

    const contentLoads = loads.filter(
      (load) => load.table === "content_dict_p1",
    );
    expect(contentLoads).toHaveLength(1);
    expect(contentLoads[0].rows).toHaveLength(1);
  });

  it("does not enter events when a content chunk fails", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps } = makeDeps();
    const hash = "a".repeat(64);
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [eventRecord("root", { input: hash })],
      contentRecords: [
        {
          start_time: new Date().toISOString().slice(0, 10),
          content_hash: hash,
          content: '"will fail"',
        },
      ],
      sessions: new Map(),
    });
    deps.streamLoadBody = vi.fn(async (table) => {
      if (table === "content_dict_p1") throw new Error("content load down");
      return { dedupedByLabel: false };
    });

    await expect(processOtelGroupJob(payload, deps)).rejects.toThrow(
      "content load down",
    );
    expect(
      vi
        .mocked(deps.streamLoadBody)
        .mock.calls.some(([table]) => table === "events_full_p1"),
    ).toBe(false);
  });

  it("dead-letters a tombstoned project on content missing-table without events", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    const hash = "a".repeat(64);
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [eventRecord("root", { input: hash })],
      contentRecords: [
        {
          start_time: new Date().toISOString().slice(0, 10),
          content_hash: hash,
          content: '"tombstoned"',
        },
      ],
      sessions: new Map(),
    });
    vi.mocked(handleMissingSplitTable).mockResolvedValue("skip");
    deps.streamLoadBody = vi.fn(async (table, body, count, options) => {
      if (table === "content_dict_p1") {
        throw new Error("table content_dict_p1 does not exist");
      }
      loads.push({ table, rows: decodeBody(body), count, options });
      return { dedupedByLabel: false };
    });

    await processOtelGroupJob(payload, deps);

    expect(loads.map((load) => load.table)).toEqual(["pg:otel_file_ledger"]);
    vi.mocked(handleMissingSplitTable).mockReset();
  });

  it("passes a backpressured content sink into the transform", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    const contentHash = "a".repeat(64);
    const contentRecord = {
      start_time: new Date().toISOString().slice(0, 10),
      content_hash: contentHash,
      content: '"large input"',
    };
    let sinkCalled = false;
    deps.transformFile = vi.fn(async (_entry, _raw, onContentRecords) => {
      if (!onContentRecords) {
        throw new Error("content sink was not provided to transform");
      }
      await onContentRecords([contentRecord], Date.now());
      sinkCalled = true;
      return {
        eventRecords: [eventRecord("root", { is_root: 1, input: contentHash })],
        contentRecords: [],
        sessions: new Map(),
      };
    });

    await processOtelGroupJob(payload, deps);

    expect(sinkCalled).toBe(true);
    expect(loads[0].table).toBe("content_dict_p1");
    expect(loads[0].rows).toEqual([contentRecord]);
  });

  it("holds transform completion behind a pending content batch", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps } = makeDeps();
    const firstHash = "a".repeat(64);
    const secondHash = "b".repeat(64);
    const startTime = new Date().toISOString().slice(0, 10);
    let transformFinished = false;
    let releaseContentLoad: () => void = () => {
      throw new Error("content load was not started");
    };
    let signalContentLoadStarted!: () => void;
    const contentLoadStarted = new Promise<void>((resolve) => {
      signalContentLoadStarted = resolve;
    });
    deps.transformFile = vi.fn(async (_entry, _raw, onContentRecords) => {
      if (!onContentRecords) throw new Error("content sink was not provided");
      await onContentRecords(
        [
          {
            start_time: startTime,
            content_hash: firstHash,
            content: "a".repeat(300),
          },
        ],
        Date.now(),
      );
      await onContentRecords(
        [
          {
            start_time: startTime,
            content_hash: secondHash,
            content: "b".repeat(300),
          },
        ],
        Date.now(),
      );
      transformFinished = true;
      return {
        eventRecords: [
          eventRecord("root", {
            is_root: 1,
            input: `${firstHash} ${secondHash}`,
          }),
        ],
        contentRecords: [],
        sessions: new Map(),
      };
    });
    let contentLoadCount = 0;
    deps.streamLoadBody = vi.fn(async (table) => {
      if (table === "content_dict_p1" && ++contentLoadCount === 1) {
        signalContentLoadStarted();
        await new Promise<void>((resolve) => {
          releaseContentLoad = resolve;
        });
      }
      return { dedupedByLabel: false };
    });

    const job = processOtelGroupJob(payload, deps);
    await contentLoadStarted;

    expect(transformFinished).toBe(false);
    expect(
      vi
        .mocked(deps.streamLoadBody)
        .mock.calls.some(([table]) => table === "events_full_p1"),
    ).toBe(false);

    releaseContentLoad();
    await job;
    expect(transformFinished).toBe(true);
  });

  it("schedules evals after events_full and before traces_scalar", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps } = makeDeps();
    const order: string[] = [];
    deps.streamLoadBody = vi.fn(async (table) => {
      order.push(table);
      return { dedupedByLabel: false };
    });
    deps.scheduleEvals = vi.fn(async () => {
      order.push("evals");
    });

    await processOtelGroupJob(payload, deps);

    expect(order).toEqual(["events_full_p1", "evals", "traces_scalar_p1"]);
  });

  it("derives scalar rows from event records after events_full", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [
        eventRecord("root", { is_root: 1, trace_id: "derived-trace" }),
      ],
      scalarRecords: [
        {
          project_id: "p1",
          id: "stale-scalar",
          start_time: Date.now(),
          event_ts: Date.now(),
        } as never,
      ],
      contentRecords: [],
      sessions: new Map(),
    });

    await processOtelGroupJob(payload, deps);

    const scalar = loads.find((load) => load.table === "traces_scalar_p1");
    expect(scalar?.rows).toMatchObject([{ id: "derived-trace" }]);
  });

  it("releases event records before traces_scalar begins", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps } = makeDeps();
    const transformedEventRecords = [eventRecord("root", { is_root: 1 })];
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: transformedEventRecords,
      scalarRecords: [
        {
          project_id: "p1",
          id: "stale-scalar",
          start_time: Date.now(),
          event_ts: Date.now(),
        } as never,
      ],
      contentRecords: [],
      sessions: new Map(),
    });
    deps.streamLoadBody = vi.fn(async (table) => {
      if (table === "traces_scalar_p1") {
        expect(transformedEventRecords).toHaveLength(0);
      }
      return { dedupedByLabel: false };
    });

    await processOtelGroupJob(payload, deps);
  });

  it("loads content records produced by the transform before events_full", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    const systemHash = "a".repeat(64);
    const userHash = "b".repeat(64);
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [
        eventRecord("first", {
          start_time: Date.UTC(2026, 6, 28, 12),
          input: `${systemHash} ${userHash}`,
        }),
        eventRecord("second", {
          start_time: Date.UTC(2026, 6, 28, 13),
          input: userHash,
        }),
      ],
      scalarRecords: [],
      contentRecords: [
        {
          start_time: "2026-07-28",
          content_hash: systemHash,
          content: '{"role":"system","content":"You are concise."}',
        },
        {
          start_time: "2026-07-28",
          content_hash: userHash,
          content: '{"role":"user","content":"Hello"}',
        },
      ],
      sessions: new Map(),
    });

    await processOtelGroupJob(payload, deps);

    expect(loads.map((load) => load.table)).toEqual([
      "content_dict_p1",
      "events_full_p1",
      "pg:otel_file_ledger",
    ]);

    const content = loads[0].rows as Array<Record<string, unknown>>;
    expect(content).toHaveLength(2);
    expect(content).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          start_time: "2026-07-28",
          content: '{"role":"system","content":"You are concise."}',
        }),
        expect.objectContaining({
          start_time: "2026-07-28",
          content: '{"role":"user","content":"Hello"}',
        }),
      ]),
    );
    expect(content.every((row) => !("project_id" in row))).toBe(true);
    expect(
      content.every((entry) => typeof entry.content_hash === "string"),
    ).toBe(true);

    const events = loads[1].rows as Array<Record<string, unknown>>;
    expect(events[0].input).toBe(`${systemHash} ${userHash}`);
    expect(events[1].input).toBe(userHash);
  });

  it("keeps an empty input list as an empty events_full text hash list", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    (deps.transformFile as ReturnType<typeof vi.fn>).mockResolvedValue({
      eventRecords: [eventRecord("empty-input", { input: "" })],
      scalarRecords: [],
      contentRecords: [],
      sessions: new Map(),
    });

    await processOtelGroupJob(payload, deps);

    expect(loads.map((load) => load.table)).toEqual([
      "events_full_p1",
      "pg:otel_file_ledger",
    ]);
    expect((loads[0].rows[0] as Record<string, unknown>).input).toBe("");
  });

  it("dedups a fileKey duplicated in the payload", async () => {
    const payload = payloadFor(["f1.json"]);
    payload.entries.push(entry("f1.json")); // defense-in-depth path
    const { deps } = makeDeps();

    await processOtelGroupJob(payload, deps);
    expect(deps.transformFile).toHaveBeenCalledTimes(1);
  });

  it("deterministic transform error dead-letters the FILE, group survives", async () => {
    const payload = payloadFor(["bad.json", "good.json"]);
    const { deps, loads } = makeDeps();
    (deps.transformFile as ReturnType<typeof vi.fn>).mockImplementation(
      async (e: OtelPendingEntryType) => {
        if (e.fileKey === "bad.json") throw new SyntaxError("not json");
        return {
          eventRecords: [eventRecord("ok")],
          scalarRecords: [],
          contentRecords: [],
          sessions: new Map(),
        };
      },
    );

    await processOtelGroupJob(payload, deps);
    const events = loads.find((l) => l.table === "events_full_p1")!;
    expect(events.rows).toHaveLength(1); // only good.json's record
    // Ledger still covers BOTH files (bad one is dead-lettered, not retried).
    const ledger = loads.find((l) => l.table === "pg:otel_file_ledger")!;
    expect(ledger.rows).toHaveLength(2);
  });

  it("transient transform error fails the WHOLE job (replay owns recovery)", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    (deps.transformFile as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("pg connection reset"),
    );

    await expect(processOtelGroupJob(payload, deps)).rejects.toThrow(
      "pg connection reset",
    );
    expect(loads).toHaveLength(0); // nothing loaded, nothing acked
  });

  it("empty group (all dead-lettered) skips loads but still writes the ledger", async () => {
    const payload = payloadFor(["bad.json"]);
    const { deps, loads } = makeDeps();
    (deps.transformFile as ReturnType<typeof vi.fn>).mockRejectedValue(
      new ZodError([]),
    );

    await processOtelGroupJob(payload, deps);
    expect(loads.map((l) => l.table)).toEqual(["pg:otel_file_ledger"]);
  });

  it("C6 replay: label deduped + ledger MISSING → scalar MUST be loaded", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps({ dedupedByLabel: true, ledger: false });

    await processOtelGroupJob(payload, deps);
    // events_full_<pid> was skipped by the FE (dedupedByLabel), but the partial
    // C6 crash (scalar never written) is only distinguishable via the
    // ledger — absent ledger means the scalar load must proceed.
    expect(loads.map((l) => l.table)).toEqual([
      "events_full_p1",
      "traces_scalar_p1",
      "pg:otel_file_ledger",
    ]);
  });

  it("D2/B4 late replay: label deduped + ledger EXISTS → scalar skipped (no resurrection)", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps({ dedupedByLabel: true, ledger: true });

    await processOtelGroupJob(payload, deps);
    expect(loads.map((l) => l.table)).toEqual([
      "events_full_p1",
      "pg:otel_file_ledger",
    ]);
  });

  it("trace_sessions failure fails the job (idempotent replay owns it)", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps } = makeDeps();
    (deps.upsertSessions as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("pg down"),
    );

    await expect(processOtelGroupJob(payload, deps)).rejects.toThrow("pg down");
  });

  it("download failure fails the job", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps } = makeDeps();
    (deps.downloadFile as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("s3 503"),
    );

    await expect(processOtelGroupJob(payload, deps)).rejects.toThrow(
      /download failed/,
    );
  });
});

describe("isDeterministicIngestError", () => {
  it("whitelists parse/schema/forbidden; rejects everything else", () => {
    expect(isDeterministicIngestError(new SyntaxError("x"))).toBe(true);
    expect(isDeterministicIngestError(new ZodError([]))).toBe(true);
    expect(isDeterministicIngestError(new ForbiddenError("gone"))).toBe(true);
    expect(isDeterministicIngestError(new Error("ECONNRESET"))).toBe(false);
    expect(isDeterministicIngestError("string error")).toBe(false);
  });
});

// Split-target routing + retention filter. Table split is universal now, so
// these run in the normal suite: prime the cache snapshot with a LIVE project
// (split=true) and its loads route to the per-project tables. Retention is read
// from PG via the mocked getSplitRetentionDays (default: effectively no TTL).
describe("processOtelGroupJob (split targets)", () => {
  afterEach(() => {
    __setSplitSnapshotForTest(null); // never leak into other tests
    vi.mocked(getSplitRetentionDays).mockResolvedValue(Number.MAX_SAFE_INTEGER);
  });

  it("routes a split project's loads to events_full_<pid>/traces_scalar_<pid>", async () => {
    __setSplitSnapshotForTest([["p1", true]]);
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    await processOtelGroupJob(payload, deps);
    const tables = loads
      .filter((l) => l.table !== "pg:otel_file_ledger")
      .map((l) => l.table);
    expect(tables).toContain("events_full_p1");
    expect(tables).toContain("traces_scalar_p1");
    expect(tables).not.toContain("events_full");
  });

  it("drops over-retention rows before the split load (row dead-letter)", async () => {
    __setSplitSnapshotForTest([["p1", true]]);
    vi.mocked(getSplitRetentionDays).mockResolvedValue(7); // 7d retention
    vi.mocked(recordIncrement).mockClear();
    const payload = payloadFor(["f1.json"]);
    const oldTs = Date.now() - 30 * 86_400_000; // 30d old, retention 7d
    const nowTs = Date.now();
    const recentHash = "a".repeat(64);
    const oldHash = "b".repeat(64);
    const { deps, loads } = makeDeps({
      transformFile: vi.fn(async () => ({
        eventRecords: [
          eventRecord("recent", { start_time: nowTs, input: recentHash }),
          eventRecord("old", { start_time: oldTs, input: oldHash }),
        ],
        scalarRecords: [],
        contentRecords: [
          {
            start_time: new Date(nowTs).toISOString().slice(0, 10),
            content_hash: recentHash,
            content: '"recent"',
          },
          {
            start_time: new Date(oldTs).toISOString().slice(0, 10),
            content_hash: oldHash,
            content: '"old"',
          },
        ],
        sessions: new Map(),
      })),
    });
    await processOtelGroupJob(payload, deps);
    const eventsLoad = loads.find((l) => l.table === "events_full_p1");
    expect(eventsLoad).toBeDefined();
    // only the in-window row survived
    expect(eventsLoad!.count).toBe(1);
    expect(eventsLoad!.rows).toHaveLength(1);
    const contentLoad = loads.find((l) => l.table === "content_dict_p1");
    expect(contentLoad?.rows).toEqual([
      expect.objectContaining({ content_hash: recentHash }),
    ]);
    expect(contentLoad?.rows[0]).not.toHaveProperty("project_id");
    expect(
      vi
        .mocked(recordIncrement)
        .mock.calls.filter(
          ([metric]) =>
            metric === "langfuse.otel_group.retention_filtered_rows",
        ),
    ).toEqual([["langfuse.otel_group.retention_filtered_rows", 1]]);
  });

  it("a project not live in cache still targets its split tables", async () => {
    __setSplitSnapshotForTest([["other", true]]); // p1 NOT split
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps();
    await processOtelGroupJob(payload, deps);
    const tables = loads.map((l) => l.table);
    expect(tables).toContain("events_full_p1");
    expect(tables).toContain("traces_scalar_p1");
    expect(tables).not.toContain("events_full");
  });
});

// Stage 1 review #4: the traces_scalar load must have the SAME missing-table
// three-way as events_full. Without it, a scalar table lost AFTER go-live (ops
// DROP / rebuild window / replica loss) means events commit, the scalar load
// throws uncaught, and the job retries to the DLQ forever — events in, scalar
// silently lost, table never reprovisioned.
describe("processOtelGroupJob (Stage 1 #4: scalar missing-table three-way)", () => {
  afterEach(() => {
    __setSplitSnapshotForTest(null);
    vi.mocked(handleMissingSplitTable).mockReset();
  });

  // events_full_p1 loads fine; traces_scalar_p1 is gone.
  const scalarMissingDeps = () => {
    const { deps, loads } = makeDeps();
    (deps.streamLoadBody as ReturnType<typeof vi.fn>).mockImplementation(
      async (table, body, count, options) => {
        if (table === "traces_scalar_p1") {
          throw new Error(
            "errCode = 2, Table [traces_scalar_p1] does not exist",
          );
        }
        loads.push({ table, rows: decodeBody(body), count, options });
        return { dedupedByLabel: false };
      },
    );
    return { deps, loads };
  };

  it("reprovision/pg-error → job throws (events committed, ledger withheld so replay heals)", async () => {
    __setSplitSnapshotForTest([["p1", true]]);
    vi.mocked(handleMissingSplitTable).mockResolvedValue("retry");
    const { deps, loads } = scalarMissingDeps();

    await expect(
      processOtelGroupJob(payloadFor(["f1.json"]), deps),
    ).rejects.toThrow(/does not exist/);

    const tables = loads.map((l) => l.table);
    expect(tables).toContain("events_full_p1"); // events already committed
    expect(tables).not.toContain("pg:otel_file_ledger"); // ledger withheld → replay re-runs scalar
    expect(handleMissingSplitTable).toHaveBeenCalledWith("p1");
  });

  it("tombstoned project → dead-letter + ledger, NO throw (no infinite retry)", async () => {
    __setSplitSnapshotForTest([["p1", true]]);
    vi.mocked(handleMissingSplitTable).mockResolvedValue("skip");
    const { deps, loads } = scalarMissingDeps();

    await processOtelGroupJob(payloadFor(["f1.json"]), deps); // resolves, no throw

    const tables = loads.map((l) => l.table);
    expect(tables).toContain("events_full_p1");
    expect(tables).toContain("pg:otel_file_ledger"); // ledger written → group never resurfaces
  });
});
