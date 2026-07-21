import { describe, it, expect, vi } from "vitest";
import { ZodError } from "zod/v4";

import {
  processOtelGroupJob,
  isDeterministicIngestError,
  type GroupJobDeps,
} from "./otelGroupJobProcessor";
import {
  computeGroupId,
  eventsFullLabelForGroup,
  type OtelGroupIngestionEventType,
  type OtelPendingEntryType,
  type StreamLoadBodySource,
} from "@langfuse/shared/src/server";
import { ForbiddenError } from "@langfuse/shared";

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

const payloadFor = (
  fileKeys: string[],
): OtelGroupIngestionEventType => ({
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
        { project_id: "p1", id: "t1", start_time: Date.now(), event_ts: Date.now() } as never,
      ],
      sessions: new Map([["s1", "default"]]),
    })),
    streamLoadBody: vi.fn(async (table, body, count, options) => {
      loads.push({ table, rows: decodeBody(body), count, options });
      return {
        dedupedByLabel:
          table === "events_full" ? (overrides.dedupedByLabel ?? false) : false,
      };
    }),
    ledgerExists: vi.fn(async () => overrides.ledger ?? false),
    upsertSessions: vi.fn(async () => {}),
    transformConcurrency: 2,
    ...overrides,
  };
  return { deps, loads };
};

describe("processOtelGroupJob (core EO semantics)", () => {
  it("happy path: label derivation, ndjson framing, no filter ratio, ledger LAST", async () => {
    const payload = payloadFor(["f1.json", "f2.json"]);
    const { deps, loads } = makeDeps();

    await processOtelGroupJob(payload, deps);

    const [events, scalar, ledger] = loads;
    expect(events.table).toBe("events_full");
    expect(events.options.label).toBe(eventsFullLabelForGroup(payload.groupId));
    expect(events.options.format).toBe("json");
    // EO hard rule: silent row-dropping is forbidden.
    expect(events.options).not.toHaveProperty("max_filter_ratio");
    expect(events.rows).toHaveLength(4); // 2 files × 2 records

    expect(scalar.table).toBe("traces_scalar");
    expect(scalar.options).not.toHaveProperty("label"); // MoW, no label

    // Ledger is the LAST load — its existence certifies end-to-end completion.
    expect(ledger.table).toBe("blob_storage_file_log");
    expect(ledger.rows).toHaveLength(2);
    expect(ledger.rows[0]).toMatchObject({
      entity_type: "otel-file",
      entity_id: "f1.json",
      event_id: payload.groupId,
      is_deleted: 0,
    });
    expect(deps.upsertSessions).toHaveBeenCalledTimes(1);
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
          sessions: new Map(),
        };
      },
    );

    await processOtelGroupJob(payload, deps);
    const events = loads.find((l) => l.table === "events_full")!;
    expect(events.rows).toHaveLength(1); // only good.json's record
    // Ledger still covers BOTH files (bad one is dead-lettered, not retried).
    const ledger = loads.find((l) => l.table === "blob_storage_file_log")!;
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
    expect(loads.map((l) => l.table)).toEqual(["blob_storage_file_log"]);
  });

  it("C6 replay: label deduped + ledger MISSING → scalar MUST be loaded", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps({ dedupedByLabel: true, ledger: false });

    await processOtelGroupJob(payload, deps);
    // events_full was skipped by the FE (dedupedByLabel), but the partial
    // C6 crash (scalar never written) is only distinguishable via the
    // ledger — absent ledger means the scalar load must proceed.
    expect(loads.map((l) => l.table)).toEqual([
      "events_full",
      "traces_scalar",
      "blob_storage_file_log",
    ]);
  });

  it("D2/B4 late replay: label deduped + ledger EXISTS → scalar skipped (no resurrection)", async () => {
    const payload = payloadFor(["f1.json"]);
    const { deps, loads } = makeDeps({ dedupedByLabel: true, ledger: true });

    await processOtelGroupJob(payload, deps);
    expect(loads.map((l) => l.table)).toEqual([
      "events_full",
      "blob_storage_file_log",
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
