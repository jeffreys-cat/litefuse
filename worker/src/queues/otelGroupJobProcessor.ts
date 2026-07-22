import { ZodError } from "zod/v4";
import pLimit from "p-limit";

import {
  dorisClient,
  eventsFullLabelForGroup,
  formatRecordForDoris,
  getS3EventStorageClient,
  logger,
  recordHistogram,
  recordIncrement,
  traceException,
  convertDateToAnalyticsDateTime,
  OtelIngestionProcessor,
  type EventRecordInsertType,
  type OtelGroupIngestionEventType,
  type OtelPendingEntryType,
  type StreamLoadBodySource,
  type StreamLoadOutcome,
  type TraceScalarRecordInsertType,
} from "@langfuse/shared/src/server";
import { ForbiddenError } from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";
import { env } from "../env";
import { env as sharedEnv } from "@langfuse/shared/src/env";
import { toTraceScalarRecord } from "../services/IngestionService";

/**
 * Self-contained otel group job (exactly-once design §3.3): one job = one
 * group = one events_full stream load batch = one deterministic label.
 *
 *   download N files → deterministic transform → stream load events_full
 *   (label) → stream load traces_scalar (MoW, gated) → fileKey ledger → ack
 *
 * No shared DorisWriter buffer, no refcount handle, no writer-internal
 * retries: BullMQ is the only retry layer (any failure fails the job), the
 * FE label registry / MoW keys are the only idempotency layers, and the
 * processor's return is the only completion signal.
 */

// ---------------------------------------------------------------------------
// Error classification (design §3.3-1 / review H3)
// ---------------------------------------------------------------------------

/**
 * Whitelist of DETERMINISTIC ingest errors — failures that will recur on
 * every replay of the same input and therefore must be dead-lettered (skipped
 * WITH a searchable record), never retried:
 *   - schema/parse errors (ZodError, JSON SyntaxError),
 *   - ForbiddenError (API key / project no longer exists — a DB lookup that
 *     answered "not found"; infrastructure failures surface as connection
 *     errors, not ForbiddenError).
 * EVERYTHING else — S3/PG/Redis/network/unknown — is presumed transient and
 * must FAIL the job (replay from S3 is free; a swallowed transient error is
 * a silently lost row).
 */
export const isDeterministicIngestError = (e: unknown): boolean => {
  if (e instanceof ZodError) return true;
  if (e instanceof SyntaxError) return true;
  if (e instanceof ForbiddenError) return true;
  return false;
};

const deadLetterRow = (params: {
  fileKey: string;
  spanId?: string;
  reason: string;
}): void => {
  // Structured + fixed-field: searchable in the log system, countable in
  // metrics. Row-level loss is EXPLICIT, never silent (design boundary).
  recordIncrement("langfuse.otel_group.row_dead_letter", 1);
  logger.error(
    `event=otel_row_dead_letter fileKey=${params.fileKey}${params.spanId ? ` spanId=${params.spanId}` : ""} reason=${params.reason.slice(0, 500)}`,
  );
};

// ---------------------------------------------------------------------------
// NDJSON body (mirrors DorisWriter's framing: one JSON object per line; the
// chunked source keeps Content-Length exact and avoids a body-sized string)
// ---------------------------------------------------------------------------

const ndjsonBody = (rows: Record<string, unknown>[]): StreamLoadBodySource => {
  const buffers = rows.map((r) =>
    Buffer.from(JSON.stringify(r) + "\n", "utf8"),
  );
  return {
    format: "ndjson",
    byteLength: buffers.reduce((a, b) => a + b.length, 0),
    chunks: () => buffers,
  };
};

// Deliberately minimal: format json + long timeout. NEVER add
// max_filter_ratio here — silently dropping rows breaks exactly-once
// (design §3.3 / plan review; repositories/doris.ts's option set with
// max_filter_ratio 0.1 must not be copied).
const LOAD_OPTS = { format: "json" as const, timeout: 600 };

// ---------------------------------------------------------------------------
// Per-file transform (real implementation; tests fake it via deps)
// ---------------------------------------------------------------------------

export type TransformedFile = {
  entry: OtelPendingEntryType;
  eventRecords: EventRecordInsertType[];
  scalarRecords: TraceScalarRecordInsertType[];
  /** distinct sessionId → environment, for the PG trace_sessions upsert */
  sessions: Map<string, string>;
};

export type GroupJobDeps = {
  downloadFile: (fileKey: string) => Promise<string>;
  transformFile: (
    entry: OtelPendingEntryType,
    raw: string,
  ) => Promise<{
    eventRecords: EventRecordInsertType[];
    scalarRecords: TraceScalarRecordInsertType[];
    sessions: Map<string, string>;
  }>;
  streamLoadBody: (
    table: string,
    body: StreamLoadBodySource,
    recordCount: number,
    options: Record<string, unknown>,
  ) => Promise<StreamLoadOutcome>;
  ledgerExists: (groupId: string) => Promise<boolean>;
  upsertSessions: (
    sessions: Map<string, { projectId: string; environment: string }>,
  ) => Promise<void>;
  /**
   * Best-effort observation-eval scheduling (side-effect boundary: NOT
   * label-protected, a replay re-schedules — declared in design §3.3). The
   * implementation must swallow its own errors; eval scheduling must never
   * fail the data path.
   */
  scheduleEvals?: (files: TransformedFile[]) => Promise<void>;
  transformConcurrency: number;
};

// ---------------------------------------------------------------------------
// Core (deterministic orchestration — the exactly-once semantics live here)
// ---------------------------------------------------------------------------

export const processOtelGroupJob = async (
  payload: OtelGroupIngestionEventType,
  deps: GroupJobDeps,
): Promise<void> => {
  const startedAt = Date.now();
  const { groupId } = payload;

  // Defense-in-depth dedup (registration + cut Lua already dedup; a payload
  // must still never load one file twice inside one label).
  const seen = new Set<string>();
  const entries = payload.entries.filter((e) => {
    if (seen.has(e.fileKey)) return false;
    seen.add(e.fileKey);
    return true;
  });

  // ① Download + transform under the transform semaphore. Deterministic
  // errors dead-letter the FILE (its rows are skipped, the rest of the group
  // lives on); anything else fails the job → BullMQ replay.
  const limit = pLimit(deps.transformConcurrency);
  const transformed: TransformedFile[] = [];
  await Promise.all(
    entries.map((entry) =>
      limit(async () => {
        let raw: string;
        try {
          raw = await deps.downloadFile(entry.fileKey);
        } catch (e) {
          // S3 errors are transient by presumption — fail the job.
          throw new Error(
            `otel group ${groupId}: download failed for ${entry.fileKey}: ${e instanceof Error ? e.message : String(e)}`,
          );
        }
        try {
          const t = await deps.transformFile(entry, raw);
          transformed.push({ entry, ...t });
        } catch (e) {
          if (isDeterministicIngestError(e)) {
            deadLetterRow({
              fileKey: entry.fileKey,
              reason:
                e instanceof Error ? `${e.name}: ${e.message}` : String(e),
            });
            return; // file-level dead letter — group continues
          }
          throw e; // transient → fail job → replay
        }
      }),
    ),
  );

  const transformMs = Date.now() - startedAt;
  const deadFiles = entries.length - transformed.length;

  const eventRows = transformed.flatMap((t) =>
    t.eventRecords.map((r) => formatRecordForDoris(r, "events_full")),
  );
  const scalarRows = transformed.flatMap((t) =>
    t.scalarRecords.map((r) => formatRecordForDoris(r, "traces_scalar")),
  );

  // ② Empty group (every file dead-lettered): nothing to load — write the
  // ledger so the files never resurface in reconciliation, and ack.
  // (streamLoadBody would early-return on an empty body; short-circuiting
  // here keeps the gate logic from ever touching that path.)
  if (eventRows.length === 0 && scalarRows.length === 0) {
    await writeLedger(payload, deps);
    logger.warn(
      `[OtelGroupJob] group=${groupId.slice(0, 12)} EMPTY (all ${entries.length} file(s) dead-lettered) — ledger written, nothing loaded`,
    );
    return;
  }

  // ③ events_full: the ONE deterministic-label load of this group.
  const label = eventsFullLabelForGroup(groupId);
  const eventsBody = eventRows.length > 0 ? ndjsonBody(eventRows) : null;
  const tEvents = Date.now();
  const outcome = eventsBody
    ? await deps.streamLoadBody("events_full", eventsBody, eventRows.length, {
        ...LOAD_OPTS,
        label,
      })
    : { dedupedByLabel: false };
  const eventsMs = Date.now() - tEvents;

  // ④ traces_scalar (MoW, no label). Delete-protection gate — DUAL condition
  // (design §3.3-3 / review B1): skip ONLY when the label deduped AND the
  // ledger already exists. events_full commits are VISIBLE immediately, so a
  // C6 replay (events_full committed, scalar never written, crash) ALSO sees
  // dedupedByLabel=true — the ledger (written after BOTH loads) is the only
  // signal separating it from a fully-completed run's late replay (which
  // must skip, or a user-deleted trace's scalar row would resurrect).
  let skipScalar = false;
  if (outcome.dedupedByLabel) {
    skipScalar = await deps.ledgerExists(groupId);
    if (skipScalar) {
      recordIncrement("langfuse.otel_group.scalar_gate_skipped", 1);
    }
  }
  const tScalar = Date.now();
  if (scalarRows.length > 0 && !skipScalar) {
    await deps.streamLoadBody(
      "traces_scalar",
      ndjsonBody(scalarRows),
      scalarRows.length,
      LOAD_OPTS,
    );
  }
  const scalarMs = Date.now() - tScalar;

  // ⑤ Side effects BEFORE ack. Eval scheduling is best-effort (impl swallows
  // errors — replays may re-schedule, declared boundary); trace_sessions
  // failures FAIL the job
  // (createMany skipDuplicates is idempotent — replay is free; swallowing
  // the error would 404 session pages forever).
  if (deps.scheduleEvals) await deps.scheduleEvals(transformed);

  const sessions = new Map<
    string,
    { projectId: string; environment: string }
  >();
  for (const t of transformed) {
    for (const [sessionId, environment] of t.sessions) {
      sessions.set(sessionId, { projectId: t.entry.projectId, environment });
    }
  }
  if (sessions.size > 0) await deps.upsertSessions(sessions);

  // ⑥ Ledger LAST (after both loads): its existence certifies "this group
  // completed once end-to-end" — which is exactly what the scalar gate and
  // the reconciliation tool key on.
  const tLedger = Date.now();
  await writeLedger(payload, deps);
  const ledgerMs = Date.now() - tLedger;

  recordIncrement("langfuse.otel_group.jobs_completed", 1);
  recordIncrement("langfuse.otel_group.files_processed", entries.length);
  recordHistogram(
    "langfuse.otel_group.job_duration_ms",
    Date.now() - startedAt,
  );
  if (outcome.dedupedByLabel) {
    recordIncrement("langfuse.otel_group.label_deduped", 1);
  }

  // Per-job performance breakdown — THE line to read for write-throughput
  // questions (one per group, ~0.3/s at target load). e2e_lag = registration
  // of the OLDEST member → fully landed: the true end-to-end ingest latency,
  // covering pending wait + grouping + queue + processing.
  const totalMs = Date.now() - startedAt;
  const eventsMB = (eventsBody?.byteLength ?? 0) / (1024 * 1024);
  const oldestTs = Math.min(...entries.map((e) => e.ts));
  const eventsPart = outcome.dedupedByLabel
    ? "LABEL_DEDUP"
    : eventsBody
      ? `${eventsMs}ms ${eventsMB.toFixed(1)}MB ${eventRows.length}rows ${eventsMs > 0 ? ((eventsMB * 1000) / eventsMs).toFixed(1) : "∞"}MB/s`
      : "none";
  const scalarPart = skipScalar
    ? "SKIPPED(gate)"
    : scalarRows.length > 0
      ? `${scalarMs}ms ${scalarRows.length}rows`
      : "none";
  logger.info(
    `[OtelGroupJob] group=${groupId.slice(0, 12)} files=${entries.length}${deadFiles > 0 ? ` dead_files=${deadFiles}` : ""} | transform=${transformMs}ms | events_full: ${eventsPart} | scalar: ${scalarPart} | ledger=${ledgerMs}ms | total=${totalMs}ms e2e_lag=${((Date.now() - oldestTs) / 1000).toFixed(1)}s`,
  );
};

const writeLedger = async (
  payload: OtelGroupIngestionEventType,
  deps: GroupJobDeps,
): Promise<void> => {
  const now = convertDateToAnalyticsDateTime(new Date());
  const rows = payload.entries.map((e) => ({
    project_id: e.projectId,
    entity_type: "otel-file",
    entity_id: e.fileKey,
    event_id: payload.groupId,
    event_ts: now,
    is_deleted: 0,
    id: e.fileKey,
    bucket_name: sharedEnv.LITEFUSE_S3_EVENT_UPLOAD_BUCKET ?? "",
    bucket_path: e.fileKey,
  }));
  // blob_storage_file_log is UNIQUE KEY(project_id, entity_type, entity_id,
  // event_id) — replayed ledger writes fold, no label needed.
  await deps.streamLoadBody(
    "blob_storage_file_log",
    ndjsonBody(rows),
    rows.length,
    LOAD_OPTS,
  );
};

// ---------------------------------------------------------------------------
// Production wiring
// ---------------------------------------------------------------------------

export const buildGroupJobDeps = (params: {
  transformFile: GroupJobDeps["transformFile"];
}): GroupJobDeps => {
  const client = dorisClient();
  return {
    downloadFile: (fileKey) =>
      getS3EventStorageClient(
        sharedEnv.LITEFUSE_S3_EVENT_UPLOAD_BUCKET!,
      ).download(fileKey),
    transformFile: params.transformFile,
    // Every load goes through the per-worker load semaphore: global in-flight
    // loads = N workers × LITEFUSE_OTEL_LOAD_CONCURRENCY (design §5.3).
    // Semaphore WAIT is measured separately — it is included in the caller's
    // per-load duration, so without this a saturated semaphore is
    // indistinguishable from a slow Doris (high ms, low MB/s) in the
    // [OtelGroupJob] line.
    streamLoadBody: async (table, body, recordCount, options) => {
      const enqueued = Date.now();
      return groupJobLoadLimiter(() => {
        const waitedMs = Date.now() - enqueued;
        recordHistogram("langfuse.otel_group.load_semaphore_wait_ms", waitedMs);
        if (waitedMs > 1_000) {
          logger.warn(
            `[OtelGroupJob] load semaphore wait ${waitedMs}ms for ${table} (pending=${groupJobLoadLimiter.pendingCount}) — LITEFUSE_OTEL_LOAD_CONCURRENCY saturated; per-load ms in the job line includes this wait`,
          );
        }
        return client.streamLoadBody(table, body, recordCount, options);
      });
    },
    ledgerExists: async (groupId) => {
      const rows = await client.query(
        `SELECT 1 AS e FROM blob_storage_file_log WHERE event_id = '${groupId.replace(/'/g, "''")}' AND entity_type = 'otel-file' LIMIT 1`,
      );
      return Array.isArray(rows) && rows.length > 0;
    },
    upsertSessions: async (sessions) => {
      await prisma.traceSession.createMany({
        data: Array.from(sessions.entries()).map(
          ([id, { projectId, environment }]) => ({
            id,
            projectId,
            environment,
          }),
        ),
        skipDuplicates: true,
      });
    },
    transformConcurrency: env.LITEFUSE_OTEL_TRANSFORM_CONCURRENCY,
  };
};

/**
 * Real per-file transform: parse → OtelIngestionProcessor.processToEvent →
 * createEventRecord per span (span-level deterministic errors dead-letter
 * the ROW; transform-internal I/O errors — getPrompt/getGenerationUsage hit
 * PG/Redis — propagate and fail the job).
 */
export const buildTransformFile = (params: {
  createEventRecord: (
    eventInput: Record<string, unknown>,
    fileKey: string,
  ) => Promise<EventRecordInsertType>;
  /**
   * Direct-write eligibility (mirrors the legacy processor's SDK gating;
   * injected by the router to avoid an import cycle). Ineligible files are
   * skipped with a counter — behavior parity with the legacy path's early
   * return for pre-v4 SDKs (effectively always eligible under the OTel-only
   * contract).
   */
  isDirectWriteEligible?: (
    entry: OtelPendingEntryType,
    parsed: unknown,
  ) => boolean;
}): GroupJobDeps["transformFile"] => {
  return async (entry, raw) => {
    const parsed = JSON.parse(raw); // SyntaxError → deterministic → file dead letter
    if (
      params.isDirectWriteEligible &&
      !params.isDirectWriteEligible(entry, parsed)
    ) {
      recordIncrement("langfuse.otel_group.file_not_direct_write_eligible", 1);
      return {
        eventRecords: [],
        scalarRecords: [],
        sessions: new Map<string, string>(),
      };
    }
    const processor = new OtelIngestionProcessor({
      projectId: entry.projectId,
      publicKey: entry.publicKey,
    });
    const eventInputs: Record<string, unknown>[] =
      processor.processToEvent(parsed);

    const eventRecords: EventRecordInsertType[] = [];
    const scalarRecords: TraceScalarRecordInsertType[] = [];
    const sessions = new Map<string, string>();
    for (const input of eventInputs) {
      let record: EventRecordInsertType;
      try {
        record = await params.createEventRecord(input, entry.fileKey);
      } catch (e) {
        if (isDeterministicIngestError(e)) {
          deadLetterRow({
            fileKey: entry.fileKey,
            spanId: String((input as { spanId?: unknown }).spanId ?? ""),
            reason: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
          });
          continue; // row-level dead letter
        }
        throw e; // I/O (PG/Redis) — fail the job
      }
      eventRecords.push(record);
      const scalar = toTraceScalarRecord(record);
      if (scalar) scalarRecords.push(scalar);
      if (record.session_id) {
        sessions.set(record.session_id, record.environment ?? "default");
      }
    }
    return { eventRecords, scalarRecords, sessions };
  };
};

export const groupJobLoadLimiter = pLimit(env.LITEFUSE_OTEL_LOAD_CONCURRENCY);

/** traceException on unexpected paths is owned by the queue-level caller. */
export const wrapGroupJobError = (groupId: string, e: unknown): Error => {
  traceException(e);
  return e instanceof Error
    ? e
    : new Error(`otel group job ${groupId} failed: ${String(e)}`);
};
