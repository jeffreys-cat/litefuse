import { ZodError } from "zod/v4";
import pLimit from "p-limit";

import {
  dorisClient,
  eventsFullLabelForGroup,
  labelForGroupTable,
  formatRecordForDoris,
  getS3EventStorageClient,
  logger,
  recordHistogram,
  recordIncrement,
  traceException,
  OtelIngestionProcessor,
  isSplitCacheReady,
  tableFor,
  getSplitRetentionDays,
  handleMissingSplitTable,
  contentDictStartTimeForEventStartTime,
  deduplicateEventInput,
  type ContentRecordInsertType,
  type EventRecordInsertType,
  type OtelGroupIngestionEventType,
  type OtelPendingEntryType,
  type StreamLoadBodySource,
  type StreamLoadOutcome,
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
 *
 * DELIBERATELY NOT whitelisted: TypeError/RangeError. They often ARE
 * deterministic (malformed attribute → undefined deref in the transform),
 * but they can equally originate inside infra client code (ioredis/prisma)
 * under transient failure — and misclassifying THAT means silent row loss.
 * The cost asymmetry decides it: a deterministic error treated as transient
 * fails loudly (retries → DLQ → poison ledger + alert, file kept in S3,
 * manual recovery possible — plus, on the legacy per-span path only,
 * label-less retries may duplicate rows, which the DUPLICATE-model table
 * tolerates by design); a transient error treated as deterministic loses
 * rows silently and unrecoverably. Do not widen this list without that
 * trade-off in mind.
 */
export const isDeterministicIngestError = (e: unknown): boolean => {
  if (e instanceof ZodError) return true;
  if (e instanceof SyntaxError) return true;
  if (e instanceof ForbiddenError) return true;
  return false;
};

/** Doris "target table doesn't exist" — the split-table three-way trigger. */
const isMissingTableError = (e: unknown): boolean => {
  const m = e instanceof Error ? e.message : String(e);
  return /does not exist|unknown table|table.*not found|TableNotFound/i.test(m);
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

/**
 * Content batches are consumed in manifest order, so their ordinal is stable
 * on a group retry. A label derived from group id, target table, and ordinal
 * avoids re-hashing the batch's JSON body or content keys.
 */
const contentDictLabelForBatch = (
  groupId: string,
  table: string,
  batchIndex: number,
): string => labelForGroupTable(groupId, `${table}:content:${batchIndex}`);

/**
 * Bounded, per-group content sink. Content dictionary rows are unique-key
 * MoW, so each committed chunk is replay-safe even if a later chunk fails.
 * Only the current chunk's serialized Buffers survive a Doris load.
 */
class ContentDictBatcher {
  private buffers = new Map<string, Buffer>();
  private bytes = 0;
  private queue: Promise<void> = Promise.resolve();
  private failure: unknown;
  private nextEntryIndex = 0;
  private nextBatchIndex = 0;
  private entryTurns = new Map<
    number,
    {
      promise: Promise<void>;
      resolve: () => void;
      reject: (reason?: unknown) => void;
    }
  >();

  constructor(
    private readonly maxBytes: number,
    private readonly maxRows: number,
    private readonly groupId: string,
    private readonly table: string,
    private readonly flushBody: (
      body: StreamLoadBodySource,
      recordCount: number,
      label: string,
    ) => Promise<void>,
  ) {}

  append(
    entryIndex: number,
    records: ContentRecordInsertType[],
  ): Promise<void> {
    return this.waitForEntryTurn(entryIndex).then(() =>
      this.enqueue(async () => {
        for (const record of records) {
          const key = `${record.start_time}\u0000${record.content_hash}`;
          if (this.buffers.has(key)) continue;

          const buffer = Buffer.from(JSON.stringify(record) + "\n", "utf8");
          if (buffer.length > this.maxBytes) {
            recordIncrement("langfuse.otel_group.oversize_content_entry", 1);
            recordHistogram(
              "langfuse.otel_group.oversize_content_entry_bytes",
              buffer.length,
            );
          }
          if (
            this.buffers.size > 0 &&
            (this.bytes + buffer.length > this.maxBytes ||
              this.buffers.size + 1 > this.maxRows)
          ) {
            await this.flush();
          }

          this.buffers.set(key, buffer);
          this.bytes += buffer.length;
          if (
            this.bytes >= this.maxBytes ||
            this.buffers.size >= this.maxRows
          ) {
            await this.flush();
          }
        }
      }),
    );
  }

  completeEntry(entryIndex: number): Promise<void> {
    return this.waitForEntryTurn(entryIndex).then(() =>
      this.enqueue(async () => {
        if (entryIndex !== this.nextEntryIndex) {
          throw new Error(
            `content_dict entry completion out of order: expected ${this.nextEntryIndex}, got ${entryIndex}`,
          );
        }
        this.nextEntryIndex++;
        const nextTurn = this.entryTurns.get(this.nextEntryIndex);
        nextTurn?.resolve();
        this.entryTurns.delete(this.nextEntryIndex);
      }),
    );
  }

  drain(): Promise<void> {
    return this.enqueue(() => this.flush());
  }

  abort(error: unknown): void {
    if (this.failure) return;
    this.failure = error;
    for (const turn of this.entryTurns.values()) turn.reject(error);
    this.entryTurns.clear();
  }

  private waitForEntryTurn(entryIndex: number): Promise<void> {
    if (this.failure) return Promise.reject(this.failure);
    if (entryIndex < this.nextEntryIndex) {
      return Promise.reject(
        new Error(
          `content_dict entry already completed: ${entryIndex} < ${this.nextEntryIndex}`,
        ),
      );
    }
    if (entryIndex === this.nextEntryIndex) return Promise.resolve();

    let turn = this.entryTurns.get(entryIndex);
    if (!turn) {
      let resolve!: () => void;
      let reject!: (reason?: unknown) => void;
      const promise = new Promise<void>((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
      });
      turn = { promise, resolve, reject };
      this.entryTurns.set(entryIndex, turn);
    }
    return turn.promise;
  }

  private enqueue(task: () => Promise<void>): Promise<void> {
    const run = this.queue.then(async () => {
      if (this.failure) throw this.failure;
      try {
        await task();
      } catch (error) {
        this.abort(error);
        throw error;
      }
    });
    this.queue = run.catch(() => undefined);
    return run;
  }

  private async flush(): Promise<void> {
    if (this.buffers.size === 0) return;

    const buffers = Array.from(this.buffers.values());
    const body: StreamLoadBodySource = {
      format: "ndjson",
      byteLength: this.bytes,
      chunks: () => buffers,
    };
    const recordCount = buffers.length;
    const label = contentDictLabelForBatch(
      this.groupId,
      this.table,
      this.nextBatchIndex++,
    );
    this.buffers.clear();
    this.bytes = 0;
    await this.flushBody(body, recordCount, label);
  }
}

class TombstonedContentTableError extends Error {}

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
  /** distinct sessionId → environment, for the PG trace_sessions upsert */
  sessions: Map<string, string>;
};

type FileTransformResult = Omit<TransformedFile, "entry"> & {
  contentRecords: ContentRecordInsertType[];
};

type ContentRecordSink = (
  records: ContentRecordInsertType[],
  eventStartTime: number,
) => Promise<void>;

export type GroupJobDeps = {
  downloadFile: (fileKey: string) => Promise<string>;
  transformFile: (
    entry: OtelPendingEntryType,
    raw: string,
    onContentRecords?: ContentRecordSink,
  ) => Promise<FileTransformResult>;
  streamLoadBody: (
    table: string,
    body: StreamLoadBodySource,
    recordCount: number,
    options: Record<string, unknown>,
  ) => Promise<StreamLoadOutcome>;
  ledgerExists: (groupId: string) => Promise<boolean>;
  /** Idempotent PG write of the completion ledger (one row per fileKey). */
  persistLedger: (params: {
    groupId: string;
    entries: OtelPendingEntryType[];
  }) => Promise<void>;
  upsertSessions: (
    sessions: Array<{ id: string; projectId: string; environment: string }>,
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

  // Cache-readiness gate — symmetric to the web registration gate. A group can
  // only be processed after the first provisioning/readiness snapshot.
  if (!isSplitCacheReady()) {
    throw new Error("otel group job deferred: split-cache not ready");
  }

  // Target tables. A group is homogeneous because ingestion registers every
  // file into its project lane; the target derives from any entry's projectId.
  const targetProjectId = entries[0]?.projectId;
  if (!targetProjectId) {
    throw new Error(`otel group ${groupId} has no project entries`);
  }
  const eventsTable = tableFor(targetProjectId, "events_full");
  const scalarTable = tableFor(targetProjectId, "traces_scalar");
  const contentDictTable = tableFor(targetProjectId, "content_dict");

  // Retention filter: a split table is dynamic_partition — a row older than
  // the project's retention would be
  // committed then silently TTL-dropped. Drop such rows before the load.
  // The cutoff is anchored to the group's newest registration ts (deterministic
  // across replays — I5), not Date.now().
  const retentionDays = await getSplitRetentionDays(targetProjectId);
  const nowRef =
    entries.length > 0 ? Math.max(...entries.map((e) => e.ts)) : Date.now();
  const retentionCutoffMs =
    retentionDays != null ? nowRef - retentionDays * 86_400_000 : null;
  let overWindowRows = 0;
  const isWithinRetention = (startTimeMs: number): boolean =>
    retentionCutoffMs === null || startTimeMs >= retentionCutoffMs;
  const withinRetention = (startTimeMs: number): boolean => {
    const retained = isWithinRetention(startTimeMs);
    if (!retained) overWindowRows++;
    return retained;
  };

  const contentBatcher = new ContentDictBatcher(
    env.LITEFUSE_OTEL_CONTENT_DICT_BATCH_BYTES,
    env.LITEFUSE_OTEL_CONTENT_DICT_BATCH_ROWS,
    groupId,
    contentDictTable,
    async (body, recordCount, label) => {
      try {
        await deps.streamLoadBody(contentDictTable, body, recordCount, {
          ...LOAD_OPTS,
          label,
        });
      } catch (e) {
        if (isMissingTableError(e)) {
          const action = await handleMissingSplitTable(targetProjectId);
          if (action === "skip") {
            throw new TombstonedContentTableError(
              `content dictionary table missing for tombstoned project ${targetProjectId}`,
            );
          }
        }
        throw e;
      }
    },
  );

  // ① Download + transform under the transform semaphore. Deterministic
  // errors dead-letter the FILE (its rows are skipped, the rest of the group
  // lives on); anything else fails the job → BullMQ replay.
  const limit = pLimit(deps.transformConcurrency);
  const transformed: TransformedFile[] = [];
  const transformResults = await Promise.allSettled(
    entries.map((entry, entryIndex) =>
      limit(async () => {
        let raw: string | null = null;
        try {
          try {
            raw = await deps.downloadFile(entry.fileKey);
          } catch (e) {
            // S3 errors are transient by presumption — fail the job.
            throw new Error(
              `otel group ${groupId}: download failed for ${entry.fileKey}: ${e instanceof Error ? e.message : String(e)}`,
            );
          }
          let usedContentSink = false;
          const t = await deps.transformFile(
            entry,
            raw,
            async (contentRecords, eventStartTime) => {
              usedContentSink = true;
              if (isWithinRetention(eventStartTime)) {
                await contentBatcher.append(entryIndex, contentRecords);
              }
            },
          );
          raw = null;
          const retainedContentReferences = new Set<string>();
          if (!usedContentSink) {
            for (const record of t.eventRecords) {
              if (!isWithinRetention(record.start_time)) continue;
              if (typeof record.input !== "string") continue;
              const contentStartTime = contentDictStartTimeForEventStartTime(
                record.start_time,
              );
              for (const contentHash of record.input.split(/\s+/)) {
                if (contentHash) {
                  retainedContentReferences.add(
                    `${contentStartTime}\u0000${contentHash}`,
                  );
                }
              }
            }

            const retainedContentRecords = t.contentRecords.filter(
              (contentRecord) =>
                retainedContentReferences.has(
                  `${contentRecord.start_time}\u0000${contentRecord.content_hash}`,
                ),
            );
            await contentBatcher.append(entryIndex, retainedContentRecords);
          }
          t.contentRecords.length = 0;
          transformed.push({
            entry,
            eventRecords: t.eventRecords,
            sessions: t.sessions,
          });
        } catch (e) {
          if (isDeterministicIngestError(e)) {
            deadLetterRow({
              fileKey: entry.fileKey,
              reason:
                e instanceof Error ? `${e.name}: ${e.message}` : String(e),
            });
            return; // file-level dead letter — group continues
          }
          contentBatcher.abort(e);
          throw e; // transient → fail job → replay
        } finally {
          raw = null;
          // Concurrent file transforms may finish in any order, but their
          // content must enter batches in manifest order so retry labels and
          // batch boundaries remain stable.
          await contentBatcher.completeEntry(entryIndex);
        }
      }),
    ),
  );

  const transformFailure = transformResults.find(
    (result): result is PromiseRejectedResult => result.status === "rejected",
  );
  if (transformFailure) {
    if (transformFailure.reason instanceof TombstonedContentTableError) {
      deadLetterRow({
        fileKey: `group:${groupId}`,
        reason: transformFailure.reason.message,
      });
      await writeLedger(payload, deps);
      return;
    }
    throw transformFailure.reason;
  }
  try {
    await contentBatcher.drain();
  } catch (e) {
    if (e instanceof TombstonedContentTableError) {
      deadLetterRow({
        fileKey: `group:${groupId}`,
        reason: e.message,
      });
      await writeLedger(payload, deps);
      return;
    }
    throw e;
  }

  const transformMs = Date.now() - startedAt;
  const deadFiles = entries.length - transformed.length;

  // Memory discipline: content is consumed above in bounded chunks. The
  // group now retains only event records and sessions until events commit;
  // scalar rows are derived after that commit instead of being precomputed.
  let eventRows: Record<string, unknown>[] | null = [];
  for (const transformedFile of transformed) {
    for (const record of transformedFile.eventRecords) {
      if (withinRetention(record.start_time)) {
        eventRows.push(formatRecordForDoris(record, eventsTable));
      }
    }
  }
  const eventRowCount = eventRows.length;
  if (overWindowRows > 0) {
    recordIncrement(
      "langfuse.otel_group.retention_filtered_rows",
      overWindowRows,
    );
    logger.warn(
      `event=otel_retention_filtered group=${groupId.slice(0, 12)} project=${targetProjectId} rows=${overWindowRows} retentionDays=${retentionDays} — rows older than retention dropped before load`,
    );
  }

  // ② Empty group (every file dead-lettered): nothing to load — write the
  // ledger so the files never resurface in reconciliation, and ack.
  // (streamLoadBody would early-return on an empty body; short-circuiting
  // here keeps the gate logic from ever touching that path.)
  if (eventRowCount === 0) {
    await writeLedger(payload, deps);
    logger.warn(
      `[OtelGroupJob] group=${groupId.slice(0, 12)} EMPTY (all ${entries.length} file(s) dead-lettered) — ledger written, nothing loaded`,
    );
    return;
  }

  // ③ events_full: the ONE deterministic-label load of this group. The
  // content batcher drained successfully before this point.
  const label = eventsFullLabelForGroup(groupId);
  let eventsBody = eventRowCount > 0 ? ndjsonBody(eventRows) : null;
  eventRows = null; // Buffers built — the formatted objects are dead weight
  const hadEventsBody = eventsBody !== null;
  const eventsBytes = eventsBody?.byteLength ?? 0;
  const tEvents = Date.now();
  let outcome: StreamLoadOutcome;
  try {
    outcome = eventsBody
      ? await deps.streamLoadBody(eventsTable, eventsBody, eventRowCount, {
          ...LOAD_OPTS,
          label,
        })
      : { dedupedByLabel: false };
  } catch (e) {
    // "Table doesn't exist" three-way (design §4.2 / Stage 1.2d): a split
    // target's table is gone. handleMissingSplitTable classifies via PG —
    // reprovision+retry (live project), pg-error→retry (never guess), or
    // skip (tombstoned project → dead-letter the group so it doesn't retry
    // forever recreating a table the deletion flow is dropping).
    if (isMissingTableError(e)) {
      const action = await handleMissingSplitTable(targetProjectId);
      if (action === "skip") {
        deadLetterRow({
          fileKey: `group:${groupId}`,
          reason: `split tables missing for tombstoned project ${targetProjectId}`,
        });
        await writeLedger(payload, deps);
        return;
      }
    }
    throw e; // transient / reprovision → BullMQ replay
  }
  eventsBody = null; // release the group-sized Buffers before the scalar load
  const eventsMs = Date.now() - tEvents;

  // traces_scalar delete-protection gate — skip only when both the events
  // label was deduplicated and the completion ledger already exists.
  let skipScalar = false;
  if (outcome.dedupedByLabel) {
    skipScalar = await deps.ledgerExists(groupId);
    if (skipScalar) {
      recordIncrement("langfuse.otel_group.scalar_gate_skipped", 1);
    }
  }

  // Eval scheduling must happen after events_full commits: the event record is
  // the eval input, and a failed events load must not schedule side effects.
  if (deps.scheduleEvals) await deps.scheduleEvals(transformed);

  // Collect the small session metadata and release each file's map before the
  // scalar phase. The rows are still upserted after scalar, before the ledger.
  const sessions = new Map<
    string,
    { id: string; projectId: string; environment: string }
  >();
  for (const t of transformed) {
    for (const [sessionId, environment] of t.sessions) {
      sessions.set(`${t.entry.projectId}\u0000${sessionId}`, {
        id: sessionId,
        projectId: t.entry.projectId,
        environment,
      });
    }
    t.sessions.clear();
  }

  // Scalar rows are derived only after events commit. This removes the
  // second long-lived object graph that used to be built during conversion.
  let scalarRows: Record<string, unknown>[] | null = null;
  let scalarRowCount = 0;
  if (!skipScalar) {
    scalarRows = [];
    for (const transformedFile of transformed) {
      for (const record of transformedFile.eventRecords) {
        if (!isWithinRetention(record.start_time)) continue;
        const scalar = toTraceScalarRecord(record);
        if (scalar) {
          scalarRows.push(formatRecordForDoris(scalar, scalarTable));
        }
      }
    }
    scalarRowCount = scalarRows.length;
  }

  // Evals and scalar derivation are complete; no later phase needs the event
  // record objects. Keep only the formatted scalar rows until their body is
  // built, then clear those rows as well.
  for (const transformedFile of transformed) {
    transformedFile.eventRecords.length = 0;
  }

  // ⑤ traces_scalar (MoW, no label). The delete-protection gate above is
  // evaluated before deriving rows so a completed replay does no extra work.
  const tScalar = Date.now();
  let scalarDeduped = false;
  if (scalarRows && scalarRowCount > 0 && !skipScalar) {
    let scalarBody: StreamLoadBodySource | null = ndjsonBody(scalarRows);
    scalarRows = null;
    // Deterministic label (see labelForGroupTable): dedup here is a
    // server-side no-op bonus on top of MoW folding — the DELETE-protection
    // semantics still belong to the ledger gate above, which short-circuits
    // before this load is even attempted.
    try {
      const scalarOutcome = await deps.streamLoadBody(
        scalarTable,
        scalarBody,
        scalarRowCount,
        { ...LOAD_OPTS, label: labelForGroupTable(groupId, "traces_scalar") },
      );
      scalarDeduped = scalarOutcome.dedupedByLabel;
    } catch (e) {
      // Same "table doesn't exist" three-way as the events load (③). Reachable
      // only when traces_scalar_<pid> is lost AFTER go-live (ops DROP, rebuild
      // window, replica loss) — the readiness gate prevents a provisioning
      // race.
      // events_full is ALREADY committed here, so on reprovision+retry the
      // replay label-dedups events and the scalar gate (ledger still absent)
      // re-attempts THIS load once the table is back. A tombstoned project →
      // dead-letter + ledger (events already in; the deletion flow drops the
      // rest), so the job doesn't retry forever recreating a table being torn
      // down.
      if (isMissingTableError(e)) {
        const action = await handleMissingSplitTable(targetProjectId);
        if (action === "skip") {
          deadLetterRow({
            fileKey: `group:${groupId}`,
            reason: `scalar table missing for tombstoned project ${targetProjectId}`,
          });
          await writeLedger(payload, deps);
          return;
        }
      }
      throw e; // transient / reprovision → BullMQ replay
    }
    scalarBody = null;
  }
  scalarRows = null;
  const scalarMs = Date.now() - tScalar;

  // ⑥ Side effects BEFORE ack. trace_sessions failures FAIL the job
  // (createMany skipDuplicates is idempotent — replay is free; swallowing
  // the error would 404 session pages forever).
  if (sessions.size > 0) {
    await deps.upsertSessions(Array.from(sessions.values()));
  }
  sessions.clear();

  // ⑦ Ledger LAST (after both loads): its existence certifies "this group
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
  const eventsMB = eventsBytes / (1024 * 1024);
  const oldestTs = Math.min(...entries.map((e) => e.ts));
  const eventsPart = outcome.dedupedByLabel
    ? "LABEL_DEDUP"
    : hadEventsBody
      ? `${eventsMs}ms ${eventsMB.toFixed(1)}MB ${eventRowCount}rows ${eventsMs > 0 ? ((eventsMB * 1000) / eventsMs).toFixed(1) : "∞"}MB/s`
      : "none";
  const scalarPart = skipScalar
    ? "SKIPPED(gate)"
    : scalarDeduped
      ? "LABEL_DEDUP"
      : scalarRowCount > 0
        ? `${scalarMs}ms ${scalarRowCount}rows`
        : "none";
  logger.info(
    `[OtelGroupJob] group=${groupId.slice(0, 12)} files=${entries.length}${deadFiles > 0 ? ` dead_files=${deadFiles}` : ""} | transform=${transformMs}ms | events_full: ${eventsPart} | scalar: ${scalarPart} | ledger=${ledgerMs}ms | total=${totalMs}ms e2e_lag=${((Date.now() - oldestTs) / 1000).toFixed(1)}s`,
  );
};

// The ledger lives in POSTGRES (otel_file_ledger), not Doris. Its Doris
// incarnation was a per-group 2-row stream load — tablet versions piled up on
// the tiny table's few tablets and tripped max_tablet_version_num (E-235,
// the 2026-07-28 incident trigger). PG absorbs small frequent inserts
// natively, replays fold via the (file_key, group_id) unique key
// (createMany skipDuplicates), no label needed — and PG is already on this
// job's critical path (upsertSessions precedes the ledger write), so this
// adds zero new failure modes.
const writeLedger = async (
  payload: OtelGroupIngestionEventType,
  deps: GroupJobDeps,
): Promise<void> => {
  await deps.persistLedger({
    groupId: payload.groupId,
    entries: payload.entries,
  });
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
      const row = await prisma.otelFileLedger.findFirst({
        where: { groupId },
        select: { id: true },
      });
      return row !== null;
    },
    persistLedger: async ({ groupId, entries }) => {
      await prisma.otelFileLedger.createMany({
        data: entries.map((e) => ({
          projectId: e.projectId,
          fileKey: e.fileKey,
          groupId,
        })),
        skipDuplicates: true,
      });
    },
    upsertSessions: async (sessions) => {
      await prisma.traceSession.createMany({
        data: sessions,
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
}): GroupJobDeps["transformFile"] => {
  return async (entry, raw, onContentRecords) => {
    // No SDK-eligibility re-check here: the web OTel route hard-rejects
    // pre-v4 SDKs with a 400 BEFORE upload/registration, so every file in
    // the pipeline (including reconcile re-injections, which lose the SDK
    // header metadata) already passed admission. A worker-side re-check
    // could only misfire and silently drop legitimate replayed files.
    const parsed = JSON.parse(raw); // SyntaxError → deterministic → file dead letter
    const processor = new OtelIngestionProcessor({
      projectId: entry.projectId,
      publicKey: entry.publicKey,
    });
    const eventInputs: Record<string, unknown>[] =
      processor.processToEvent(parsed);

    const eventRecords: EventRecordInsertType[] = [];
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
      const deduplicated = deduplicateEventInput(
        record.input,
        record.start_time,
      );
      const eventRecord = { ...record, input: deduplicated.input };
      if (deduplicated.contentEntries.length > 0 && onContentRecords) {
        await onContentRecords(
          deduplicated.contentEntries,
          eventRecord.start_time,
        );
      }
      eventRecords.push(eventRecord);
      if (eventRecord.session_id) {
        sessions.set(
          eventRecord.session_id,
          eventRecord.environment ?? "default",
        );
      }
    }
    return {
      eventRecords,
      contentRecords: [],
      sessions,
    };
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
