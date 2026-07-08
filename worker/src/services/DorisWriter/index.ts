import {
  dorisClient,
  DorisClientType,
  formatRecordForDoris,
  BlobStorageFileLogInsertType,
  EventRecordInsertType,
  getCurrentSpan,
  ObservationRecordInsertType,
  recordGauge,
  recordHistogram,
  recordIncrement,
  ScoreRecordInsertType,
  TraceRecordInsertType,
  TraceScalarRecordInsertType,
  TraceMetricsAggRecordInsertType,
  DatasetRunItemRecordInsertType,
} from "@langfuse/shared/src/server";

import { env as sharedEnv } from "@langfuse/shared/src/env";
import { env as workerEnv } from "../../env";
import { logger } from "@langfuse/shared/src/server";
import { instrumentAsync } from "@langfuse/shared/src/server";
import { SpanKind } from "@opentelemetry/api";
import { randomUUID } from "crypto";

// Byte count → megabytes with one decimal, for gauge/backpressure log lines.
const mb = (n: number): string => (n / 1048576).toFixed(1);

/**
 * Buffers rows per table (a plain FIFO array per table) and writes them to
 * Doris via Stream Load with a bounded worker pool:
 *
 *  - addToQueue serializes each row once and pushes it to its table's array.
 *  - A drain pool of at most maxConcurrentLoads workers pulls a batch from a
 *    "ready" table and writes it. A batch is capped by batchSize rows AND
 *    maxBatchBytes bytes (whichever first — keeps every Stream Load body under
 *    the BE's 100MB json limit); ready = a full batch by rows or bytes, or the
 *    oldest row is older than writeInterval (staleness, so low-volume tables
 *    still flush on time).
 *  - Backpressure: addToQueue awaits when a table's buffered bytes reach
 *    maxQueueSizeBytes, released when a drain frees space. Total worker memory
 *    is bounded by ~maxQueueSizeBytes (buffered) + maxConcurrentLoads * batch
 *    (in flight); the rest of the backlog stays in BullMQ/Redis.
 *  - Retry: a failed batch is parked in a per-table retryBuffer as one entry
 *    (the whole batch, kept intact) with its own attempts count and a
 *    retryNotBefore timestamp = now + exp-backoff(attempts). Fresh rows keep
 *    flowing through queue[table] untouched; a due entry is picked up by any
 *    free drain worker (preferred over fresh work). Backoff is thus per-batch —
 *    a poison batch retries alone without throttling healthy batches — and its
 *    "wait" is just a timestamp, so it never occupies a concurrency slot. Entry
 *    bytes stay counted in queueSizeBytes (same budget as fresh), so a sick BE
 *    fills the buffer → backpressure → backlog stays in Redis, never OOM.
 */
export class DorisWriter {
  private static instance: DorisWriter | null = null;
  private static client: DorisClientType | null = null;
  batchSize: number;
  maxQueueSizeBytes: number;
  maxBatchBytes: number;
  writeInterval: number;
  gaugeInterval: number;
  maxAttempts: number;
  maxConcurrentLoads: number;
  retryBackoffBaseMs: number;
  retryBackoffMaxMs: number;
  queue: DorisQueue;
  queueSizeBytes: Map<TableName, number>;

  // Tables whose batches carry a caller-owned STABLE label (generated once at
  // batch assembly, reused verbatim on every retry) → exactly-once via the FE
  // label registry (client treats "Label Already Exists"+FINISHED as success).
  // Opt-in ONLY for tables where duplicate application corrupts data
  // (AGGREGATE-KEY SUM columns). MoW unique-key tables stay label-less: their
  // keys make replays idempotent, and for per-attempt labels "already exists"
  // must remain a plain failure (collision). Instance field (not module const)
  // so tests can override. (Field initializers run at construction, so the
  // enum below is initialized by then.)
  stableLabelTables: ReadonlySet<TableName> = new Set([
    TableName.TracesMetricsAgg,
  ]);

  // Per-table retry buffer: each RetryEntry is one failed batch (kept intact,
  // written as a single Stream Load) carrying its own attempts + retryNotBefore.
  private retryBuffer: DorisRetryBuffer;
  private retryIdSeq = 0;
  // A single precise wake-up for the soonest FUTURE retry, decoupled from the
  // staleness tick / writeInterval. Already-due entries are served by the
  // running drain pool, so this only ever arms for entries not yet due.
  private retryTimer: NodeJS.Timeout | null = null;

  // Worker-pool state. At most maxConcurrentLoads drain workers run at once.
  private activeWorkers = 0;
  // Rotating start index for pickReadyTable's round-robin scan (anti-starvation).
  private drainCursor = 0;
  private stalenessTimer: NodeJS.Timeout | null = null;
  gaugeIntervalId: NodeJS.Timeout | null = null;

  // Producers parked by backpressure; each resolved fn re-checks its table's
  // cap. Woken by notifyBufferDrained() when a drain reduces buffered bytes.
  private bufferWaiters: Array<() => void> = [];

  // Per-window add/flush counters drive the gauge log. All reset to 0
  // each time the gauge tick emits, so each log line shows the rate over
  // exactly one interval. flushDurTotalMs/flushBatchCount additionally yield
  // the window's average successful-load duration (load_avg) — the direct
  // "writes got slow" signal.
  private addCounters = new Map<TableName, number>();
  private flushCounters = new Map<TableName, number>();
  private flushDurTotalMs = new Map<TableName, number>();
  private flushBatchCount = new Map<TableName, number>();
  // Failure-side window counters: failed write ATTEMPTS (each parkForRetry
  // call) and DROPPED rows (finite-maxAttempts exhaustion only).
  private failCounters = new Map<TableName, number>();
  private dropCounters = new Map<TableName, number>();

  // Backpressure episode tracking, per table: how many producers are currently
  // parked (bpWaiterCount), when the episode started (bpEngagedSince) and how
  // many enqueues it blocked (bpBlockedEnqueues). Drives edge-triggered
  // ENGAGED/released logs (one line per episode, no per-wake spam) and the
  // pool gauge's `waiters=` field.
  private bpWaiterCount = new Map<TableName, number>();
  private bpEngagedSince = new Map<TableName, number>();
  private bpBlockedEnqueues = new Map<TableName, number>();

  private constructor() {
    this.batchSize = workerEnv.LITEFUSE_INGESTION_DORIS_WRITE_BATCH_SIZE;
    this.maxQueueSizeBytes =
      workerEnv.LITEFUSE_INGESTION_DORIS_MAX_QUEUE_SIZE_BYTES;
    this.maxBatchBytes =
      workerEnv.LITEFUSE_INGESTION_DORIS_MAX_BATCH_SIZE_BYTES;
    this.writeInterval = workerEnv.LITEFUSE_INGESTION_DORIS_WRITE_INTERVAL_MS;
    this.gaugeInterval = workerEnv.LITEFUSE_INGESTION_DORIS_GAUGE_INTERVAL_MS;
    this.maxAttempts = sharedEnv.LITEFUSE_INGESTION_DORIS_MAX_ATTEMPTS;
    // Guard against a 0 that would deadlock the drain pool (env is .positive(),
    // so this only defends against a bad programmatic/test override).
    this.maxConcurrentLoads = Math.max(
      1,
      workerEnv.LITEFUSE_INGESTION_DORIS_MAX_CONCURRENT_LOADS,
    );
    this.retryBackoffBaseMs =
      workerEnv.LITEFUSE_INGESTION_DORIS_RETRY_BACKOFF_BASE_MS;
    this.retryBackoffMaxMs =
      workerEnv.LITEFUSE_INGESTION_DORIS_RETRY_BACKOFF_MAX_MS;

    this.queue = {
      [TableName.Traces]: [],
      [TableName.Scores]: [],
      [TableName.Observations]: [],
      [TableName.BlobStorageFileLog]: [],
      [TableName.DatasetRunItems]: [],
      [TableName.EventsFull]: [],
      [TableName.TracesScalar]: [],
      [TableName.TracesMetricsAgg]: [],
    };

    this.queueSizeBytes = new Map();

    this.retryBuffer = {
      [TableName.Traces]: [],
      [TableName.Scores]: [],
      [TableName.Observations]: [],
      [TableName.BlobStorageFileLog]: [],
      [TableName.DatasetRunItems]: [],
      [TableName.EventsFull]: [],
      [TableName.TracesScalar]: [],
      [TableName.TracesMetricsAgg]: [],
    };

    this.start();
  }

  /**
   * Get the singleton instance of DorisWriter.
   * Client parameter is only used for testing.
   */
  public static getInstance(dorisClient?: DorisClientType) {
    if (dorisClient) {
      DorisWriter.client = dorisClient;
    }

    if (!DorisWriter.instance) {
      DorisWriter.instance = new DorisWriter();
    }

    return DorisWriter.instance;
  }

  private start() {
    logger.info(
      `Starting DorisWriter. staleness/interval: ${this.writeInterval} ms, batch size: ${this.batchSize}, max queue: ${this.maxQueueSizeBytes} bytes, max concurrent loads: ${this.maxConcurrentLoads}`,
    );

    // Staleness tick: nudge the drain pool every writeInterval so a partial
    // batch (< batchSize) that has been waiting gets flushed even if no new
    // rows arrive — this is what covers low-volume tables.
    this.stalenessTimer = setInterval(
      () => this.scheduleDrain(),
      this.writeInterval,
    );

    // Periodic queue gauge — one line per active table + one pool line, so the
    // write state is readable at a glance: buffer fill vs cap (how close to
    // backpressure), retry depth, in/out rates, average successful-load
    // duration (writes got slow?), pool saturation and parked producers
    // (backpressure happening right now?). Tables completely silent in the
    // window are skipped.
    const gaugeWindowSec = Math.round(this.gaugeInterval / 1000);
    this.gaugeIntervalId = setInterval(() => {
      let printedAny = false;
      for (const t of Object.values(TableName)) {
        const len = this.queue[t]?.length ?? 0;
        const retrying = this.retryBuffer[t]?.length ?? 0;
        const added = this.addCounters.get(t) ?? 0;
        const flushed = this.flushCounters.get(t) ?? 0;
        const failed = this.failCounters.get(t) ?? 0;
        const dropped = this.dropCounters.get(t) ?? 0;
        const bufBytes = this.queueSizeBytes.get(t) ?? 0;
        const durTotal = this.flushDurTotalMs.get(t) ?? 0;
        const durCount = this.flushBatchCount.get(t) ?? 0;
        this.addCounters.set(t, 0);
        this.flushCounters.set(t, 0);
        this.failCounters.set(t, 0);
        this.dropCounters.set(t, 0);
        this.flushDurTotalMs.set(t, 0);
        this.flushBatchCount.set(t, 0);
        if (
          len === 0 &&
          retrying === 0 &&
          added === 0 &&
          flushed === 0 &&
          failed === 0 &&
          dropped === 0
        )
          continue;
        printedAny = true;
        const pct = Math.round((bufBytes / this.maxQueueSizeBytes) * 100);
        const loadAvg =
          durCount > 0 ? `${(durTotal / durCount / 1000).toFixed(1)}s` : "-";
        // drop= only when non-zero — it's impossible in the default infinite-
        // retry mode, so don't waste a column on a constant 0.
        const dropPart = dropped > 0 ? ` drop=${dropped}` : "";
        logger.info(
          `[DorisWriter.gauge.${gaugeWindowSec}s] ${t.padEnd(22)} q=${String(len).padEnd(7)} buf=${mb(bufBytes)}/${mb(this.maxQueueSizeBytes)}MB(${pct}%) retry=${String(retrying).padEnd(5)} +${String(added).padEnd(7)} -${String(flushed).padEnd(7)} fail=${String(failed).padEnd(5)}${dropPart} load_avg=${loadAvg}`,
        );
      }
      let totalWaiters = 0;
      for (const n of this.bpWaiterCount.values()) totalWaiters += n;
      if (printedAny || this.activeWorkers > 0 || totalWaiters > 0) {
        logger.info(
          `[DorisWriter.gauge.${gaugeWindowSec}s] pool loads=${this.activeWorkers}/${this.maxConcurrentLoads} waiters=${totalWaiters}`,
        );
      }
    }, this.gaugeInterval);
  }

  public async shutdown(): Promise<void> {
    logger.info("Shutting down DorisWriter...");

    if (this.stalenessTimer) {
      clearInterval(this.stalenessTimer);
      this.stalenessTimer = null;
    }
    if (this.gaugeIntervalId) {
      clearInterval(this.gaugeIntervalId);
      this.gaugeIntervalId = null;
    }

    await this.flushRemaining();

    // flushRemaining's failed batches can re-arm the retry timer; clear it last
    // so shutdown doesn't leave a dangling timer holding the process open.
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }

    logger.info("DorisWriter shutdown complete.");
  }

  public async addToQueue<T extends TableName>(
    tableName: T,
    data: RecordInsertType<T>,
  ): Promise<void> {
    // Backpressure: block while this table's buffered bytes are at the hard
    // cap. Released by notifyBufferDrained() when a drain reduces the bytes.
    // Awaiting (not busy-polling) yields the event loop, so BullMQ job locks
    // keep renewing while intake is throttled and the backlog stays in Redis.
    // Episodes are logged edge-triggered: one ENGAGED warn when the first
    // producer parks, one released info when the last parked producer gets
    // through — so the log answers "is intake blocked, since when, how much"
    // without a line per wake-up.
    if ((this.queueSizeBytes.get(tableName) ?? 0) >= this.maxQueueSizeBytes) {
      const waiters = (this.bpWaiterCount.get(tableName) ?? 0) + 1;
      this.bpWaiterCount.set(tableName, waiters);
      if (waiters === 1) {
        this.bpEngagedSince.set(tableName, Date.now());
        this.bpBlockedEnqueues.set(tableName, 0);
        logger.warn(
          `[DorisWriter] ${tableName} BACKPRESSURE engaged: buf ${mb(this.queueSizeBytes.get(tableName) ?? 0)}/${mb(this.maxQueueSizeBytes)}MB (q=${this.queue[tableName].length} rows, retry=${this.retryBuffer[tableName].length}) — intake blocked`,
        );
      }
      this.bpBlockedEnqueues.set(
        tableName,
        (this.bpBlockedEnqueues.get(tableName) ?? 0) + 1,
      );

      while (
        (this.queueSizeBytes.get(tableName) ?? 0) >= this.maxQueueSizeBytes
      ) {
        await new Promise<void>((resolve) => this.bufferWaiters.push(resolve));
      }

      const left = (this.bpWaiterCount.get(tableName) ?? 1) - 1;
      this.bpWaiterCount.set(tableName, left);
      if (left === 0) {
        const since = this.bpEngagedSince.get(tableName) ?? Date.now();
        logger.info(
          `[DorisWriter] ${tableName} backpressure released after ${((Date.now() - since) / 1000).toFixed(1)}s (${this.bpBlockedEnqueues.get(tableName) ?? 0} enqueues were blocked)`,
        );
      }
    }

    // Format + serialize exactly once, here at enqueue. The drain path just
    // concatenates these strings, so a row is never re-formatted or
    // re-serialized (not on flush, not on retry). estimatedSizeBytes is the
    // exact byte length of what we send.
    const line = JSON.stringify(formatRecordForDoris(data, tableName));
    const estimatedSizeBytes = Buffer.byteLength(line, "utf8");
    this.queue[tableName].push({
      createdAt: Date.now(),
      attempts: 1,
      line,
      estimatedSizeBytes,
    });
    this.queueSizeBytes.set(
      tableName,
      (this.queueSizeBytes.get(tableName) ?? 0) + estimatedSizeBytes,
    );
    this.addCounters.set(tableName, (this.addCounters.get(tableName) ?? 0) + 1);

    logger.debug(
      `[DorisWriter.addToQueue] ${tableName} length=${this.queue[tableName].length}`,
    );

    this.scheduleDrain();
  }

  /**
   * Launch drain workers (up to maxConcurrentLoads) while there is ready work.
   * Each worker pulls ready batches until none remain, then exits.
   */
  private scheduleDrain(): void {
    while (
      this.activeWorkers < this.maxConcurrentLoads &&
      this.pickReadyTable(Date.now()) !== null
    ) {
      this.activeWorkers++;
      void this.drainWorker()
        .catch((err) => logger.error("DorisWriter.drainWorker", err))
        .finally(() => {
          this.activeWorkers--;
        });
    }
  }

  private async drainWorker(): Promise<void> {
    for (;;) {
      // One timestamp for this whole iteration so pickReadyTable and
      // takeDueRetryEntry agree exactly — a table reported "ready" for a retry
      // must then yield that retry (no skew that could spin this loop).
      const now = Date.now();
      const table = this.pickReadyTable(now);
      if (!table) return;

      // Prefer a due retry entry over fresh work. Its bytes were re-counted in
      // queueSizeBytes when it was parked; release them now (re-added on failure
      // by parkForRetry, stay released on success). No await between the pick
      // above and this splice, so no other worker can grab the same entry.
      const entry = this.takeDueRetryEntry(table, now);
      if (entry) {
        this.queueSizeBytes.set(
          table,
          (this.queueSizeBytes.get(table) ?? 0) - entry.bytes,
        );
        this.notifyBufferDrained();
        await this.processBatch({
          table,
          items: entry.items,
          attemptsAlreadyMade: entry.attempts,
          label: entry.label,
        });
        continue;
      }

      const { items, bytes, label } = this.takeFreshBatch(table);
      // No fresh rows either (the table was ready only for a retry that a
      // concurrent worker already took, or nothing is ready) — re-evaluate.
      if (items.length === 0) continue;

      this.queueSizeBytes.set(
        table,
        (this.queueSizeBytes.get(table) ?? 0) - bytes,
      );
      this.notifyBufferDrained();

      for (const item of items) {
        recordHistogram(
          "langfuse.queue.doris_writer.wait_time",
          now - item.createdAt,
          { unit: "milliseconds" },
        );
      }
      await this.processBatch({ table, items, attemptsAlreadyMade: 0, label });
    }
  }

  /**
   * The readiest table to write, or null. A table is ready when it has a due
   * retry entry, holds a full fresh batch, or its oldest fresh row is older than
   * writeInterval (staleness). Scans round-robin from a rotating cursor so no
   * table can be starved by an earlier-in-enum-order table (events_full is last,
   * so a fixed-order scan could starve it while e.g. scores keep flowing).
   */
  private pickReadyTable(now: number): TableName | null {
    const tables = Object.values(TableName);
    for (let i = 0; i < tables.length; i++) {
      const pos = (this.drainCursor + i) % tables.length;
      if (this.hasReadyWork(tables[pos], now)) {
        this.drainCursor = (pos + 1) % tables.length;
        return tables[pos];
      }
    }
    return null;
  }

  /**
   * A table has work now if a retry is due or its fresh queue is ready.
   * Fresh readiness is three-dimensional — rows (a full batchSize), bytes
   * (buffered fresh bytes reach maxBatchBytes: with big rows a byte-capped
   * batch is complete long before the row count, and without this trigger it
   * would sit until the staleness tick), or time (oldest row older than
   * writeInterval).
   */
  private hasReadyWork(table: TableName, now: number): boolean {
    const buf = this.retryBuffer[table];
    let retryBytes = 0;
    for (const e of buf) {
      if (now >= e.retryNotBefore) return true;
      retryBytes += e.bytes;
    }
    const q = this.queue[table];
    // queueSizeBytes counts fresh + parked retries; subtract the parked part
    // so the byte trigger fires on FRESH bytes only (a big parked-but-not-due
    // retry must not make the fresh queue look flush-ready).
    const freshBytes = (this.queueSizeBytes.get(table) ?? 0) - retryBytes;
    return (
      q.length >= this.batchSize ||
      freshBytes >= this.maxBatchBytes ||
      (q.length > 0 && now - q[0].createdAt >= this.writeInterval)
    );
  }

  /**
   * Assemble one batch from the head of a table's fresh queue: take rows until
   * batchSize rows OR the next row would push the batch past maxBatchBytes —
   * always at least one row (a single row can't be split; one larger than the
   * cap ships alone). Single splice (no per-row shift), and the byte sum is
   * returned so the caller doesn't re-reduce. Caps every Stream Load body
   * safely under the BE's streaming_load_json_max_mb.
   */
  private takeFreshBatch(table: TableName): {
    items: QueuedRow[];
    bytes: number;
    label?: string;
  } {
    const q = this.queue[table];
    let count = 0;
    let bytes = 0;
    while (count < this.batchSize && count < q.length) {
      const next = q[count].estimatedSizeBytes;
      if (count > 0 && bytes + next > this.maxBatchBytes) break;
      bytes += next;
      count++;
    }
    // Stable label is born HERE, with the batch, and follows it verbatim
    // through every retry (RetryEntry.label). The batch is never split or
    // merged afterwards, so label ↔ content binding holds for life — the
    // precondition for the FE's label-dedup semantics.
    const label = this.stableLabelTables.has(table)
      ? `langfuse_${table}_${Date.now()}_${randomUUID()}`
      : undefined;
    return { items: q.splice(0, count), bytes, label };
  }

  /**
   * Remove and return the earliest-due retry entry for a table, or null if none
   * is due. Synchronous (no await) so the splice can't race another worker.
   */
  private takeDueRetryEntry(table: TableName, now: number): RetryEntry | null {
    const buf = this.retryBuffer[table];
    let idx = -1;
    let earliest = Infinity;
    for (let i = 0; i < buf.length; i++) {
      if (buf[i].retryNotBefore <= now && buf[i].retryNotBefore < earliest) {
        earliest = buf[i].retryNotBefore;
        idx = i;
      }
    }
    if (idx < 0) return null;
    return buf.splice(idx, 1)[0];
  }

  /**
   * Park a failed batch for a later retry, or drop it if it has exhausted a
   * finite maxAttempts. `attempts` is the number of attempts already made
   * (>= 1). The batch's bytes were released from queueSizeBytes when it was
   * taken; on park we re-add them (shared budget → backpressure), on drop we
   * leave them released. `reason` is the failure message — logged on EVERY
   * backoff/drop line so a sustained failure stays diagnosable from any
   * retry log line, not just the first error before the backoff kicked in.
   */
  private parkForRetry(params: {
    table: TableName;
    items: QueuedRow[];
    attempts: number;
    bytes: number;
    reason: string;
    label?: string;
  }): void {
    const { table, items, attempts, bytes, label } = params;
    // The cause is carried VERBATIM — no truncation. Every retry/drop line is
    // self-sufficient (grep the warn stream alone and you have the full
    // failure). Line size stays bounded because the client caps embedded peer
    // response bodies at RESPONSE_LOG_MAX_CHARS when building the message.
    const reason = params.reason;
    // Window counter for the gauge's fail= (one per failed attempt).
    this.failCounters.set(table, (this.failCounters.get(table) ?? 0) + 1);
    // maxAttempts <= 0 means retry forever (never drop) — the default.
    if (this.maxAttempts > 0 && attempts >= this.maxAttempts) {
      // TODO - Add to a dead letter queue in Redis rather than dropping.
      recordIncrement("langfuse.queue.doris_writer.error", items.length);
      this.dropCounters.set(
        table,
        (this.dropCounters.get(table) ?? 0) + items.length,
      );
      logger.error(
        `Max attempts (${this.maxAttempts}) reached for ${table} batch of ${items.length}. Dropping. cause: ${reason}`,
        { sample: items[0]?.line.slice(0, 500) },
      );
      return;
    }

    const delay = Math.min(
      this.retryBackoffBaseMs * 2 ** Math.min(attempts - 1, 30),
      this.retryBackoffMaxMs,
    );
    const id = ++this.retryIdSeq;
    this.retryBuffer[table].push({
      id,
      items,
      bytes,
      attempts,
      retryNotBefore: Date.now() + delay,
      label,
    });
    this.queueSizeBytes.set(
      table,
      (this.queueSizeBytes.get(table) ?? 0) + bytes,
    );
    this.rearmRetryTimer();
    logger.warn(
      `[DorisWriter] ${table} retry #${id} (${items.length} rows) attempt ${attempts} failed; backing off ${delay}ms.${label ? ` label=${label}` : ""} cause: ${reason}`,
    );
  }

  /**
   * Arm a single timer for the soonest FUTURE retry (not-yet-due). Already-due
   * entries are handled by the running drain pool — a worker's loop keeps
   * picking due entries until none remain — so arming for them would busy-spin.
   * Called after every retryBuffer mutation; recomputes the true minimum.
   */
  private rearmRetryTimer(): void {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    const now = Date.now();
    let earliestFuture = Infinity;
    for (const t of Object.values(TableName)) {
      for (const e of this.retryBuffer[t]) {
        if (e.retryNotBefore > now && e.retryNotBefore < earliestFuture) {
          earliestFuture = e.retryNotBefore;
        }
      }
    }
    if (earliestFuture === Infinity) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.scheduleDrain();
      this.rearmRetryTimer();
    }, earliestFuture - now);
  }

  /** Wake all backpressured producers; each re-checks its table's cap. */
  private notifyBufferDrained(): void {
    if (this.bufferWaiters.length === 0) return;
    const waiters = this.bufferWaiters;
    this.bufferWaiters = [];
    for (const w of waiters) w();
  }

  /**
   * Write one batch. On failure park the whole batch for retry via
   * parkForRetry. `attemptsAlreadyMade` is how many attempts this batch had
   * before this one (0 for a fresh batch, the entry's attempts for a retry), so
   * the failure records attempt `attemptsAlreadyMade + 1`. The batch's bytes are
   * already released from queueSizeBytes by the caller.
   */
  private async processBatch(params: {
    table: TableName;
    items: QueuedRow[];
    attemptsAlreadyMade: number;
    /** Stable per-batch label (stableLabelTables only); undefined otherwise. */
    label?: string;
  }): Promise<void> {
    const { table, items, attemptsAlreadyMade, label } = params;
    return instrumentAsync(
      { name: "write-to-doris", spanKind: SpanKind.CONSUMER },
      async () => {
        // The retry boundary is exactly "did the write succeed?". Everything up
        // to and including writeToDoris is inside the try, so any failure before
        // the rows are committed re-parks the batch (no loss). Post-write work
        // (metrics/logs) is deliberately OUTSIDE this try: once the rows are
        // committed we must never re-park (that would double-write), so a throw
        // there must not reach this catch.
        const processingStartTime = Date.now();
        try {
          recordIncrement("langfuse.queue.doris_writer.request");
          const currentSpan = getCurrentSpan();
          if (currentSpan) {
            currentSpan.setAttributes({ [`${table}-length`]: items.length });
          }

          // Rows are pre-serialized; wrap them in a JSON array by joining with
          // commas — byte-identical to JSON.stringify(records), the format Doris
          // accepts with strip_outer_array=true.
          await this.writeToDoris({
            table,
            body: `[${items.map((item) => item.line).join(",")}]`,
            recordCount: items.length,
            label,
          });
        } catch (err) {
          // Not logged here: the client logged the detailed error-level line
          // ("Stream load failed for <table> ...") and parkForRetry's warn
          // carries the cause. One detailed line + one state line per failure.
          const bytes = items.reduce((s, i) => s + i.estimatedSizeBytes, 0);
          this.parkForRetry({
            table,
            items,
            attempts: attemptsAlreadyMade + 1,
            bytes,
            reason: err instanceof Error ? err.message : String(err),
            label,
          });
          return;
        }

        // Write succeeded. Post-write bookkeeping is best-effort and must never
        // trigger a re-park; swallow any error so it can't bubble to the drain
        // worker (which would otherwise die mid-loop) or duplicate the write.
        try {
          const durationMs = Date.now() - processingStartTime;
          recordHistogram(
            "langfuse.queue.doris_writer.processing_time",
            durationMs,
            { unit: "milliseconds" },
          );
          // Window accumulators for the gauge's load_avg.
          this.flushDurTotalMs.set(
            table,
            (this.flushDurTotalMs.get(table) ?? 0) + durationMs,
          );
          this.flushBatchCount.set(
            table,
            (this.flushBatchCount.get(table) ?? 0) + 1,
          );
          logger.info(
            `[DorisWriter] Flushed ${items.length} records to Doris ${table}. Queue length: ${this.queue[table].length}`,
          );
          this.flushCounters.set(
            table,
            (this.flushCounters.get(table) ?? 0) + items.length,
          );
          recordGauge(
            "ingestion_doris_insert_queue_length",
            this.queue[table].length,
            { unit: "records", entityType: table },
          );
        } catch (err) {
          logger.error(`DorisWriter.processBatch ${table} post-write`, err);
        }
      },
    );
  }

  private async writeToDoris(params: {
    table: TableName;
    body: string;
    recordCount: number;
    label?: string;
  }): Promise<void> {
    const startTime = Date.now();

    // Rows are pre-formatted + pre-serialized at enqueue and the body is a JSON
    // array string, so it must be loaded with strip_outer_array=true. The body
    // format and these flags have to agree: read_json_by_line=true expects a
    // newline-delimited body instead, and mixing them makes Doris try to parse
    // the whole array/lines as a single JSON value ("Not an json object or json
    // array").
    // On failure the error propagates to processBatch → parkForRetry, whose
    // warn line carries the cause; the client already logged the detailed
    // error-level line. No extra logging here (it was pure duplication).
    await (DorisWriter.client ?? dorisClient()).insert(
      params.table,
      params.body,
      params.recordCount,
      {
        format: "json",
        strip_outer_array: true,
        read_json_by_line: false,
        timeout: 600, // 10 minutes
        // Stable per-batch label → exactly-once (stableLabelTables only).
        label: params.label,
      },
    );

    logger.debug(`DorisWriter.writeToDoris: ${Date.now() - startTime} ms`);

    recordGauge("ingestion_doris_insert", params.recordCount);
  }

  /**
   * Flush everything currently queued right now, bounded by maxConcurrentLoads
   * (tasks are collected as thunks and run in pool-sized waves — the flush must
   * not fire dozens of concurrent Stream Loads at the BE just because it's
   * shutting down). Runs a few passes so batches parked by a failed write get
   * another attempt instead of being silently dropped — bounded so a
   * persistently-down BE can't hang shutdown. Used on shutdown and by tests.
   */
  private async flushRemaining(maxPasses = 3): Promise<void> {
    for (let pass = 0; pass < maxPasses; pass++) {
      const tasks: Array<() => Promise<void>> = [];
      for (const t of Object.values(TableName)) {
        // Parked retries first (ignore their backoff — this is a final flush).
        const buf = this.retryBuffer[t];
        while (buf.length > 0) {
          const entry = buf.pop() as RetryEntry;
          this.queueSizeBytes.set(
            t,
            (this.queueSizeBytes.get(t) ?? 0) - entry.bytes,
          );
          tasks.push(() =>
            this.processBatch({
              table: t,
              items: entry.items,
              attemptsAlreadyMade: entry.attempts,
              label: entry.label,
            }),
          );
        }
        const q = this.queue[t];
        while (q.length > 0) {
          const { items, bytes, label } = this.takeFreshBatch(t);
          this.queueSizeBytes.set(t, (this.queueSizeBytes.get(t) ?? 0) - bytes);
          tasks.push(() =>
            this.processBatch({
              table: t,
              items,
              attemptsAlreadyMade: 0,
              label,
            }),
          );
        }
      }
      this.notifyBufferDrained();
      if (tasks.length === 0) return;
      // Pool-sized waves. Guard the step against a (test-only) 0 override,
      // which would otherwise loop forever on an empty slice.
      const step = Math.max(1, this.maxConcurrentLoads);
      for (let i = 0; i < tasks.length; i += step) {
        await Promise.allSettled(tasks.slice(i, i + step).map((fn) => fn()));
      }
    }
  }

  /**
   * Force flush all queues immediately - useful for testing
   */
  public async forceFlushAll(): Promise<void> {
    await this.flushRemaining();
  }
}

export enum TableName {
  Traces = "traces",
  Scores = "scores",
  Observations = "observation_source",
  BlobStorageFileLog = "blob_storage_file_log",
  DatasetRunItems = "dataset_run_items_rmt",
  EventsFull = "events_full",
  // One scalar row per trace (root span), dual-written next to EventsFull.
  // Serves the flat trace-list fast path (see migration 0039).
  TracesScalar = "traces_scalar",
  // One increment row per span, folded by the AGGREGATE KEY model into a
  // realtime per-trace metric rollup (see migration 0040). Writes carry a
  // stable label (stableLabelTables) — duplicate application corrupts SUMs.
  TracesMetricsAgg = "trace_metrics_agg",
}

type RecordInsertType<T extends TableName> = T extends TableName.Scores
  ? ScoreRecordInsertType
  : T extends TableName.Observations
    ? ObservationRecordInsertType
    : T extends TableName.Traces
      ? TraceRecordInsertType
      : T extends TableName.BlobStorageFileLog
        ? BlobStorageFileLogInsertType
        : T extends TableName.DatasetRunItems
          ? DatasetRunItemRecordInsertType
          : T extends TableName.EventsFull
            ? EventRecordInsertType
            : T extends TableName.TracesScalar
              ? TraceScalarRecordInsertType
              : T extends TableName.TracesMetricsAgg
                ? TraceMetricsAggRecordInsertType
                : never;

// A queued row: its final, already-formatted JSON string plus bookkeeping. We
// keep only the string — not the source object — so the queue stays compact and
// retries never re-format or re-serialize.
type QueuedRow = {
  createdAt: number;
  attempts: number;
  line: string;
  estimatedSizeBytes: number;
};

type DorisQueue = {
  [T in TableName]: QueuedRow[];
};

// A parked failed batch: the intact set of rows plus retry bookkeeping. `bytes`
// is cached so byte accounting on take/park is O(1) (and matches what was
// released from queueSizeBytes). `attempts` is the number of write attempts
// already made; `retryNotBefore` is the epoch-ms gate the drain waits for.
type RetryEntry = {
  id: number;
  items: QueuedRow[];
  bytes: number;
  attempts: number;
  retryNotBefore: number;
  /**
   * Stable per-batch stream-load label (stableLabelTables only). Born at batch
   * assembly, reused VERBATIM on every retry — the FE label registry then
   * dedups a retry whose earlier attempt actually committed (exactly-once for
   * AGGREGATE-KEY tables). Never regenerate; never split/merge a labeled batch
   * except after a DEFINITE abort (aborted txns free the label — a timeout is
   * NOT definite, the txn may still commit).
   */
  label?: string;
};

type DorisRetryBuffer = {
  [T in TableName]: RetryEntry[];
};
