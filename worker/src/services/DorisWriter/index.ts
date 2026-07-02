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
  DatasetRunItemRecordInsertType,
} from "@langfuse/shared/src/server";

import { env as sharedEnv } from "@langfuse/shared/src/env";
import { env as workerEnv } from "../../env";
import { logger } from "@langfuse/shared/src/server";
import { instrumentAsync } from "@langfuse/shared/src/server";
import { SpanKind } from "@opentelemetry/api";

/**
 * Buffers rows per table and writes them to Doris via Stream Load.
 *
 * Data structure — chunk queue (produce per row, consume per batch):
 *   - Each table has one "open" batch being filled and a FIFO of "sealed"
 *     batches ready to write. push() appends a serialized row to the open
 *     batch; when it reaches batchSize (or ages past writeInterval — staleness)
 *     it is sealed and moved to the sealed list. Consumers pull whole sealed
 *     batches. This makes enqueue O(1) and dequeue O(#batches), avoiding the
 *     O(#rows) front-splice a flat row array would need, while covering both
 *     high volume (full batches) and low volume (staleness flush).
 *
 * Concurrency + memory:
 *   - A drain pool of at most maxConcurrentLoads workers pulls ready batches
 *     and writes them, bounding in-flight batches.
 *   - Backpressure: addToQueue awaits when a table's buffered bytes reach
 *     maxQueueSizeBytes, released when a drain frees space. Total worker memory
 *     is bounded by ~maxQueueSizeBytes (buffered) + maxConcurrentLoads * batch
 *     (in flight); the rest of the backlog stays in BullMQ/Redis.
 */
export class DorisWriter {
  private static instance: DorisWriter | null = null;
  private static client: DorisClientType | null = null;
  batchSize: number;
  maxQueueSizeBytes: number;
  writeInterval: number;
  gaugeInterval: number;
  maxAttempts: number;
  maxConcurrentLoads: number;
  queue: DorisQueue;

  // Worker-pool state. At most maxConcurrentLoads drain workers run at once.
  private activeWorkers = 0;
  // Rotating start index for pickReadyTable's round-robin scan (anti-starvation).
  private drainCursor = 0;
  private running = false;
  private stalenessTimer: NodeJS.Timeout | null = null;
  gaugeIntervalId: NodeJS.Timeout | null = null;

  // Producers parked by backpressure; each resolved fn re-checks its table's
  // cap. Woken by notifyBufferDrained() when a drain reduces buffered bytes.
  private bufferWaiters: Array<() => void> = [];

  // Per-window add/flush counters drive the gauge log. Both reset to 0
  // each time the gauge tick emits, so each log line shows the rate over
  // exactly one interval.
  private addCounters = new Map<TableName, number>();
  private flushCounters = new Map<TableName, number>();

  private constructor() {
    this.batchSize = workerEnv.LITEFUSE_INGESTION_DORIS_WRITE_BATCH_SIZE;
    this.maxQueueSizeBytes =
      workerEnv.LITEFUSE_INGESTION_DORIS_MAX_QUEUE_SIZE_BYTES;
    this.writeInterval = workerEnv.LITEFUSE_INGESTION_DORIS_WRITE_INTERVAL_MS;
    this.gaugeInterval = workerEnv.LITEFUSE_INGESTION_DORIS_GAUGE_INTERVAL_MS;
    this.maxAttempts = sharedEnv.LITEFUSE_INGESTION_DORIS_MAX_ATTEMPTS;
    // Guard against a 0 that would deadlock the drain pool (env is .positive(),
    // so this only defends against a bad programmatic/test override).
    this.maxConcurrentLoads = Math.max(
      1,
      workerEnv.LITEFUSE_INGESTION_DORIS_MAX_CONCURRENT_LOADS,
    );

    this.queue = {
      [TableName.Traces]: emptyBuffer(),
      [TableName.Scores]: emptyBuffer(),
      [TableName.Observations]: emptyBuffer(),
      [TableName.BlobStorageFileLog]: emptyBuffer(),
      [TableName.DatasetRunItems]: emptyBuffer(),
      [TableName.EventsFull]: emptyBuffer(),
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
    this.running = true;

    // Staleness tick: nudge the drain pool every writeInterval so a partial
    // open batch (< batchSize) that has been waiting gets sealed and flushed
    // even if no new rows arrive — this is what covers low-volume tables.
    this.stalenessTimer = setInterval(
      () => this.scheduleDrain(),
      this.writeInterval,
    );

    // Periodic queue gauge — one log line per table per window, but
    // skip tables that are completely silent (q=0 and no add/flush in
    // the window). Format: `q=<rows> +<added> -<flushed>`.
    const gaugeWindowSec = Math.round(this.gaugeInterval / 1000);
    this.gaugeIntervalId = setInterval(() => {
      for (const t of Object.values(TableName)) {
        const len = this.rowCount(t);
        const added = this.addCounters.get(t) ?? 0;
        const flushed = this.flushCounters.get(t) ?? 0;
        this.addCounters.set(t, 0);
        this.flushCounters.set(t, 0);
        if (len === 0 && added === 0 && flushed === 0) continue;
        logger.info(
          `[DorisWriter.gauge.${gaugeWindowSec}s] ${t.padEnd(22)} q=${String(len).padEnd(7)} +${String(added).padEnd(7)} -${flushed}`,
        );
      }
    }, this.gaugeInterval);
  }

  public async shutdown(): Promise<void> {
    logger.info("Shutting down DorisWriter...");

    this.running = false;
    if (this.stalenessTimer) {
      clearInterval(this.stalenessTimer);
      this.stalenessTimer = null;
    }
    if (this.gaugeIntervalId) {
      clearInterval(this.gaugeIntervalId);
      this.gaugeIntervalId = null;
    }

    await this.flushRemaining();

    logger.info("DorisWriter shutdown complete.");
  }

  public async addToQueue<T extends TableName>(
    tableName: T,
    data: RecordInsertType<T>,
  ): Promise<void> {
    const buf = this.queue[tableName];

    // Backpressure: block while this table's buffered bytes are at the hard
    // cap. Released by notifyBufferDrained() when a drain reduces the bytes.
    // Awaiting (not busy-polling) yields the event loop, so BullMQ job locks
    // keep renewing while intake is throttled and the backlog stays in Redis.
    while (buf.bufferedBytes >= this.maxQueueSizeBytes) {
      await new Promise<void>((resolve) => this.bufferWaiters.push(resolve));
    }

    // Format + serialize exactly once, here at enqueue. The drain path just
    // concatenates these strings, so a row is never re-formatted or
    // re-serialized (not on flush, not on retry). estimatedSizeBytes is the
    // exact byte length of what we send.
    const line = JSON.stringify(formatRecordForDoris(data, tableName));
    const estimatedSizeBytes = Buffer.byteLength(line, "utf8");
    buf.open.push({
      createdAt: Date.now(),
      attempts: 1,
      line,
      estimatedSizeBytes,
    });
    buf.openBytes += estimatedSizeBytes;
    buf.bufferedBytes += estimatedSizeBytes;
    this.addCounters.set(tableName, (this.addCounters.get(tableName) ?? 0) + 1);

    // Seal a full batch immediately so it's available to a drain worker.
    if (buf.open.length >= this.batchSize) {
      this.sealOpen(tableName, Date.now());
    }

    logger.debug(
      `[DorisWriter.addToQueue] ${tableName} rows=${this.rowCount(tableName)}`,
    );

    this.scheduleDrain();
  }

  /** Move the open batch into the sealed FIFO with the given eligibility time. */
  private sealOpen(table: TableName, readyAt: number): void {
    const buf = this.queue[table];
    if (buf.open.length === 0) return;
    buf.sealed.push({ rows: buf.open, bytes: buf.openBytes, readyAt });
    buf.open = [];
    buf.openBytes = 0;
  }

  /**
   * Launch drain workers (up to maxConcurrentLoads) while there is ready work.
   * Each worker pulls ready batches until none remain, then exits.
   */
  private scheduleDrain(): void {
    while (
      this.activeWorkers < this.maxConcurrentLoads &&
      this.pickReadyTable() !== null
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
      const table = this.pickReadyTable();
      if (!table) return;
      const batch = this.takeReadyBatch(table);
      if (!batch) return;

      const now = Date.now();
      for (const row of batch.rows) {
        recordHistogram(
          "langfuse.queue.doris_writer.wait_time",
          now - row.createdAt,
          { unit: "milliseconds" },
        );
      }
      await this.processBatch(table, batch.rows);
    }
  }

  /**
   * A table with a batch eligible to write right now, or null. Scans round-robin
   * from a rotating cursor so no table can be starved by an
   * earlier-in-enum-order table under sustained load (events_full is last in the
   * enum, so a fixed-order scan could starve it while e.g. scores keep flowing).
   */
  private pickReadyTable(): TableName | null {
    const now = Date.now();
    const tables = Object.values(TableName);
    for (let i = 0; i < tables.length; i++) {
      const pos = (this.drainCursor + i) % tables.length;
      if (this.hasReadyBatch(tables[pos], now)) {
        this.drainCursor = (pos + 1) % tables.length;
        return tables[pos];
      }
    }
    return null;
  }

  private hasReadyBatch(table: TableName, now: number): boolean {
    const buf = this.queue[table];
    if (buf.sealed.some((b) => b.readyAt <= now)) return true;
    // A partial open batch becomes ready once its oldest row is stale.
    return (
      buf.open.length > 0 && now - buf.open[0].createdAt >= this.writeInterval
    );
  }

  /**
   * Remove and return the next eligible batch for a table (sealing a stale open
   * batch if needed), or null. Updates buffered bytes and wakes producers.
   */
  private takeReadyBatch(table: TableName): SealedBatch | null {
    const buf = this.queue[table];
    const now = Date.now();

    if (
      buf.open.length > 0 &&
      now - buf.open[0].createdAt >= this.writeInterval
    ) {
      this.sealOpen(table, now);
    }

    const idx = buf.sealed.findIndex((b) => b.readyAt <= now);
    if (idx === -1) return null;
    const [batch] = buf.sealed.splice(idx, 1);
    buf.bufferedBytes -= batch.bytes;
    this.notifyBufferDrained();
    return batch;
  }

  /** Wake all backpressured producers; each re-checks its table's cap. */
  private notifyBufferDrained(): void {
    if (this.bufferWaiters.length === 0) return;
    const waiters = this.bufferWaiters;
    this.bufferWaiters = [];
    for (const w of waiters) w();
  }

  /** Write one batch; on failure re-queue rows (attempts+1) up to maxAttempts. */
  private async processBatch(
    table: TableName,
    rows: QueuedRow[],
  ): Promise<void> {
    return instrumentAsync(
      { name: "write-to-doris", spanKind: SpanKind.CONSUMER },
      async () => {
        recordIncrement("langfuse.queue.doris_writer.request");
        const currentSpan = getCurrentSpan();
        if (currentSpan) {
          currentSpan.setAttributes({ [`${table}-length`]: rows.length });
        }

        try {
          const processingStartTime = Date.now();

          // Rows are pre-serialized; wrap them in a JSON array by joining with
          // commas — byte-identical to JSON.stringify(records), the format Doris
          // accepts with strip_outer_array=true.
          await this.writeToDoris({
            table,
            body: `[${rows.map((r) => r.line).join(",")}]`,
            recordCount: rows.length,
          });

          recordHistogram(
            "langfuse.queue.doris_writer.processing_time",
            Date.now() - processingStartTime,
            { unit: "milliseconds" },
          );

          logger.info(
            `[DorisWriter] Flushed ${rows.length} records to Doris ${table}. Rows queued: ${this.rowCount(table)}`,
          );

          this.flushCounters.set(
            table,
            (this.flushCounters.get(table) ?? 0) + rows.length,
          );

          recordGauge(
            "ingestion_doris_insert_queue_length",
            this.rowCount(table),
            { unit: "records", entityType: table },
          );
        } catch (err) {
          logger.error(`DorisWriter.processBatch ${table}`, err);

          const survivors: QueuedRow[] = [];
          let bytes = 0;
          for (const row of rows) {
            if (row.attempts < this.maxAttempts) {
              survivors.push({ ...row, attempts: row.attempts + 1 });
              bytes += row.estimatedSizeBytes;
            } else {
              // TODO - Add to a dead letter queue in Redis rather than dropping
              recordIncrement("langfuse.queue.doris_writer.error");
              logger.error(
                `Max attempts reached for ${table} record. Dropping record.`,
                { line: row.line.slice(0, 500) },
              );
            }
          }
          if (survivors.length > 0) {
            // Re-queue as a sealed batch with a delayed readyAt so the retry is
            // spaced by ~writeInterval instead of a drain worker hot-looping on
            // a fast-failing batch.
            const buf = this.queue[table];
            buf.sealed.push({
              rows: survivors,
              bytes,
              readyAt: Date.now() + this.writeInterval,
            });
            buf.bufferedBytes += bytes;
          }
        }
      },
    );
  }

  private async writeToDoris(params: {
    table: TableName;
    body: string;
    recordCount: number;
  }): Promise<void> {
    const startTime = Date.now();

    // Rows are pre-formatted + pre-serialized at enqueue and the body is a JSON
    // array string, so it must be loaded with strip_outer_array=true. The body
    // format and these flags have to agree: read_json_by_line=true expects a
    // newline-delimited body instead, and mixing them makes Doris try to parse
    // the whole array/lines as a single JSON value ("Not an json object or json
    // array").
    await (DorisWriter.client ?? dorisClient())
      .insert(params.table, params.body, params.recordCount, {
        format: "json",
        strip_outer_array: true,
        read_json_by_line: false,
        timeout: 600, // 10 minutes
      })
      .catch((err: any) => {
        logger.error(`DorisWriter.writeToDoris ${err}`);
        throw err;
      });

    logger.debug(`DorisWriter.writeToDoris: ${Date.now() - startTime} ms`);

    recordGauge("ingestion_doris_insert", params.recordCount);
  }

  /**
   * Flush everything currently queued right now (seal open batches and write
   * every sealed batch regardless of readyAt). Used on shutdown and by tests.
   * Runs a few passes so rows re-queued by a failed write (which land back in
   * `sealed` after this pass drained it) get another attempt instead of being
   * silently dropped — bounded so a persistently-down BE can't hang shutdown.
   */
  private async flushRemaining(maxPasses = 3): Promise<void> {
    for (let pass = 0; pass < maxPasses; pass++) {
      const tasks: Promise<void>[] = [];
      for (const t of Object.values(TableName)) {
        const buf = this.queue[t];
        if (buf.open.length > 0) this.sealOpen(t, 0);
        while (buf.sealed.length > 0) {
          const batch = buf.sealed.shift() as SealedBatch;
          buf.bufferedBytes -= batch.bytes;
          tasks.push(this.processBatch(t, batch.rows));
        }
      }
      this.notifyBufferDrained();
      if (tasks.length === 0) return;
      await Promise.allSettled(tasks);
    }
  }

  /**
   * Force flush all queues immediately - useful for testing
   */
  public async forceFlushAll(): Promise<void> {
    await this.flushRemaining();
  }

  /** Total buffered rows for a table (open + sealed). */
  private rowCount(table: TableName): number {
    const buf = this.queue[table];
    let n = buf.open.length;
    for (const b of buf.sealed) n += b.rows.length;
    return n;
  }

  /**
   * Flattened view of all buffered rows for a table (sealed batches oldest
   * first, then the open batch). Used by tests to inspect queue state.
   */
  private queuedRows(table: TableName): QueuedRow[] {
    const buf = this.queue[table];
    const rows: QueuedRow[] = [];
    for (const b of buf.sealed) rows.push(...b.rows);
    rows.push(...buf.open);
    return rows;
  }
}

export enum TableName {
  Traces = "traces",
  Scores = "scores",
  Observations = "observation_source",
  BlobStorageFileLog = "blob_storage_file_log",
  DatasetRunItems = "dataset_run_items_rmt",
  EventsFull = "events_full",
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

// A sealed (full or staleness-sealed) batch ready to write once readyAt passes.
type SealedBatch = {
  rows: QueuedRow[];
  bytes: number;
  readyAt: number;
};

// Per-table chunk buffer: one open batch being filled + a FIFO of sealed
// batches. bufferedBytes = open bytes + all sealed bytes (drives backpressure).
type TableBuffer = {
  open: QueuedRow[];
  openBytes: number;
  sealed: SealedBatch[];
  bufferedBytes: number;
};

const emptyBuffer = (): TableBuffer => ({
  open: [],
  openBytes: 0,
  sealed: [],
  bufferedBytes: 0,
});

type DorisQueue = {
  [T in TableName]: TableBuffer;
};
