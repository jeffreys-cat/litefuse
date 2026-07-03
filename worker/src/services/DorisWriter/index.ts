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
 * Buffers rows per table (a plain FIFO array per table) and writes them to
 * Doris via Stream Load with a bounded worker pool:
 *
 *  - addToQueue serializes each row once and pushes it to its table's array.
 *  - A drain pool of at most maxConcurrentLoads workers pulls a batch
 *    (splice batchSize from the front) from a "ready" table and writes it —
 *    ready = the array holds a full batch, or its oldest row is older than
 *    writeInterval (staleness, so low-volume tables still flush on time).
 *  - Backpressure: addToQueue awaits when a table's buffered bytes reach
 *    maxQueueSizeBytes, released when a drain frees space. Total worker memory
 *    is bounded by ~maxQueueSizeBytes (buffered) + maxConcurrentLoads * batch
 *    (in flight); the rest of the backlog stays in BullMQ/Redis.
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
  queueSizeBytes: Map<TableName, number>;

  // Worker-pool state. At most maxConcurrentLoads drain workers run at once.
  private activeWorkers = 0;
  // Rotating start index for pickReadyTable's round-robin scan (anti-starvation).
  private drainCursor = 0;
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
      [TableName.Traces]: [],
      [TableName.Scores]: [],
      [TableName.Observations]: [],
      [TableName.BlobStorageFileLog]: [],
      [TableName.DatasetRunItems]: [],
      [TableName.EventsFull]: [],
    };

    this.queueSizeBytes = new Map();

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

    // Periodic queue gauge — one log line per table per window, but
    // skip tables that are completely silent (q=0 and no add/flush in
    // the window). Format: `q=<rows> +<added> -<flushed>`.
    const gaugeWindowSec = Math.round(this.gaugeInterval / 1000);
    this.gaugeIntervalId = setInterval(() => {
      for (const t of Object.values(TableName)) {
        const len = this.queue[t]?.length ?? 0;
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
    // Backpressure: block while this table's buffered bytes are at the hard
    // cap. Released by notifyBufferDrained() when a drain reduces the bytes.
    // Awaiting (not busy-polling) yields the event loop, so BullMQ job locks
    // keep renewing while intake is throttled and the backlog stays in Redis.
    while (
      (this.queueSizeBytes.get(tableName) ?? 0) >= this.maxQueueSizeBytes
    ) {
      await new Promise<void>((resolve) => this.bufferWaiters.push(resolve));
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
      const items = this.queue[table].splice(0, this.batchSize);
      if (items.length === 0) return;

      const bytes = items.reduce((s, i) => s + i.estimatedSizeBytes, 0);
      this.queueSizeBytes.set(
        table,
        (this.queueSizeBytes.get(table) ?? 0) - bytes,
      );
      this.notifyBufferDrained();

      const now = Date.now();
      for (const item of items) {
        recordHistogram(
          "langfuse.queue.doris_writer.wait_time",
          now - item.createdAt,
          { unit: "milliseconds" },
        );
      }
      await this.processBatch(table, items);
    }
  }

  /**
   * The readiest table to write, or null. A table is ready when it holds a full
   * batch or its oldest row is older than writeInterval (staleness). Scans
   * round-robin from a rotating cursor so no table can be starved by an
   * earlier-in-enum-order table (events_full is last, so a fixed-order scan
   * could starve it while e.g. scores keep flowing).
   */
  private pickReadyTable(): TableName | null {
    const now = Date.now();
    const tables = Object.values(TableName);
    for (let i = 0; i < tables.length; i++) {
      const pos = (this.drainCursor + i) % tables.length;
      const q = this.queue[tables[pos]];
      const ready =
        q.length >= this.batchSize ||
        (q.length > 0 && now - q[0].createdAt >= this.writeInterval);
      if (ready) {
        this.drainCursor = (pos + 1) % tables.length;
        return tables[pos];
      }
    }
    return null;
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
    items: QueuedRow[],
  ): Promise<void> {
    return instrumentAsync(
      { name: "write-to-doris", spanKind: SpanKind.CONSUMER },
      async () => {
        recordIncrement("langfuse.queue.doris_writer.request");
        const currentSpan = getCurrentSpan();
        if (currentSpan) {
          currentSpan.setAttributes({ [`${table}-length`]: items.length });
        }

        try {
          const processingStartTime = Date.now();

          // Rows are pre-serialized; wrap them in a JSON array by joining with
          // commas — byte-identical to JSON.stringify(records), the format Doris
          // accepts with strip_outer_array=true.
          await this.writeToDoris({
            table,
            body: `[${items.map((item) => item.line).join(",")}]`,
            recordCount: items.length,
          });

          recordHistogram(
            "langfuse.queue.doris_writer.processing_time",
            Date.now() - processingStartTime,
            { unit: "milliseconds" },
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
          logger.error(`DorisWriter.processBatch ${table}`, err);

          for (const item of items) {
            // maxAttempts <= 0 means retry forever (never drop) — the default.
            if (this.maxAttempts <= 0 || item.attempts < this.maxAttempts) {
              // Reset createdAt so the re-queued row isn't immediately "stale"
              // again — this spaces DorisWriter-level retries by ~writeInterval
              // (via the staleness tick) instead of a drain worker hot-looping.
              this.queue[table].push({
                ...item,
                attempts: item.attempts + 1,
                createdAt: Date.now(),
              });
              this.queueSizeBytes.set(
                table,
                (this.queueSizeBytes.get(table) ?? 0) + item.estimatedSizeBytes,
              );
            } else {
              // TODO - Add to a dead letter queue in Redis rather than dropping
              recordIncrement("langfuse.queue.doris_writer.error");
              logger.error(
                `Max attempts reached for ${table} record. Dropping record.`,
                { line: item.line.slice(0, 500) },
              );
            }
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
   * Flush everything currently queued right now, bounded by maxConcurrentLoads.
   * Runs a few passes so rows re-queued by a failed write get another attempt
   * instead of being silently dropped — bounded so a persistently-down BE can't
   * hang shutdown. Used on shutdown and by tests.
   */
  private async flushRemaining(maxPasses = 3): Promise<void> {
    for (let pass = 0; pass < maxPasses; pass++) {
      const tasks: Promise<void>[] = [];
      for (const t of Object.values(TableName)) {
        const q = this.queue[t];
        while (q.length > 0) {
          const items = q.splice(0, this.batchSize);
          const bytes = items.reduce((s, i) => s + i.estimatedSizeBytes, 0);
          this.queueSizeBytes.set(t, (this.queueSizeBytes.get(t) ?? 0) - bytes);
          tasks.push(this.processBatch(t, items));
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

type DorisQueue = {
  [T in TableName]: QueuedRow[];
};
