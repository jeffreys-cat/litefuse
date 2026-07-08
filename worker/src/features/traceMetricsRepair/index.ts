import { commandDoris, logger } from "@langfuse/shared/src/server";

import { env } from "../../env";
import { PeriodicExclusiveRunner } from "../../utils/PeriodicExclusiveRunner";

export const TRACE_METRICS_REPAIR_LOCK_KEY = "langfuse:trace-metrics-repair";

/**
 * T+1 repair for trace_metrics_agg (migration 0040).
 *
 * The agg table is maintained by realtime per-span increments; writer retries
 * are exactly-once (stable labels), but residual duplication — BullMQ job
 * replay after a worker crash, OTel client re-delivery (no merge layer on the
 * OTel path) — can double-apply SUM columns for the CURRENT day. This runner
 * makes history exact: it recomputes recent CLOSED day-partitions from
 * events_full (MoW unique key → exact ground truth) via
 * INSERT OVERWRITE ... PARTITION(*), whose auto-detect mode atomically
 * replaces exactly the partitions the SELECT produces (verified on Doris
 * 4.0.6).
 *
 * Idempotent and safe to re-run: spans that arrive late (a span that started
 * yesterday is exported after it ends) keep folding onto the overwritten
 * partition as normal increments, and the next repair pass folds them into
 * the exact recomputation again. Multiple workers coordinate via a Redis
 * lock; only one repairs at a time.
 *
 * Known edge (verified): if a day is entirely EMPTY in events_full, the
 * SELECT yields no rows and PARTITION(*) detects nothing — a non-empty agg
 * partition for such a day would be left untouched. Unreachable in normal
 * operation (every agg increment originates from a span written to
 * events_full with the same derived date, and deletes are mirrored), so not
 * special-cased.
 */
export class TraceMetricsRepairRunner extends PeriodicExclusiveRunner {
  constructor() {
    super({
      name: "trace-metrics-repair",
      lockKey: TRACE_METRICS_REPAIR_LOCK_KEY,
      // Recomputing a day of events_full at target scale (billions of spans)
      // can take a while — hold the lock generously.
      lockTtlSeconds: 3600,
    });
  }

  protected get defaultIntervalMs(): number {
    return env.LITEFUSE_TRACE_METRICS_REPAIR_INTERVAL_MS;
  }

  protected async execute(): Promise<void> {
    await this.withLock(async () => {
      const daysBack = env.LITEFUSE_TRACE_METRICS_REPAIR_DAYS_BACK;
      // Oldest first so a mid-run failure leaves the most-stale partition
      // repaired; today (d=0) is deliberately excluded — it is the live
      // partition where realtime-approximate is the accepted contract.
      for (let d = daysBack; d >= 1; d--) {
        const day = new Date(Date.now() - d * 24 * 60 * 60 * 1000)
          .toISOString()
          .slice(0, 10);
        const startedAt = Date.now();
        // Column order MUST match the trace_metrics_agg DDL (positional
        // INSERT). WHERE start_time_date = day prunes the events_full scan to
        // the one partition; PARTITION(*) then overwrites only the agg
        // partition(s) the result touches.
        await commandDoris({
          query: `
            INSERT OVERWRITE TABLE trace_metrics_agg PARTITION(*)
            SELECT
              project_id,
              trace_id,
              start_time_date,
              SUM(input_tokens_calculated)  AS input_tokens,
              SUM(output_tokens_calculated) AS output_tokens,
              SUM(total_tokens_calculated)  AS total_tokens,
              SUM(input_cost_calculated)    AS input_cost,
              SUM(output_cost_calculated)   AS output_cost,
              SUM(total_cost)               AS total_cost,
              SUM(CASE WHEN level = 'ERROR'   THEN 1 ELSE 0 END) AS error_count,
              SUM(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
              SUM(CASE WHEN level = 'DEFAULT' THEN 1 ELSE 0 END) AS default_count,
              SUM(CASE WHEN level = 'DEBUG'   THEN 1 ELSE 0 END) AS debug_count,
              SUM(IF(is_root = 0, 1, 0))    AS observation_count,
              MIN(start_time)               AS min_start_time,
              MAX(start_time)               AS max_start_time,
              MIN(end_time)                 AS min_end_time,
              MAX(end_time)                 AS max_end_time,
              MAX(event_ts)                 AS event_ts
            FROM events_full
            WHERE trace_id IS NOT NULL
              AND start_time_date = {repairDay: String}
            GROUP BY project_id, trace_id, start_time_date
          `,
          params: { repairDay: day },
          tags: {
            feature: "tracing",
            type: "trace-metrics-repair",
          },
        });
        logger.info(
          `[TraceMetricsRepair] overwrote trace_metrics_agg partition ${day} from events_full in ${Date.now() - startedAt}ms`,
        );
      }
    });
  }
}
