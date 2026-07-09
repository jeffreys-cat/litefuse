import {
  commandDoris,
  logger,
  queryDoris,
  redis,
} from "@langfuse/shared/src/server";

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
 * the exact recomputation again.
 *
 * Scheduling: ONCE per day at a wall-clock time in the DORIS SERVER'S
 * TIMEZONE (SELECT @@time_zone, resolved once at the first wake-up) — the
 * operator sets LITEFUSE_TRACE_METRICS_REPAIR_AT (HH:MM); unset it defaults
 * to 00:00, i.e. midnight of the database's own day. NOT an interval from
 * worker boot. Every worker arms a timer for the next occurrence (a boot
 * mid-day does NOT trigger a run). Once-per-CLUSTER needs two mechanisms,
 * because worker replicas neither boot together nor share a clock:
 *   1. the Redis lock excludes CONCURRENT runs (replicas waking within the
 *      same window elect one winner; losers advance to tomorrow), and
 *   2. a per-occurrence done-marker (checked inside the lock, set after a
 *      successful run) excludes SEQUENTIAL re-runs — with clock skew a
 *      replica can wake after the winner already finished and released the
 *      lock, and without the marker it would repeat the whole recompute.
 * The lock uses onUnavailable "fail": if Redis is down at fire time, every
 * replica skips (the day stays realtime-approximate) instead of all of them
 * stampeding the same INSERT OVERWRITE concurrently.
 * A crash mid-repair leaves no marker, so a later-waking replica retries the
 * same occurrence; if none wakes, the day stays approximate until the next
 * scheduled run (raise DAYS_BACK to make the next run re-cover it).
 *
 * Known edge (verified): if a day is entirely EMPTY in events_full, the
 * SELECT yields no rows and PARTITION(*) detects nothing — a non-empty agg
 * partition for such a day would be left untouched. Unreachable in normal
 * operation (every agg increment originates from a span written to
 * events_full with the same derived date, and deletes are mirrored), so not
 * special-cased.
 */
export class TraceMetricsRepairRunner extends PeriodicExclusiveRunner {
  // Epoch-ms of the next scheduled fire; 0 = schedule not armed yet (the
  // Doris timezone hasn't been resolved). execute() runs at boot
  // (PeriodicRunner semantics) and whenever a timer expires; anything before
  // this timestamp is a non-due wake-up and only re-arms the timer.
  private nextFireAt = 0;
  // Doris server timezone (SELECT @@time_zone), resolved at the first wake-up
  // and cached for the process lifetime.
  private dorisTz: string | null = null;

  constructor() {
    super({
      name: "trace-metrics-repair",
      lockKey: TRACE_METRICS_REPAIR_LOCK_KEY,
      // Recomputing a day of events_full at target scale (billions of spans)
      // can take a while — hold the lock generously.
      lockTtlSeconds: 3600,
      // Redis down at fire time → skip the day (stays realtime-approximate)
      // rather than every replica running the recompute concurrently.
      onUnavailable: "fail",
    });
  }

  /**
   * Next epoch-ms strictly after `nowMs` at which the wall clock in `tz`
   * reads HH:MM. `tz` is an IANA name ("Asia/Shanghai", "Etc/UTC") or a fixed
   * offset ("+08:00") — both shapes Doris's @@time_zone can return.
   *
   * The computed instant is skew-independent: a replica whose clock is off by
   * s seconds sees both `nowMs` and the formatted wall clock shifted by the
   * same s, which cancels — all replicas agree on the fire instant (to the
   * second; sub-second is truncated for exactly that reason), so the
   * done-marker key derived from it matches across the fleet.
   */
  static nextOccurrenceInTz(hhmm: string, tz: string, nowMs: number): number {
    const [h, m] = hhmm.split(":").map(Number);
    const targetSecs = h * 3600 + m * 60;

    let nowSecs: number;
    const offset = /^([+-])(\d{2}):(\d{2})$/.exec(tz);
    if (offset) {
      const offMs =
        (offset[1] === "-" ? -1 : 1) *
        (Number(offset[2]) * 3600 + Number(offset[3]) * 60) *
        1000;
      const d = new Date(nowMs + offMs);
      nowSecs =
        d.getUTCHours() * 3600 + d.getUTCMinutes() * 60 + d.getUTCSeconds();
    } else {
      // IANA zone via Intl (throws RangeError on an unknown zone — the caller
      // falls back to UTC with a warning rather than not scheduling at all).
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: tz,
        hourCycle: "h23",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).formatToParts(new Date(nowMs));
      const get = (t: string) =>
        Number(parts.find((p) => p.type === t)?.value ?? 0);
      nowSecs = (get("hour") % 24) * 3600 + get("minute") * 60 + get("second");
    }

    let deltaSecs = targetSecs - nowSecs;
    if (deltaSecs <= 0) deltaSecs += 24 * 3600;
    return Math.floor(nowMs / 1000) * 1000 + deltaSecs * 1000;
  }

  // Fallback cadence only — execute() always returns an explicit delay, so
  // this is never used to pace the schedule.
  protected get defaultIntervalMs(): number {
    return 60 * 60 * 1000;
  }

  private computeNextFire(nowMs: number): number {
    try {
      return TraceMetricsRepairRunner.nextOccurrenceInTz(
        env.LITEFUSE_TRACE_METRICS_REPAIR_AT,
        this.dorisTz ?? "Etc/UTC",
        nowMs,
      );
    } catch (e) {
      logger.warn(
        `[TraceMetricsRepair] cannot interpret Doris timezone '${this.dorisTz}' (${e instanceof Error ? e.message : String(e)}); scheduling in UTC`,
      );
      this.dorisTz = "Etc/UTC";
      return TraceMetricsRepairRunner.nextOccurrenceInTz(
        env.LITEFUSE_TRACE_METRICS_REPAIR_AT,
        "Etc/UTC",
        nowMs,
      );
    }
  }

  protected async execute(): Promise<number> {
    const now = Date.now();

    // First wake-up (boot): resolve the Doris timezone, then arm the schedule
    // — never run at boot. A failed resolution retries in 5 minutes (the
    // schedule must not silently fall back to the wrong day boundary).
    if (this.nextFireAt === 0) {
      try {
        const rows = await queryDoris<{ tz: string }>({
          query: "SELECT @@time_zone AS tz",
          tags: { feature: "tracing", type: "trace-metrics-repair" },
        });
        this.dorisTz = rows[0]?.tz || "Etc/UTC";
      } catch (e) {
        logger.warn(
          `[TraceMetricsRepair] failed to resolve Doris timezone, retrying in 5m: ${e instanceof Error ? e.message : String(e)}`,
        );
        return 5 * 60 * 1000;
      }
      this.nextFireAt = this.computeNextFire(now);
      logger.info(
        `[TraceMetricsRepair] scheduled daily at ${env.LITEFUSE_TRACE_METRICS_REPAIR_AT} ${this.dorisTz} — next run ${new Date(this.nextFireAt).toISOString()}`,
      );
      return this.nextFireAt - now;
    }

    // Not due (timer jitter): sleep until the scheduled time. 1s epsilon
    // absorbs setTimeout firing marginally early.
    if (now < this.nextFireAt - 1000) {
      return this.nextFireAt - now;
    }

    // The occurrence this wake-up serves, BEFORE advancing — keys the
    // done-marker. Minute precision (not date): fire instants are
    // skew-independent, and minute granularity keeps two same-UTC-date
    // occurrences distinct if the operator moves REPAIR_AT during the day.
    const occurrence = new Date(this.nextFireAt).toISOString().slice(0, 16);

    // Advance the schedule BEFORE running so a failure can't hot-loop — win
    // or lose the lock, this worker's next attempt is tomorrow.
    this.nextFireAt = this.computeNextFire(now);

    await this.withLock(async () => {
      // Sequential-rerun guard (see class doc): a skew-late replica arriving
      // after the winner finished must not repeat the recompute. Checked
      // inside the lock → no check-then-run race.
      const markerKey = `${TRACE_METRICS_REPAIR_LOCK_KEY}:done:${occurrence}`;
      if (await redis?.get(markerKey)) {
        logger.info(
          `[TraceMetricsRepair] occurrence ${occurrence} already repaired by another worker; skipping`,
        );
        return;
      }
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

      // Mark AFTER success: a crash mid-repair leaves no marker, so a
      // later-waking replica retries this occurrence. 48h TTL comfortably
      // outlives any clock skew while self-cleaning.
      await redis?.set(markerKey, new Date().toISOString(), "EX", 48 * 3600);
    });

    return Math.max(1000, this.nextFireAt - Date.now());
  }
}
