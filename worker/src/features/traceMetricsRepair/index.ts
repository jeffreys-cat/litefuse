import { commandDoris, logger, queryDoris } from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { env } from "../../env";
import { PeriodicRunner } from "../../utils/PeriodicRunner";

/** cron_jobs.name row that carries this job's durable execution state. */
export const TRACE_METRICS_REPAIR_JOB_NAME = "trace_metrics_repair";

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
 * Scheduling: ONCE per day at a wall-clock time in the DORIS SERVER'S
 * TIMEZONE (SELECT @@time_zone, resolved once at the first wake-up) — the
 * operator sets LITEFUSE_TRACE_METRICS_REPAIR_AT (HH:MM); unset it defaults
 * to 00:00, i.e. midnight of the database's own day. NOT an interval from
 * worker boot: every worker arms a timer for the next occurrence, so a boot
 * mid-day never triggers a run.
 *
 * Once-per-cluster is AT-MOST-ONCE and DURABLE, via an atomic claim on the
 * Postgres cron_jobs row (NOT a Redis lock/marker): at target scale a
 * duplicate run is not "harmless idempotency" — it is a second full-day scan
 * and partition rewrite of billions of spans, i.e. an incident in itself — so
 * the design errs toward MISSING a day over ever running twice:
 *   - the claim (UPDATE ... WHERE last_run < occurrence) happens BEFORE the
 *     recompute and is transactional in PG: exactly one replica can win an
 *     occurrence, regardless of replica count, clock skew (fire instants are
 *     computed skew-independently), or how late a replica wakes;
 *   - the claim survives Redis restarts/evictions/failovers (which could
 *     erase a volatile marker and readmit a duplicate) — PG is the durable
 *     home for "this day already ran";
 *   - a claimant that crashes mid-run does NOT get retried by anyone: the
 *     occurrence stays claimed, the day stays realtime-approximate, and the
 *     crash is visible in cron_jobs (state='running', job_started_at set) for
 *     operators. Recovery is deliberate/manual: raise DAYS_BACK so the next
 *     day's run re-covers it, or run the migration's backfill statement.
 *   - PG unavailable at fire time → claim retried every 5 minutes (the claim
 *     stays atomic, so delayed attempts cannot double-run).
 *
 * Idempotent per statement and safe to re-run MANUALLY: late-arriving spans
 * keep folding onto the overwritten partition as normal increments, and the
 * next repair folds them into the exact recomputation again.
 *
 * Known edge (verified): if a day is entirely EMPTY in events_full, the
 * SELECT yields no rows and PARTITION(*) detects nothing — a non-empty agg
 * partition for such a day would be left untouched. Unreachable in normal
 * operation (every agg increment originates from a span written to
 * events_full with the same derived date, and deletes are mirrored), so not
 * special-cased.
 */
export class TraceMetricsRepairRunner extends PeriodicRunner {
  // Epoch-ms of the next scheduled fire; 0 = schedule not armed yet (the
  // Doris timezone hasn't been resolved). execute() runs at boot
  // (PeriodicRunner semantics) and whenever a timer expires; anything before
  // this timestamp is a non-due wake-up and only re-arms the timer.
  private nextFireAt = 0;
  // Doris server timezone (SELECT @@time_zone), resolved at the first wake-up
  // and cached for the process lifetime.
  private dorisTz: string | null = null;

  protected get name(): string {
    return "trace-metrics-repair";
  }

  /**
   * Next epoch-ms strictly after `nowMs` at which the wall clock in `tz`
   * reads HH:MM. `tz` is an IANA name ("Asia/Shanghai", "Etc/UTC") or a fixed
   * offset ("+08:00") — both shapes Doris's @@time_zone can return.
   *
   * The computed instant is skew-independent: a replica whose clock is off by
   * s seconds sees both `nowMs` and the formatted wall clock shifted by the
   * same s, which cancels — all replicas agree on the fire instant (to the
   * second; sub-second is truncated for exactly that reason), so the claimed
   * occurrence value derived from it matches across the fleet.
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

  /**
   * Atomically claim `occurrence` in cron_jobs. Returns true for exactly one
   * caller across the cluster; false when it (or a later occurrence) is
   * already claimed. The WHERE guard makes the row a monotonic watermark.
   */
  private async claimOccurrence(occurrence: Date): Promise<boolean> {
    // Ensure the row exists (first deployment); concurrent creates race
    // benignly — the loser's create throws on the PK and the claim below
    // still decides a single winner.
    await prisma.cronJobs
      .createMany({
        data: [{ name: TRACE_METRICS_REPAIR_JOB_NAME }],
        skipDuplicates: true,
      })
      .catch(() => undefined);

    const res = await prisma.cronJobs.updateMany({
      where: {
        name: TRACE_METRICS_REPAIR_JOB_NAME,
        OR: [{ lastRun: null }, { lastRun: { lt: occurrence } }],
      },
      data: {
        lastRun: occurrence,
        jobStartedAt: new Date(),
        state: "running",
      },
    });
    return res.count === 1;
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

    // The occurrence this wake-up serves — the cluster-wide claim value.
    // Minute precision: fire instants are skew-independent, and minute
    // granularity keeps two same-day occurrences distinct if the operator
    // moves REPAIR_AT during the day.
    const occurrence = new Date(Math.floor(this.nextFireAt / 60_000) * 60_000);

    // Claim BEFORE running and BEFORE advancing. PG down → retry the claim in
    // 5m (atomic, so a delayed attempt can never double-run); claimed by
    // another replica → advance to tomorrow and go back to sleep.
    let claimed: boolean;
    try {
      claimed = await this.claimOccurrence(occurrence);
    } catch (e) {
      logger.warn(
        `[TraceMetricsRepair] claim failed (postgres unavailable?), retrying in 5m: ${e instanceof Error ? e.message : String(e)}`,
      );
      return 5 * 60 * 1000;
    }

    this.nextFireAt = this.computeNextFire(now);

    if (!claimed) {
      logger.info(
        `[TraceMetricsRepair] occurrence ${occurrence.toISOString()} already claimed by another worker; next run ${new Date(this.nextFireAt).toISOString()}`,
      );
      return Math.max(1000, this.nextFireAt - Date.now());
    }

    try {
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
        // INSERT). WHERE DATE(start_time) = day prunes the events_full scan
        // to the one partition (events_full partitions on
        // date_trunc(start_time,'day') and the optimizer prunes through the
        // DATE() form — verified on 4.0.6); PARTITION(*) then overwrites only
        // the agg partition(s) the result touches.
        await commandDoris({
          query: `
            INSERT OVERWRITE TABLE trace_metrics_agg PARTITION(*)
            SELECT
              project_id,
              trace_id,
              DATE(start_time) AS start_time_date,
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
              AND DATE(start_time) = {repairDay: String}
            GROUP BY project_id, trace_id, DATE(start_time)
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
      await prisma.cronJobs.update({
        where: { name: TRACE_METRICS_REPAIR_JOB_NAME },
        data: { state: "ok", jobStartedAt: null },
      });
    } catch (e) {
      // AT-MOST-ONCE: the occurrence stays claimed — nobody retries a failed
      // run automatically (a second full-day recompute at scale is an
      // incident, not a retry). The day stays realtime-approximate; the
      // failure is loud here and visible in cron_jobs (state below).
      logger.error(
        `[TraceMetricsRepair] repair run for occurrence ${occurrence.toISOString()} FAILED — closed day(s) remain approximate until the next scheduled run (or manual backfill): ${e instanceof Error ? e.message : String(e)}`,
      );
      await prisma.cronJobs
        .update({
          where: { name: TRACE_METRICS_REPAIR_JOB_NAME },
          data: { state: "failed", jobStartedAt: null },
        })
        .catch(() => undefined);
    }

    return Math.max(1000, this.nextFireAt - Date.now());
  }
}
