import { describe, it, expect } from "vitest";

import { TraceMetricsRepairRunner } from "./index";

const T = (iso: string) => new Date(iso).getTime();
const next = TraceMetricsRepairRunner.nextOccurrenceInTz;

describe("TraceMetricsRepairRunner.nextOccurrenceInTz", () => {
  it("UTC: fires later today when the configured time is still ahead", () => {
    expect(next("02:00", "Etc/UTC", T("2026-07-09T01:00:00Z"))).toBe(
      T("2026-07-09T02:00:00Z"),
    );
  });

  it("UTC: fires tomorrow when the configured time already passed today", () => {
    expect(next("02:00", "Etc/UTC", T("2026-07-09T03:00:00Z"))).toBe(
      T("2026-07-10T02:00:00Z"),
    );
  });

  it("is strictly-after: exactly at the configured time schedules the NEXT day (no same-day double fire)", () => {
    expect(next("02:00", "Etc/UTC", T("2026-07-09T02:00:00Z"))).toBe(
      T("2026-07-10T02:00:00Z"),
    );
  });

  it("IANA zone: default 00:00 in Asia/Shanghai = 16:00 UTC of the previous UTC day", () => {
    // 2026-07-09 15:00 UTC = 23:00 CST → next CST midnight is 16:00 UTC.
    expect(next("00:00", "Asia/Shanghai", T("2026-07-09T15:00:00Z"))).toBe(
      T("2026-07-09T16:00:00Z"),
    );
  });

  it("fixed-offset zone (+08:00 — the other shape Doris @@time_zone returns)", () => {
    expect(next("00:00", "+08:00", T("2026-07-09T15:00:00Z"))).toBe(
      T("2026-07-09T16:00:00Z"),
    );
    // Negative offset: 00:00 at -05:00 = 05:00 UTC.
    expect(next("00:00", "-05:00", T("2026-07-09T03:00:00Z"))).toBe(
      T("2026-07-09T05:00:00Z"),
    );
  });

  it("truncates sub-second so skewed replicas compute the identical fire instant", () => {
    const a = next("02:00", "Etc/UTC", T("2026-07-09T01:00:00.123Z"));
    const b = next("02:00", "Etc/UTC", T("2026-07-09T01:00:00.987Z"));
    expect(a).toBe(b);
    expect(a).toBe(T("2026-07-09T02:00:00Z"));
  });

  it("handles day/month rollover (23:59 late in the day, end of month)", () => {
    expect(next("23:59", "Etc/UTC", T("2026-07-31T23:59:30Z"))).toBe(
      T("2026-08-01T23:59:00Z"),
    );
  });

  it("throws on an unknown IANA zone (caller falls back to UTC with a warning)", () => {
    expect(() => next("00:00", "Not/AZone", Date.now())).toThrow();
  });
});
