import { describe, it, expect } from "vitest";

import { TraceMetricsRepairRunner } from "./index";

const T = (iso: string) => new Date(iso).getTime();

describe("TraceMetricsRepairRunner.nextOccurrence", () => {
  it("fires later today when the configured time is still ahead (UTC)", () => {
    expect(
      TraceMetricsRepairRunner.nextOccurrence("02:00", T("2026-07-09T01:00:00Z")),
    ).toBe(T("2026-07-09T02:00:00Z"));
  });

  it("fires tomorrow when the configured time already passed today", () => {
    expect(
      TraceMetricsRepairRunner.nextOccurrence("02:00", T("2026-07-09T03:00:00Z")),
    ).toBe(T("2026-07-10T02:00:00Z"));
  });

  it("is strictly-after: exactly at the configured time schedules tomorrow (a due run advances to the NEXT day, no double-fire)", () => {
    expect(
      TraceMetricsRepairRunner.nextOccurrence("02:00", T("2026-07-09T02:00:00Z")),
    ).toBe(T("2026-07-10T02:00:00Z"));
  });

  it("handles day/month rollover (23:59 late in the day, end of month)", () => {
    expect(
      TraceMetricsRepairRunner.nextOccurrence("23:59", T("2026-07-31T23:59:30Z")),
    ).toBe(T("2026-08-01T23:59:00Z"));
  });
});
