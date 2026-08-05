import { describe, it, expect, beforeEach, vi } from "vitest";

// Table split is universal: isSplitProject reads ONLY the split cache (a project
// is split once it is LIVE in the control-table snapshot). Mock the cache so the
// live/not-live decision can be driven without PG.
const { splitCacheMock } = vi.hoisted(() => ({
  splitCacheMock: { members: new Set<string>() },
}));
vi.mock("../tableSplitCache", () => ({
  splitProjectInCache: (projectId: string) =>
    splitCacheMock.members.has(projectId),
}));

import {
  isSplitProject,
  tableFor,
  metricsAggTableFor,
  laneFor,
  toLogicalTable,
  sharedTableFor,
} from "../tableRouting";

const PID = "cmqiwxsca0006pj070fdkn0vd";

beforeEach(() => {
  splitCacheMock.members = new Set<string>();
});

describe("tableRouting", () => {
  describe("project NOT live in the split cache (pre-provision / pre-backfill)", () => {
    it("isSplitProject is false → shared tables", () => {
      expect(isSplitProject(PID)).toBe(false);
      expect(tableFor(PID, "events_full")).toBe("events_full");
      expect(tableFor(PID, "traces_scalar")).toBe("traces_scalar");
    });

    it("metricsAggTableFor returns the shared MV name", () => {
      expect(metricsAggTableFor(PID)).toBe("trace_metrics_agg");
    });

    it("laneFor returns null (shared shard pool)", () => {
      expect(laneFor(PID)).toBeNull();
    });
  });

  describe("project LIVE in the split cache", () => {
    beforeEach(() => {
      splitCacheMock.members = new Set([PID]);
    });

    it("isSplitProject is true", () => {
      expect(isSplitProject(PID)).toBe(true);
    });

    it("tableFor suffixes splittable tables with the projectId", () => {
      expect(tableFor(PID, "events_full")).toBe(`events_full_${PID}`);
      expect(tableFor(PID, "traces_scalar")).toBe(`traces_scalar_${PID}`);
    });

    it("metricsAggTableFor suffixes the MV", () => {
      expect(metricsAggTableFor(PID)).toBe(`trace_metrics_agg_${PID}`);
    });

    it("laneFor returns the dedicated lane", () => {
      expect(laneFor(PID)).toBe(`lane-${PID}`);
    });

    it("only live projects split — a different project stays shared", () => {
      splitCacheMock.members = new Set(["other-project"]);
      expect(isSplitProject(PID)).toBe(false);
      expect(tableFor(PID, "events_full")).toBe("events_full");
    });
  });

  describe("toLogicalTable (reverse of tableFor)", () => {
    it("strips the projectId suffix from split physical names", () => {
      expect(toLogicalTable(`events_full_${PID}`)).toBe("events_full");
      expect(toLogicalTable(`traces_scalar_${PID}`)).toBe("traces_scalar");
      expect(toLogicalTable(`trace_metrics_agg_${PID}`)).toBe(
        "trace_metrics_agg",
      );
    });

    it("returns shared/unknown names unchanged (identity)", () => {
      expect(toLogicalTable("events_full")).toBe("events_full");
      expect(toLogicalTable("traces_scalar")).toBe("traces_scalar");
      expect(toLogicalTable("scores")).toBe("scores");
      expect(toLogicalTable("trace_metrics_agg")).toBe("trace_metrics_agg");
    });

    it("round-trips tableFor whether live or not", () => {
      expect(toLogicalTable(tableFor(PID, "events_full"))).toBe("events_full");
      splitCacheMock.members = new Set([PID]);
      expect(toLogicalTable(tableFor(PID, "events_full"))).toBe("events_full");
      expect(toLogicalTable(tableFor(PID, "traces_scalar"))).toBe(
        "traces_scalar",
      );
    });
  });

  describe("sharedTableFor (cross-project escape hatch)", () => {
    it("returns the logical name whether or not the project is split", () => {
      for (const members of [new Set<string>(), new Set([PID])]) {
        splitCacheMock.members = members;
        expect(sharedTableFor("events_full")).toBe("events_full");
        expect(sharedTableFor("traces_scalar")).toBe("traces_scalar");
      }
    });
  });

  describe("non-splittable tables are never suffixed", () => {
    it("returns the logical name even for a live split project", () => {
      splitCacheMock.members = new Set([PID]);
      expect(tableFor(PID, "traces")).toBe("traces");
      expect(tableFor(PID, "observations")).toBe("observations");
      expect(tableFor(PID, "scores")).toBe("scores");
    });
  });
});
