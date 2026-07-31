import { describe, it, expect, beforeEach, vi } from "vitest";

// env is module-level-parsed; a hoisted holder lets each case flip the mode.
const { envMock, splitCacheMock } = vi.hoisted(() => ({
  envMock: { LITEFUSE_DORIS_TABLE_SPLIT_MODE: "none" } as {
    LITEFUSE_DORIS_TABLE_SPLIT_MODE: string;
  },
  splitCacheMock: { members: new Set<string>() },
}));
vi.mock("../../../env", () => ({ env: envMock }));
// Mock the split cache so project_id_with_rule can be driven without PG.
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
  envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "none";
  splitCacheMock.members = new Set<string>();
});

describe("tableRouting", () => {
  describe("mode=none (Stage 0 identity — zero behaviour change)", () => {
    it("isSplitProject is false", () => {
      expect(isSplitProject(PID)).toBe(false);
    });

    it("tableFor returns the shared logical name for splittable tables", () => {
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

  describe("mode=project_id (every project split)", () => {
    beforeEach(() => {
      envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "project_id";
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
  });

  describe("mode=project_id_with_rule (control table via cache)", () => {
    beforeEach(() => {
      envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "project_id_with_rule";
    });

    it("project NOT in the split cache → not split (cold cache reads shared)", () => {
      expect(isSplitProject(PID)).toBe(false);
      expect(tableFor(PID, "events_full")).toBe("events_full");
      expect(laneFor(PID)).toBeNull();
    });

    it("project IN the split cache → split", () => {
      splitCacheMock.members = new Set([PID]);
      expect(isSplitProject(PID)).toBe(true);
      expect(tableFor(PID, "events_full")).toBe(`events_full_${PID}`);
      expect(tableFor(PID, "traces_scalar")).toBe(`traces_scalar_${PID}`);
      expect(metricsAggTableFor(PID)).toBe(`trace_metrics_agg_${PID}`);
      expect(laneFor(PID)).toBe(`lane-${PID}`);
    });

    it("only cached projects split — a different project stays shared", () => {
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

    it("round-trips tableFor in both modes", () => {
      envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "none";
      expect(toLogicalTable(tableFor(PID, "events_full"))).toBe("events_full");
      envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "project_id";
      expect(toLogicalTable(tableFor(PID, "events_full"))).toBe("events_full");
      expect(toLogicalTable(tableFor(PID, "traces_scalar"))).toBe(
        "traces_scalar",
      );
    });
  });

  describe("sharedTableFor (cross-project escape hatch)", () => {
    it("returns the logical name in every split mode (always shared)", () => {
      for (const mode of ["none", "project_id", "project_id_with_rule"]) {
        envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = mode;
        expect(sharedTableFor("events_full")).toBe("events_full");
        expect(sharedTableFor("traces_scalar")).toBe("traces_scalar");
      }
    });
  });

  describe("non-splittable tables are never suffixed", () => {
    it("returns the logical name regardless of split mode", () => {
      for (const mode of ["none", "project_id", "project_id_with_rule"]) {
        envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = mode;
        expect(tableFor(PID, "traces")).toBe("traces");
        expect(tableFor(PID, "observations")).toBe("observations");
        expect(tableFor(PID, "scores")).toBe("scores");
      }
    });
  });
});
