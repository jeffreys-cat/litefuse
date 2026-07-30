import { describe, it, expect, beforeEach, vi } from "vitest";

// env is module-level-parsed; a hoisted holder lets each case flip the mode.
const { envMock } = vi.hoisted(() => ({
  envMock: { LITEFUSE_DORIS_TABLE_SPLIT_MODE: "none" } as {
    LITEFUSE_DORIS_TABLE_SPLIT_MODE: string;
  },
}));
vi.mock("../../../env", () => ({ env: envMock }));

import {
  isSplitProject,
  tableFor,
  metricsAggTableFor,
  laneFor,
} from "../tableRouting";

const PID = "cmqiwxsca0006pj070fdkn0vd";

beforeEach(() => {
  envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "none";
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

  describe("mode=project_id_with_rule (Stage 0 stub — control table absent)", () => {
    beforeEach(() => {
      envMock.LITEFUSE_DORIS_TABLE_SPLIT_MODE = "project_id_with_rule";
    });

    it("isSplitProject is false until Stage 1 wires the control table", () => {
      expect(isSplitProject(PID)).toBe(false);
      expect(tableFor(PID, "events_full")).toBe("events_full");
      expect(laneFor(PID)).toBeNull();
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
