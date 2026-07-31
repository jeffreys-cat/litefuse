import {
  isForeignSplitTable,
  findForeignSplitTables,
  filterVisibleTables,
} from "@/src/features/discover/server/queryUtils";

const OWN = "cmqiwxsca0006pj070fdkn0vd";
const OTHER = "cmqiwxsca0006pj070fdkn0ve";

describe("Discover split-table access control (Stage 1.7)", () => {
  describe("isForeignSplitTable", () => {
    it("another project's split table is foreign", () => {
      expect(isForeignSplitTable(`events_full_${OTHER}`, OWN)).toBe(true);
      expect(isForeignSplitTable(`traces_scalar_${OTHER}`, OWN)).toBe(true);
      expect(isForeignSplitTable(`trace_metrics_agg_${OTHER}`, OWN)).toBe(true);
    });
    it("own split table is NOT foreign", () => {
      expect(isForeignSplitTable(`events_full_${OWN}`, OWN)).toBe(false);
      expect(isForeignSplitTable(`traces_scalar_${OWN}`, OWN)).toBe(false);
    });
    it("shared/other tables are never foreign", () => {
      expect(isForeignSplitTable("events_full", OWN)).toBe(false);
      expect(isForeignSplitTable("traces_scalar", OWN)).toBe(false);
      expect(isForeignSplitTable("scores", OWN)).toBe(false);
      expect(isForeignSplitTable("dataset_run_items_rmt", OWN)).toBe(false);
    });
  });

  describe("findForeignSplitTables", () => {
    it("catches a foreign table in the top-level FROM", () => {
      expect(
        findForeignSplitTables(`SELECT * FROM events_full_${OTHER}`, OWN),
      ).toEqual([`events_full_${OTHER}`]);
    });
    it("catches foreign tables in subqueries and JOINs", () => {
      const sql = `SELECT * FROM (SELECT id FROM traces_scalar_${OTHER}) t JOIN events_full_${OTHER} e ON e.id = t.id`;
      const found = findForeignSplitTables(sql, OWN);
      expect(found).toContain(`traces_scalar_${OTHER}`);
      expect(found).toContain(`events_full_${OTHER}`);
    });
    it("allows own split tables and shared tables", () => {
      expect(
        findForeignSplitTables(
          `SELECT * FROM events_full_${OWN} JOIN scores s ON s.trace_id = id`,
          OWN,
        ),
      ).toEqual([]);
      expect(findForeignSplitTables(`SELECT * FROM events_full`, OWN)).toEqual(
        [],
      );
    });
  });

  describe("filterVisibleTables", () => {
    it("hides other projects' split tables from SHOW TABLES", () => {
      const rows = [
        { Tables_in_litefuse: "events_full" },
        { Tables_in_litefuse: `events_full_${OWN}` },
        { Tables_in_litefuse: `events_full_${OTHER}` },
        { Tables_in_litefuse: `traces_scalar_${OTHER}` },
        { Tables_in_litefuse: "scores" },
      ];
      const visible = filterVisibleTables(rows, OWN).map(
        (r) => r.Tables_in_litefuse,
      );
      expect(visible).toEqual([
        "events_full",
        `events_full_${OWN}`,
        "scores",
      ]);
    });
  });
});
