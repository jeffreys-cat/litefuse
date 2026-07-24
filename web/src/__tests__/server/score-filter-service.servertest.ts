import { v4 } from "uuid";
import {
  commandDoris,
  applyScoreFilters,
  isScoreFilterColumn,
  getTracesTable,
} from "@langfuse/shared/src/server";
import { type FilterState } from "@langfuse/shared";

// applyScoreFilters pre-resolves score filters (which live in the `scores` table)
// into a trace_id IN filter so the trace-list query stays on the traces_mv fast
// path instead of JOINing scores. These tests seed events_full + scores directly
// into Doris (the fork has no CK-style createTracesCh helper) and assert the
// resolver returns the right trace_ids and that end-to-end getTracesTable then
// returns only the matching traces.
describe("applyScoreFilters (score filter -> trace_id resolution)", () => {
  const now = new Date();
  const dateStr = now.toISOString().slice(0, 10); // YYYY-MM-DD
  const dtStr = now.toISOString().slice(0, 19).replace("T", " "); // datetime(3)

  // Seed a root span (is_root=1) so the trace shows up in getTracesTable.
  const seedTrace = async (projectId: string, traceId: string) => {
    await commandDoris({
      query: `INSERT INTO events_full
        (project_id, trace_id, start_time_date, span_id, parent_span_id, is_root,
         start_time, end_time, name, environment, event_ts, is_deleted)
        VALUES
        ('${projectId}', '${traceId}', '${dateStr}', 't-${traceId}', '', 1,
         '${dtStr}', '${dtStr}', 'test', 'default', '${dtStr}', 0)`,
    });
  };

  const seedScore = async (
    projectId: string,
    traceId: string,
    name: string,
    value: number,
  ) => {
    await commandDoris({
      query: `INSERT INTO scores
        (project_id, timestamp_date, id, trace_id, observation_id, name, value,
         data_type, timestamp, source, event_ts, is_deleted)
        VALUES
        ('${projectId}', '${dateStr}', '${v4()}', '${traceId}', NULL, '${name}', ${value},
         'NUMERIC', '${dtStr}', 'API', '${dtStr}', 0)`,
    });
  };

  const numericScoreFilter = (
    key: string,
    operator: ">" | "<" | ">=" | "<=" | "=",
    value: number,
  ): FilterState => [
    { type: "numberObject", column: "scores_avg", key, operator, value },
  ];

  const findIdInFilter = (filterState: FilterState) =>
    filterState.find(
      (f) =>
        f.type === "stringOptions" &&
        f.column === "id" &&
        f.operator === "any of",
    ) as { value: string[] } | undefined;

  it("passes filterState through unchanged when there are no score filters", async () => {
    const projectId = v4();
    const input: FilterState = [
      { type: "string", column: "userId", operator: "=", value: "u1" },
    ];

    const res = await applyScoreFilters({ filterState: input, projectId });

    expect(res.hasNoMatches).toBe(false);
    expect(res.filterState).toEqual(input);
  });

  it("resolves a numeric score filter to the matching trace_ids", async () => {
    const projectId = v4();
    const scoreName = "accuracy-" + v4();
    const highTrace = v4();
    const lowTrace = v4();

    await seedTrace(projectId, highTrace);
    await seedTrace(projectId, lowTrace);
    await seedScore(projectId, highTrace, scoreName, 90);
    await seedScore(projectId, lowTrace, scoreName, 10);

    const res = await applyScoreFilters({
      filterState: numericScoreFilter(scoreName, ">", 50),
      projectId,
    });

    expect(res.hasNoMatches).toBe(false);
    // The original score filter must be removed.
    expect(res.filterState.some((f) => isScoreFilterColumn(f.column))).toBe(
      false,
    );
    const idIn = findIdInFilter(res.filterState);
    expect(idIn).toBeDefined();
    expect(idIn!.value).toContain(highTrace);
    expect(idIn!.value).not.toContain(lowTrace);
  });

  it("returns hasNoMatches when the score filter matches nothing", async () => {
    const projectId = v4();
    const scoreName = "nomatch-" + v4();
    const traceId = v4();

    await seedTrace(projectId, traceId);
    await seedScore(projectId, traceId, scoreName, 10);

    const res = await applyScoreFilters({
      filterState: numericScoreFilter(scoreName, ">", 1000),
      projectId,
    });

    expect(res.hasNoMatches).toBe(true);
  });

  it("end-to-end: getTracesTable with the resolved filter returns only matching traces", async () => {
    const projectId = v4();
    const scoreName = "e2e-" + v4();
    const matchTrace = v4();
    const otherTrace = v4();

    await seedTrace(projectId, matchTrace);
    await seedTrace(projectId, otherTrace);
    await seedScore(projectId, matchTrace, scoreName, 95);
    await seedScore(projectId, otherTrace, scoreName, 5);

    const { filterState, hasNoMatches } = await applyScoreFilters({
      filterState: numericScoreFilter(scoreName, ">=", 90),
      projectId,
    });
    expect(hasNoMatches).toBe(false);

    const rows = await getTracesTable({
      projectId,
      filter: filterState,
      searchQuery: undefined,
      orderBy: { column: "timestamp", order: "DESC" },
      limit: 50,
      page: 0,
    });

    const ids = rows.map((r) => r.id);
    expect(ids).toContain(matchTrace);
    expect(ids).not.toContain(otherTrace);
  });
});
