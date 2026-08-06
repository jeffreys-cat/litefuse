import { convertDateToAnalyticsDateTime } from "./analytics";
import { queryDoris } from "./doris";
import { executeDorisProjectFanout } from "../doris/crossProjectTableRouting";

export type BillingUnitCountByProjectAndDay = {
  projectId: string;
  date: string;
  traces: number;
  observations: number;
  scores: number;
  total: number;
};

export async function getBillingUnitCountsByProjectAndDay(params: {
  start: Date;
  end: Date;
  projectIds: string[];
}): Promise<BillingUnitCountByProjectAndDay[]> {
  const [eventRows, scoreRows] = await Promise.all([
    executeDorisProjectFanout<{
      project_id: string;
      date: string;
      traces: string;
      observations: string;
    }>({
      logicalTable: "events_full",
      projectIds: params.projectIds,
      queryTarget: (target) =>
        queryDoris({
          query: `
            SELECT project_id, CAST(created_at AS DATE) AS date,
              SUM(CASE WHEN is_root = 1 THEN 1 ELSE 0 END) AS traces,
              COUNT(*) AS observations
            FROM \`${target.physicalTable}\`
            WHERE project_id IN ({projectIds: Array(String)})
              AND created_at >= {start: DateTime}
              AND created_at < {end: DateTime}
            GROUP BY project_id, CAST(created_at AS DATE)
          `,
          params: {
            projectIds: target.projectIds,
            start: convertDateToAnalyticsDateTime(params.start),
            end: convertDateToAnalyticsDateTime(params.end),
          },
          tags: { feature: "billing", type: "units", kind: "analytic" },
        }),
    }),
    queryDoris<{ project_id: string; date: string; scores: string }>({
      query: `
        SELECT
          project_id,
          CAST(created_at AS DATE) AS date,
          COUNT(*) AS scores
        FROM scores
        WHERE project_id IN ({projectIds: Array(String)})
          AND created_at >= {start: DateTime}
          AND created_at < {end: DateTime}
        GROUP BY project_id, CAST(created_at AS DATE)
      `,
      params: {
        projectIds: params.projectIds,
        start: convertDateToAnalyticsDateTime(params.start),
        end: convertDateToAnalyticsDateTime(params.end),
      },
      tags: { feature: "billing", type: "units", kind: "analytic" },
    }),
  ]);

  const counts = new Map<string, BillingUnitCountByProjectAndDay>();
  for (const row of eventRows) {
    const key = `${row.project_id}:${row.date}`;
    const traces = Number(row.traces);
    const observations = Number(row.observations);
    counts.set(key, {
      projectId: row.project_id,
      date: row.date,
      traces,
      observations,
      scores: 0,
      total: traces + observations,
    });
  }
  for (const row of scoreRows) {
    const key = `${row.project_id}:${row.date}`;
    const scores = Number(row.scores);
    const existing = counts.get(key) ?? {
      projectId: row.project_id,
      date: row.date,
      traces: 0,
      observations: 0,
      scores: 0,
      total: 0,
    };
    existing.scores = scores;
    existing.total += scores;
    counts.set(key, existing);
  }
  return [...counts.values()];
}
