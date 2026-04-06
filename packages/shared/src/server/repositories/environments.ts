import { queryDoris } from "./doris";

export type EnvironmentFilterProps = {
  projectId: string;
  fromTimestamp?: Date;
};

export const getEnvironmentsForProject = async (
  props: EnvironmentFilterProps,
): Promise<{ environment: string }[]> => {
  const { projectId, fromTimestamp } = props;

  const query = `
      SELECT DISTINCT environment FROM (
        SELECT DISTINCT environment
        FROM traces
        WHERE project_id = {projectId: String}
        ${fromTimestamp ? "AND timestamp >= {fromTimestamp: DateTime64(3)}" : ""}
        UNION ALL
        SELECT DISTINCT environment
        FROM observations
        WHERE project_id = {projectId: String}
        ${fromTimestamp ? "AND start_time >= {fromTimestamp: DateTime64(3)}" : ""}
        UNION ALL
        SELECT DISTINCT environment
        FROM scores
        WHERE project_id = {projectId: String}
        ${fromTimestamp ? "AND timestamp >= {fromTimestamp: DateTime64(3)}" : ""}
      ) t
    `;

  const results = await queryDoris<{
    environment: string;
  }>({
    query,
    params: { projectId, fromTimestamp },
    tags: {
      feature: "tracing",
      type: "environment",
      kind: "byId",
      projectId,
    },
  });

  // Always add default environment to list
  results.push({ environment: "default" });

  return Array.from(new Set(results.map((e) => e.environment))).map(
    (environment) => ({
      environment,
    }),
  );
};
