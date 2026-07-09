import { queryDoris } from "./doris";

export type EnvironmentFilterProps = {
  projectId: string;
  fromTimestamp?: Date;
};

export const getEnvironmentsForProject = async (
  props: EnvironmentFilterProps,
): Promise<{ environment: string }[]> => {
  const { projectId, fromTimestamp } = props;

  // traces_scalar (one row per trace) instead of a DISTINCT over every span of
  // every partition in events_full. Environment is a trace-scoped attribute in
  // practice — readers COALESCE a span's empty environment to the root's — so
  // the root rows carry the full environment set; an environment that only
  // ever appears on child spans and never on any root would be missed (not a
  // shape real SDK exports produce).
  const query = `
      SELECT DISTINCT environment FROM (
        SELECT DISTINCT environment
        FROM traces_scalar
        WHERE project_id = {projectId: String}
        ${fromTimestamp ? "AND start_time >= {fromTimestamp: DateTime}" : ""}
        UNION ALL
        SELECT DISTINCT environment
        FROM scores
        WHERE project_id = {projectId: String}
        ${fromTimestamp ? "AND timestamp >= {fromTimestamp: DateTime}" : ""}
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
