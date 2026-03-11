import { AGGREGATABLE_SCORE_TYPES } from "../../domain/scores";
import { queryClickhouse } from "./clickhouse";
import { queryDoris } from "./doris";
import { isDorisBackend } from "./analytics";

export type EnvironmentFilterProps = {
  projectId: string;
  fromTimestamp?: Date;
};

export const getEnvironmentsForProject = async (
  props: EnvironmentFilterProps,
): Promise<{ environment: string }[]> => {
  const { projectId, fromTimestamp } = props;

  if (isDorisBackend()) {
    const query = `
      SELECT environments
      FROM project_environments
      WHERE project_id = {projectId: String}
    `;

    const results = await queryDoris<{
      environments: string[];
    }>({
      query,
      params: { projectId },
      tags: {
        feature: "tracing",
        type: "environment",
        kind: "byId",
        projectId,
      },
    });

    const environments = results.length > 0 ? results[0].environments : [];
    environments.push("default");
    return Array.from(new Set(environments)).map((environment) => ({
      environment,
    }));
  }

  const query = `
    (
      SELECT distinct environment
      FROM traces
      WHERE project_id = {projectId: String}
      ${fromTimestamp ? "AND timestamp >= {fromTimestamp: DateTime64(3)}" : ""}
    ) UNION ALL (
      SELECT distinct environment
      FROM observations
      WHERE project_id = {projectId: String}
      ${fromTimestamp ? "AND start_time >= {fromTimestamp: DateTime64(3)}" : ""}
    ) UNION ALL (
      SELECT distinct environment
      FROM scores
      WHERE project_id = {projectId: String}
      AND data_type IN ({dataTypes: Array(String)})
      ${fromTimestamp ? "AND timestamp >= {fromTimestamp: DateTime64(3)}" : ""}
    )
  `;

  const results = await queryClickhouse<{
    environment: string;
  }>({
    query,
    params: {
      projectId,
      fromTimestamp,
      dataTypes: AGGREGATABLE_SCORE_TYPES,
    },
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
