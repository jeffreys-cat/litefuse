import {
  createPublicApiObservationsColumnMapping,
  deriveFilters,
  StringFilter,
  type ObservationRecordReadType,
  queryDoris,
  measureAndReturn,
  observationsTableUiColumnDefinitions,
  convertObservation,
  convertDateToAnalyticsDateTime,
  dq,
} from "@langfuse/shared/src/server";
import { type FilterState, observationsTableCols } from "@langfuse/shared";

type QueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  name?: string;
  type?: string;
  parentObservationId?: string;
  fromStartTime?: string;
  toStartTime?: string;
  version?: string;
  advancedFilters?: FilterState;
};

export const generateObservationsForPublicApi = async (props: QueryType) => {
  const chFilter = generateFilter(props);
  // Trace-table filters must live inside the EXISTS subquery (where `t` is in
  // scope). The fork uses EXISTS instead of LEFT JOIN to dodge a Doris Nereids
  // crash, so re-applying them in the outer WHERE references an undefined `t`
  // alias and errors with "Unknown column 'user_id' in 't'".
  const traceFilters = chFilter.filter((f) => f.table === "traces");
  const observationFilters = chFilter.filter((f) => f.table !== "traces");
  const appliedObservationFilter = observationFilters.apply();
  const appliedTraceFilter =
    traceFilters.length() > 0 ? traceFilters.apply() : null;

  // Doris query - no FINAL modifier needed
  const query = `
    SELECT
      id,
      trace_id,
      project_id,
      type,
      parent_observation_id,
      environment,
      start_time,
      end_time,
      name,
      metadata,
      level,
      status_message,
      version,
      input,
      output,
      provided_model_name,
      internal_model_id,
      model_parameters,
      provided_usage_details,
      usage_details,
      provided_cost_details,
      cost_details,
      total_cost,
      completion_start_time,
      prompt_id,
      prompt_name,
      prompt_version,
      created_at,
      updated_at,
      event_ts
    FROM observations o
    WHERE o.project_id = {projectId: String}
      ${appliedTraceFilter ? `AND EXISTS (SELECT 1 FROM traces t WHERE o.trace_id = t.id AND t.project_id = o.project_id AND ${appliedTraceFilter.query})` : ""}
      ${appliedObservationFilter.query ? `AND ${appliedObservationFilter.query}` : ""}
    ORDER BY start_time DESC
    ${props.limit !== undefined && props.page !== undefined ? `LIMIT {limit: Int32} OFFSET {offset: Int32}` : ""}
  `;

  return measureAndReturn({
    operationName: "generateObservationsForPublicApi",
    projectId: props.projectId,
    input: {
      params: {
        ...appliedObservationFilter.params,
        ...(appliedTraceFilter ? appliedTraceFilter.params : {}),
        projectId: props.projectId,
        ...(props.limit !== undefined ? { limit: props.limit } : {}),
        ...(props.page !== undefined
          ? { offset: (props.page - 1) * props.limit }
          : {}),
      },
      tags: {
        feature: "tracing",
        type: "observation",
        projectId: props.projectId,
        operation_name: "generateObservationsForPublicApi",
      },
    },
    fn: async (input) => {
      const result = await queryDoris<ObservationRecordReadType>({
        query,
        params: input.params,
        tags: input.tags,
      });
      return result.map((r) => convertObservation(r));
    },
  });
};

export const getObservationsCountForPublicApi = async (props: QueryType) => {
  const chFilter = generateFilter(props);
  const traceFilters = chFilter.filter((f) => f.table === "traces");
  const observationFilters = chFilter.filter((f) => f.table !== "traces");
  const appliedObservationFilter = observationFilters.apply();
  const appliedTraceFilter =
    traceFilters.length() > 0 ? traceFilters.apply() : null;

  const query = `
    SELECT count(*) as count
    FROM observation_source o
    WHERE o.project_id = {projectId: String}
    ${appliedTraceFilter ? `AND EXISTS (SELECT 1 FROM traces t WHERE o.trace_id = t.id AND t.project_id = o.project_id AND ${appliedTraceFilter.query})` : ""}
    ${appliedObservationFilter.query ? `AND ${appliedObservationFilter.query}` : ""}
  `;

  return measureAndReturn({
    operationName: "getObservationsCountForPublicApi",
    projectId: props.projectId,
    input: {
      params: {
        ...appliedObservationFilter.params,
        ...(appliedTraceFilter ? appliedTraceFilter.params : {}),
        projectId: props.projectId,
      },
      tags: {
        feature: "tracing",
        type: "observation",
        projectId: props.projectId,
        operation_name: "getObservationsCountForPublicApi",
      },
    },
    fn: async (input) => {
      const records = await queryDoris<{ count: string }>({
        query,
        params: input.params,
        tags: input.tags,
      });
      return records.map((record) => Number(record.count)).shift();
    },
  });
};

const filterParams = createPublicApiObservationsColumnMapping(
  "observations",
  "o",
  "parent_observation_id",
);

const generateFilter = (query: QueryType) => {
  const { advancedFilters, ...simpleFilterProps } = query;
  const chFilter = deriveFilters(
    simpleFilterProps,
    filterParams,
    advancedFilters,
    observationsTableUiColumnDefinitions.filter(
      (c) => c.tableName !== "scores",
    ),
    observationsTableCols,
  );

  // Remove score filters since observations don't support scores in response
  const filteredChFilter = chFilter.filter((f) => f.table !== "scores");

  // Add project filter
  filteredChFilter.push(
    new StringFilter({
      table: "observations",
      field: "project_id",
      operator: "=",
      value: query.projectId,
    }),
  );
  return filteredChFilter;
};
