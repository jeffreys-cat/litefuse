import { getBackendSrv } from "../shims/grafana-runtime";
import {
  getQueryTableChartsSQL,
  getQueryTableResultCountSQL,
  getQueryTableResultSQL,
  getSurroundingSQL,
} from "./sql";

export function getTableDataService(payload: any) {
  const QueryTableResultSQL = getQueryTableResultSQL(payload);
  const response = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getTableData",
          rawSql: QueryTableResultSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
  return response;
}

export function getTableDataChartsService(payload: any) {
  const QueryTableChartsSQL = getQueryTableChartsSQL(payload);
  const response = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getTableDataCharts",
          rawSql: QueryTableChartsSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
  return response;
}

export function getTopDataService(payload: any) {
  const QueryTableResultSQL = getQueryTableResultSQL(payload);
  const response = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getTableTopData",
          rawSql: QueryTableResultSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
  return response;
}

export function getTableDataCountService(payload: any) {
  const QueryTableResultCountSQL = getQueryTableResultCountSQL(payload);
  const response = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getTableCountData",
          rawSql: QueryTableResultCountSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
  return response;
}

export function getSurroundingDataService(payload: any) {
  const surroundingSQL = getSurroundingSQL(payload);
  const response = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getSurroundingData",
          rawSql: surroundingSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
  return response;
}
