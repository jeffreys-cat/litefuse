import { getBackendSrv } from "../shims/grafana-runtime";
import {
  buildTraceAggSQLFromParams,
  getOperationListSQL,
  getQueryTableTraceSQL,
  getServiceListSQL,
} from "./traces.sql";

// 获取table的Trace数据
export function getTableDataTraceService(payload: any) {
  const traceSQL = getQueryTableTraceSQL(payload);
  return getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getTableDataTrace",
          rawSql: traceSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
}

// 查询Traces
export function getTracesService(payload: any) {
  const getTracesSQL = buildTraceAggSQLFromParams(payload);
  return getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getTraces",
          rawSql: getTracesSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
}

// 查询Trace Services
export function getServiceListService(payload: any) {
  const serviceListSQL = getServiceListSQL(payload);
  return getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getServiceList",
          rawSql: serviceListSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
}

// 查询Trace Operations
export function getOperationListService(payload: any) {
  const operationListSQL = getOperationListSQL(payload);
  return getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getOperationList",
          rawSql: operationListSQL,
          format: "table",
        },
      ],
    },
    credentials: "include",
  });
}
