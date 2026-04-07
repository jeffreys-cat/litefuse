// @ts-nocheck
import { useCallback, useEffect } from "react";
import type { Dayjs } from "dayjs";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import { useRouter } from "next/router";
import {
  currentCatalogAtom,
  currentDatabaseAtom,
  currentDateAtom,
  currentIndexAtom,
  currentTableAtom,
  currentTimeFieldAtom,
  dataFilterAtom,
  discoverLoadingAtom,
  intervalAtom,
  pageAtom,
  pageSizeAtom,
  searchTypeAtom,
  searchValueAtom,
  tableDataAtom,
  tableDataChartsAtom,
  tableFieldsAtom,
  tableTotalCountAtom,
  topDataAtom,
} from "store/discover";
import {
  getTableDataChartsService,
  getTableDataCountService,
  getTableDataService,
  getTopDataService,
} from "services/discover";
import {
  encodeBase64,
  getChartsData,
  convertRowsToTableData,
  convertRowsToTableDataViaFieldsType,
  generateHighlightedResults,
  getIndexesStatement,
} from "utils/data";
import { generateTableDataUID } from "utils/utils";
import { FORMAT_DATE, getAutoInterval, IntervalEnum } from "../../constants";
import { useLuceneWhereClause } from "./useLuceneWhereClause";

type RefreshOptions = {
  skipPageReset?: boolean;
};

export function useDiscoverData() {
  const router = useRouter();
  const projectId = router.query.projectId as string;

  const [page, setPage] = useAtom(pageAtom);
  const pageSize = useAtomValue(pageSizeAtom);
  const setTableData = useSetAtom(tableDataAtom);
  const setTableDataCharts = useSetAtom(tableDataChartsAtom);
  const currentTimeField = useAtomValue(currentTimeFieldAtom);
  const interval = useAtomValue(intervalAtom);
  const currentIndexes = useAtomValue(currentIndexAtom);
  const tableFields = useAtomValue(tableFieldsAtom);
  const searchType = useAtomValue(searchTypeAtom);
  const dataFilter = useAtomValue(dataFilterAtom);
  const searchValue = useAtomValue(searchValueAtom);
  const setTopData = useSetAtom(topDataAtom);
  const currentTable = useAtomValue(currentTableAtom);
  const currentCatalog = useAtomValue(currentCatalogAtom);
  const currentDatabase = useAtomValue(currentDatabaseAtom);
  const currentDate = useAtomValue(currentDateAtom);
  const setTableTotalCount = useSetAtom(tableTotalCountAtom);
  const [loading, setLoading] = useAtom(discoverLoadingAtom);
  const buildLuceneWhereClause = useLuceneWhereClause();

  const getTableData = useCallback(async () => {
    if (!currentTable || !currentDatabase || !projectId) {
      return;
    }
    setLoading((prev) => ({ ...prev, getTableData: true }));
    const indexesStatement = getIndexesStatement(
      currentIndexes,
      tableFields,
      searchValue,
    );
    const payload: any = {
      catalog: currentCatalog,
      database: currentDatabase,
      table: currentTable,
      timeField: currentTimeField,
      startDate: currentDate[0]?.utc().format(FORMAT_DATE),
      endDate: (currentDate[1] as Dayjs).utc().format(FORMAT_DATE),
      cluster: "",
      sort: "DESC",
      search_type: searchType,
      indexes: "",
      page: page,
      page_size: pageSize,
    };

    if (searchType === "Search") {
      payload.indexes_statement = indexesStatement;
    }
    payload.data_filters = dataFilter.length > 0 ? dataFilter : [];

    if (searchType === "Lucene") {
      try {
        const luceneWhere = await buildLuceneWhereClause();
        if (luceneWhere) {
          payload.lucene_where = luceneWhere;
        }
      } catch (error) {
        setLoading((prev) => ({ ...prev, getTableData: false }));
        setTableData([]);
        console.error("Lucene query build failed", error);
        return;
      }
    }

    if (searchValue && searchType !== "Lucene") {
      payload.search_value =
        searchType === "Search" ? encodeBase64(searchValue) : searchValue;
    }

    try {
      const { rows } = await getTableDataService(projectId, payload);
      setLoading((prev) => ({ ...prev, getTableData: false }));

      if (!rows || rows.length === 0) {
        setTableData([]);
        return;
      }

      const rowsData = convertRowsToTableData(rows);
      const resData = generateHighlightedResults(
        {
          search_value: searchValue,
          indexes: currentIndexes || [],
        },
        rowsData,
      );

      const rowsDataWithUid = await generateTableDataUID(resData);
      setTableData(rowsDataWithUid);
    } catch (err) {
      setLoading((prev) => ({ ...prev, getTableData: false }));
      console.error("查询错误", err);
    }
  }, [
    buildLuceneWhereClause,
    currentCatalog,
    currentDate,
    currentDatabase,
    currentIndexes,
    currentTable,
    currentTimeField,
    dataFilter,
    page,
    pageSize,
    projectId,
    searchType,
    searchValue,
    setLoading,
    setTableData,
    tableFields,
  ]);

  const getTableDataCharts = useCallback(async () => {
    if (!currentTable || !currentDatabase || !projectId) {
      return;
    }
    setLoading((prev) => ({ ...prev, getTableDataCharts: true }));
    const autoInterval = getAutoInterval(currentDate as any);
    const timeInterval =
      interval === IntervalEnum.Auto ? autoInterval.interval_unit : interval;
    const timeIntervalValue =
      interval === IntervalEnum.Auto ? autoInterval.interval_value : 1;
    const indexesStatement = getIndexesStatement(
      currentIndexes,
      tableFields,
      searchValue,
    );
    const payload: any = {
      catalog: "internal",
      database: currentDatabase,
      table: currentTable,
      timeField: currentTimeField,
      startDate: currentDate[0]?.utc().format(FORMAT_DATE),
      endDate: (currentDate[1] as Dayjs).utc().format(FORMAT_DATE),
      cluster: "",
      data_filters: [],
      sort: "DESC",
      interval: timeInterval,
      interval_value: timeIntervalValue,
      search_type: searchType,
      indexes: indexesStatement,
    };

    if (dataFilter.length > 0) {
      payload.data_filters = dataFilter;
    }

    if (searchType === "Lucene") {
      try {
        const luceneWhere = await buildLuceneWhereClause();
        if (luceneWhere) {
          payload.lucene_where = luceneWhere;
        }
      } catch (error) {
        setLoading((prev) => ({ ...prev, getTableDataCharts: false }));
        setTableDataCharts([]);
        console.error("Lucene query build failed", error);
        return;
      }
    }

    if (searchValue && searchType !== "Lucene") {
      payload.search_value =
        searchType === "Search" ? encodeBase64(searchValue) : searchValue;
    }

    try {
      const { rows } = await getTableDataChartsService(projectId, payload);
      setLoading((prev) => ({ ...prev, getTableDataCharts: false }));

      if (!rows || rows.length === 0) {
        setTableDataCharts([]);
        return;
      }

      // rows are already row-major: [{ TT: "2026-03-25 14:00:00", "sum(cnt)": 5 }, ...]
      const chartsData = getChartsData(
        rows as any[],
        currentDate as [Dayjs, Dayjs],
      );
      setTableDataCharts(chartsData);
    } catch (err) {
      setLoading((prev) => ({ ...prev, getTableDataCharts: false }));
      console.error("查询错误", err);
    }
  }, [
    buildLuceneWhereClause,
    currentDate,
    currentDatabase,
    currentIndexes,
    currentTable,
    currentTimeField,
    dataFilter,
    interval,
    projectId,
    searchType,
    searchValue,
    setLoading,
    setTableDataCharts,
    tableFields,
  ]);

  const getTopData = useCallback(async () => {
    if (!currentTable || !currentDatabase || !projectId) {
      return;
    }
    const indexesStatement = getIndexesStatement(
      currentIndexes,
      tableFields,
      searchValue,
    );
    const payload: any = {
      catalog: currentCatalog,
      database: currentDatabase,
      table: currentTable,
      timeField: currentTimeField,
      startDate: currentDate[0]?.utc().format(FORMAT_DATE),
      endDate: (currentDate[1] as Dayjs).utc().format(FORMAT_DATE),
      cluster: "",
      sort: "DESC",
      search_type: searchType,
      indexes: "",
      page: page,
      page_size: 500,
    };

    if (searchType === "Search") {
      payload.indexes_statement = indexesStatement;
    }
    payload.data_filters = dataFilter.length > 0 ? dataFilter : [];

    if (searchValue && searchType !== "Lucene") {
      payload.search_value =
        searchType === "Search" ? encodeBase64(searchValue) : searchValue;
    }

    if (searchType === "Lucene") {
      try {
        const luceneWhere = await buildLuceneWhereClause();
        if (luceneWhere) {
          payload.lucene_where = luceneWhere;
        }
      } catch (error) {
        console.error("Lucene query build failed", error);
        setTopData([]);
        return;
      }
    }

    try {
      const { rows } = await getTopDataService(projectId, payload);

      if (!rows || rows.length === 0) {
        setTopData([]);
        return;
      }

      // Convert rows to the format expected by sidebar (with field type info)
      const rowsData = convertRowsToTableDataViaFieldsType(rows, tableFields);
      setTopData(rowsData);
    } catch (err) {
      console.error("查询错误", err);
      setTopData([]);
    }
  }, [
    buildLuceneWhereClause,
    currentCatalog,
    currentDate,
    currentDatabase,
    currentIndexes,
    currentTable,
    currentTimeField,
    dataFilter,
    page,
    projectId,
    searchType,
    searchValue,
    setTopData,
    tableFields,
  ]);

  const getTableDataCount = useCallback(async () => {
    if (!currentTable || !currentDatabase || !projectId) {
      return;
    }
    const autoInterval = getAutoInterval(currentDate as any);
    const timeInterval =
      interval === IntervalEnum.Auto ? autoInterval.interval_unit : interval;
    const timeIntervalValue =
      interval === IntervalEnum.Auto ? autoInterval.interval_value : 1;
    const indexesStatement = getIndexesStatement(
      currentIndexes,
      tableFields,
      searchValue,
    );
    const payload: any = {
      catalog: "internal",
      database: currentDatabase,
      table: currentTable,
      timeField: currentTimeField,
      startDate: currentDate[0]?.utc().format(FORMAT_DATE),
      endDate: (currentDate[1] as Dayjs).utc().format(FORMAT_DATE),
      cluster: "",
      sort: "DESC",
      interval: timeInterval,
      data_filters: [],
      interval_value: timeIntervalValue,
      search_type: searchType,
      indexes: indexesStatement,
    };

    if (dataFilter.length > 0) {
      payload.data_filters = dataFilter;
    }

    if (searchType === "Lucene") {
      try {
        const luceneWhere = await buildLuceneWhereClause();
        if (luceneWhere) {
          payload.lucene_where = luceneWhere;
        }
      } catch (error) {
        console.error("Lucene query build failed", error);
        setTableTotalCount(0);
        return;
      }
    }

    if (searchValue && searchType !== "Lucene") {
      payload.search_value =
        searchType === "Search" ? encodeBase64(searchValue) : searchValue;
    }

    try {
      const { rows } = await getTableDataCountService(projectId, payload);

      if (!rows || rows.length === 0) {
        setTableTotalCount(0);
        return;
      }

      const firstRow = rows[0] as Record<string, unknown>;
      const totalCount = Number(
        firstRow.total_count ?? firstRow["SUM(table_per_time.cnt)"] ?? 0,
      );
      setTableTotalCount(totalCount || 0);
    } catch (err) {
      console.error("查询错误", err);
      setTableTotalCount(0);
    }
  }, [
    buildLuceneWhereClause,
    currentDate,
    currentDatabase,
    currentIndexes,
    currentTable,
    currentTimeField,
    dataFilter,
    interval,
    projectId,
    searchType,
    searchValue,
    setTableTotalCount,
    tableFields,
  ]);

  const clearData = useCallback(() => {
    setTableDataCharts([]);
    setTableTotalCount(0);
    setTableData([]);
    setTopData([]);
  }, [setTableData, setTableDataCharts, setTableTotalCount, setTopData]);

  const refreshData = useCallback(
    ({ skipPageReset = false }: RefreshOptions = {}) => {
      if (!skipPageReset) {
        setPage(1);
      }
      if (!currentTimeField) {
        clearData();
        return;
      }
      void getTableDataCharts();
      void getTableDataCount();
      void getTableData();
      void getTopData();
    },
    [
      clearData,
      currentTimeField,
      getTableData,
      getTableDataCharts,
      getTableDataCount,
      getTopData,
      setPage,
    ],
  );

  const handleQuerying = useCallback(() => {
    if (!currentTimeField) {
      clearData();
      return;
    }
    refreshData();
  }, [clearData, currentTimeField, refreshData]);

  useEffect(() => {
    if (!currentTimeField) {
      return;
    }
    void getTableData();
    void getTopData();
    void getTableDataCharts();
    void getTableDataCount();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentTimeField, page]);

  useEffect(() => {
    refreshData({ skipPageReset: false });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentDate, currentTimeField, dataFilter, interval, currentTable]);

  return {
    loading,
    onQuerying: handleQuerying,
  };
}
