// @ts-nocheck
"use client";
import type { ColumnDef, Row } from "@tanstack/react-table";
import React, { useEffect, useMemo, useState } from "react";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import { useRouter } from "next/router";
import { IconButton } from "components/ui/icon-button";
import { Pagination } from "components/ui/pagination";
import { Tab, TabContent, TabsBar } from "components/ui/tabs";
import { useDiscoverTheme } from "components/ui/theme";
import JsonView from "@uiw/react-json-view";
import {
  tableTotalCountAtom,
  tableDataAtom,
  selectedFieldsAtom,
  selectedRowAtom,
  surroundingTableDataAtom,
  pageSizeAtom,
  pageAtom,
  afterCountAtom,
  beforeCountAtom,
  surroundingDataFilterAtom,
  surroundingSelectedFieldsAtom,
  currentTimeFieldAtom,
} from "store/discover";
import { get } from "lodash-es";
import { Button as AntButton } from "antd";
import SDCollapsibleTable from "components/selectdb-ui/sd-collapsible-table";
import { ColumnStyleWrapper, HoverStyle } from "./discover-content.style";
import { css } from "@emotion/css";
import { ContentTableActions } from "./content-table-actions";
import { SELECTDB_THEME, SELECTDB_THEME_LIGHT } from "./json-viewer.theme";
import { ContentItem } from "./content-item";
import SurroundingLogs from "components/surrounding-logs";
import {
  Drawer,
  DrawerContent,
  DrawerHeader,
  DrawerTitle,
} from "@/src/components/ui/drawer";
import { TablePeekView } from "@/src/components/table/peek";
import { usePeekNavigation } from "@/src/components/table/peek/hooks/usePeekNavigation";
import { PeekViewTraceDetail } from "@/src/components/table/peek/peek-trace-detail";

type DiscoverTableRow = {
  _original: Record<string, unknown>;
  _source: string;
  _uid?: string;
  time: string;
};

function getTraceIdFromRow(row: DiscoverTableRow) {
  const traceId = row._original?.trace_id;
  if (typeof traceId === "string" && traceId.length > 0) {
    return traceId;
  }

  const id = row._original?.id;
  if (typeof id === "string" && id.length > 0) {
    return id;
  }

  return undefined;
}

export default function DiscoverContent({
  fetchNextPage: _fetchNextPage,
}: {
  fetchNextPage: (page: number) => void;
}) {
  const router = useRouter();
  const theme = useDiscoverTheme();
  const [fields, setFields] = useState<any[]>([]);
  const tableTotalCount = useAtomValue(tableTotalCountAtom);
  const [tableData, _setTableData] = useAtom(tableDataAtom);
  const [selectedFields, setSelectedFields] = useAtom(selectedFieldsAtom);
  const hasSelectedFields = selectedFields.length > 0;
  const currentTimeField = useAtomValue(currentTimeFieldAtom);
  // const [surroundingOpen, setSurroundingOpen] = useState(false);
  const [_selectedRow, setSelectedRow] = useAtom(selectedRowAtom);
  const setSurroundingTableData = useSetAtom(surroundingTableDataAtom);
  const setSurroundingDataFilter = useSetAtom(surroundingDataFilterAtom);
  const setSelectedSurroundingFields = useSetAtom(
    surroundingSelectedFieldsAtom,
  );
  const setBeforeCount = useSetAtom(beforeCountAtom);
  const setAfterCount = useSetAtom(afterCountAtom);
  const [pageSize, _setPageSize] = useAtom(pageSizeAtom);
  const [page, setPage] = useAtom(pageAtom);
  const [surroundingLogsOpen, setSurroundingLogsOpen] = useState(false);
  const [_fieldKeyBg, setFieldKeyBg] = useState<string>("#3f3f4f");

  useEffect(() => {
    if (theme.isDark) {
      setFieldKeyBg("#3f3f4f");
    } else {
      setFieldKeyBg("rgb(191, 217, 253)");
    }
  }, [theme.isDark]);

  const [state, updateState] = useState([
    {
      label: "Table",
      value: "Table",
      active: true,
    },
    {
      label: "JSON",
      value: "JSON",
      active: false,
    },
  ]);

  useEffect(() => {
    const data = tableData.map((item) => {
      return {
        _original: item._original,
        time: item._original?.[currentTimeField] || "",
        _source: item._source,
        _uid: item?._uid,
      };
    });
    setFields(data);
  }, [tableData, currentTimeField]);

  const projectId = router.query.projectId as string | undefined;

  const peekNavigationProps = usePeekNavigation({
    queryParams: ["observation", "display", "timestamp", "peekProjectId"],
    extractParamsValuesFromRow: (row: DiscoverTableRow) => {
      const params: Record<string, string> = {};

      if (!row.time) {
        const rowProjectId = row._original?.project_id;
        if (typeof rowProjectId === "string" && rowProjectId.length > 0) {
          params.peekProjectId = rowProjectId;
        }
        return params;
      }

      const parsedTimestamp = new Date(row.time);
      if (!Number.isNaN(parsedTimestamp.getTime())) {
        params.timestamp = parsedTimestamp.toISOString();
      }

      const rowProjectId = row._original?.project_id;
      if (typeof rowProjectId === "string" && rowProjectId.length > 0) {
        params.peekProjectId = rowProjectId;
      }

      return params;
    },
  });

  const handleRemove = React.useCallback(
    (field: any) => {
      const index = selectedFields.findIndex(
        (item: any) => item.Field === field.Field,
      );
      selectedFields.splice(index, 1);
      setSelectedFields([...selectedFields]);
    },
    [selectedFields, setSelectedFields],
  );

  const openTracePeek = React.useCallback(
    (traceId: unknown, row?: DiscoverTableRow) => {
      if (typeof traceId !== "string" || !traceId) {
        return;
      }

      peekNavigationProps.openPeek(traceId, row);
    },
    [peekNavigationProps],
  );

  const renderSubComponent = ({ row }: { row: Row<any> }) => {
    const subTableData = Object.keys(row.original._original).map((key) => {
      return {
        field: key,
        value: row.original._original[key],
      };
    });
    return (
      <div
        className={css`
          position: relative;
        `}
      >
        <TabsBar
          className={css`
            ${theme.isDark
              ? "background-color: hsl(var(--n9) / 0.4);"
              : "background-color: hsl(var(--b1) / 0.6);"}
          `}
        >
          {state.map((tab, index) => {
            return (
              <Tab
                key={index}
                label={tab.label}
                active={tab.active}
                onChangeTab={() =>
                  updateState(
                    state.map((tab, idx) => ({
                      ...tab,
                      active: idx === index,
                    })),
                  )
                }
                counter={subTableData.length}
              />
            );
          })}
        </TabsBar>

        <TabContent>
          {state[0].active && (
            <table
              // className="bg-b1/20 pl-4 backdrop-blur-md dark:bg-n9/60"
              className={css`
                padding-left: 16px;
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                width: 100%;
                ${theme.isDark
                  ? "background-color: hsl(var(--n9) / 0.6);"
                  : "background-color: hsl(var(--b1) / 0.2)"}
              `}
            >
              <tbody>
                {subTableData.map((item: any) => {
                  let fieldValue = item.value;
                  const fieldName = item.field;
                  if (typeof fieldValue === "object") {
                    fieldValue = JSON.stringify(fieldValue);
                  }
                  const tableRowStyle = css`
                    &:hover {
                      .filter-table-content {
                        visibility: visible;
                      }
                    }
                  `;
                  return (
                    <tr className={`${tableRowStyle}`} key={fieldName}>
                      <td
                        className={css`
                          height: 32px;
                          width: 70px;
                        `}
                      >
                        <div
                          className={`filter-table-content ${css`
                            visibility: hidden;
                          `}`}
                        >
                          <ContentTableActions
                            fieldName={fieldName}
                            fieldValue={fieldValue}
                          />
                        </div>
                      </td>
                      <td
                        className={css`
                          height: 32px;
                          font-size: 12px;
                        `}
                      >
                        {fieldName || "-"}
                      </td>
                      <td
                        className={css`
                          height: 32px;
                          font-size: 12px;
                          white-space: normal;
                        `}
                      >
                        <div
                          className={css`
                            width: 100%;
                            word-break: break-all;
                          `}
                        >
                          {fieldValue || "-"}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
          {state[1].active && (
            <div>
              <JsonView
                value={row.original._original}
                className={`-mt-2 pl-11 leading-6! ${css`
                  .w-rjv-wrap {
                    border-left: none !important;
                  }
                `}`}
                shortenTextAfterLength={0}
                indentWidth={36}
                displayDataTypes={false}
                enableClipboard={false}
                style={theme.isDark ? SELECTDB_THEME : SELECTDB_THEME_LIGHT}
              />
            </div>
          )}
        </TabContent>
        <a
          onClick={() => {
            console.log("row", row);
            setSurroundingLogsOpen(true);
            setSelectedRow(row.original);
          }}
          className={css`
            position: absolute;
            right: 1rem;
            top: 0;
            cursor: pointer;
            padding-top: 0.5rem;
            &:hover {
              color: rgb(43, 102, 253);
            }
          `}
        >
          Surrounding Logs
        </a>
      </div>
    );
  };

  const columns = useMemo<Array<ColumnDef<any>>>(() => {
    let dynamicColumns: Array<ColumnDef<any>> = [
      {
        accessorKey: "collapse",
        header: ``,
        cell: ({ row, getValue: _getValue }) => {
          return (
            row.getCanExpand() && (
              <div className="flex items-center">
                {row.getIsExpanded() ? (
                  <IconButton
                    onClick={row.getToggleExpandedHandler()}
                    name="arrow-down"
                    tooltip="收起"
                  />
                ) : (
                  <IconButton
                    onClick={row.getToggleExpandedHandler()}
                    name="arrow-right"
                    tooltip="展开"
                  />
                )}
                <div className="ml-1">{_getValue<string>()}</div>
              </div>
            )
          );
        },
      },
      {
        header: "Time",
        accessorKey: "time",
        cell: ({ row: _row, getValue }) => {
          const fieldValue = getValue<string>();
          const fieldName = currentTimeField;
          const fieldType = "DATE";
          const timeField = fieldValue;
          return (
            <div
              className={`${css`
                width: 240px;
              `} ${HoverStyle}`}
            >
              <div
                className={css`
                  display: flex;
                  align-items: center;
                `}
              >
                {timeField}
                <div
                  className={`filter-content ${css`
                    visibility: hidden;
                  `}`}
                >
                  <ContentItem
                    fieldName={fieldName}
                    fieldValue={fieldValue}
                    fieldType={fieldType}
                  />
                </div>
              </div>
            </div>
          );
        },
      },
    ];
    if (!hasSelectedFields) {
      dynamicColumns.push({
        accessorKey: "_source",
        header: "_source",
        cell: ({ row, getValue }) => {
          const traceId = getTraceIdFromRow(row.original);
          const isTracePeekEnabled = Boolean(traceId);
          const hoverBackgroundColor = theme.isDark
            ? "hsl(var(--muted) / 0.28)"
            : "hsl(var(--muted) / 0.6)";
          const hoverBorderColor = theme.isDark
            ? "hsl(var(--border) / 0.55)"
            : "hsl(var(--border) / 0.85)";

          function createMarkup() {
            return { __html: getValue<string>() };
          }
          return (
            <div
              onClick={() => {
                if (!isTracePeekEnabled) {
                  return;
                }
                openTracePeek(traceId, row.original);
              }}
              onKeyDown={(event) => {
                if (!isTracePeekEnabled) {
                  return;
                }
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  openTracePeek(traceId, row.original);
                }
              }}
              role={isTracePeekEnabled ? "button" : undefined}
              tabIndex={isTracePeekEnabled ? 0 : undefined}
              className={css`
                padding-top: 0.5rem;
                padding-bottom: 0.5rem;
                font-size: 0.875rem;
                line-height: 1.25rem;
                border-radius: 0.5rem;
                transition:
                  background-color 160ms ease,
                  box-shadow 160ms ease;
                ${isTracePeekEnabled
                  ? `cursor: pointer; &:hover { background-color: ${hoverBackgroundColor}; box-shadow: inset 0 0 0 1px ${hoverBorderColor}; } &:focus-visible { outline: none; background-color: ${hoverBackgroundColor}; box-shadow: inset 0 0 0 1px ${hoverBorderColor}, 0 0 0 2px hsl(var(--ring) / 0.35); }`
                  : ""}
              `}
            >
              <ColumnStyleWrapper
                className={css`
                  & .field-key {
                    background-color: ${theme.isDark
                      ? "#3f3f4f"
                      : "rgb(191, 217, 253)"};
                  }
                `}
              >
                <div
                  dangerouslySetInnerHTML={createMarkup()}
                  className={css`
                    max-height: 12rem;
                    overflow: auto;
                    word-break: break-all;
                    white-space: pre-wrap;
                  `}
                />
              </ColumnStyleWrapper>
            </div>
          );
        },
      });
    } else {
      dynamicColumns = [
        ...dynamicColumns,
        ...selectedFields.map((field: any) => {
          return {
            accessorKey: field.Field,
            header: () => (
              <div
                className={css`
                  display: flex;
                  align-items: center;
                `}
              >
                <div>{field.Field}</div>
                <IconButton
                  name="times"
                  tooltip="Remove"
                  style={{
                    marginLeft: "8px",
                    cursor: "pointer",
                    marginTop: "2px",
                  }}
                  onClick={(e) => {
                    handleRemove(field);
                    e.preventDefault();
                    e.stopPropagation();
                  }}
                />
              </div>
            ),
            cell: ({ row, getValue: _getValue }: any) => {
              // let fieldValue = row.original._original[field.Field];
              let fieldValue = get(row.original._original, field.Field);
              const fieldName = field.Field;
              const fieldType = field.Type;
              const traceId = getTraceIdFromRow(row.original);
              const isTraceIdField =
                field.Field === "trace_id" || field.Field === "id";
              if (typeof fieldValue === "object") {
                fieldValue = JSON.stringify(fieldValue);
              }
              return (
                <div
                  className={`${HoverStyle} ${css`
                    display: flex;
                    align-items: center;
                    min-height: 48px;
                  `}`}
                >
                  <div
                    className={css`
                      max-height: 192px;
                      overflow: auto;
                    `}
                  >
                    <div
                      className={css`
                        display: flex;
                        align-items: center;
                        padding: 16px;
                        word-break: break-all;
                      `}
                    >
                      {isTraceIdField &&
                      typeof fieldValue === "string" &&
                      fieldValue === traceId ? (
                        <AntButton
                          onClick={() =>
                            openTracePeek(fieldValue, row.original)
                          }
                          type="link"
                        >
                          {fieldValue}
                        </AntButton>
                      ) : (
                        <span
                          className={css`
                            font-size: 12px;
                          `}
                        >
                          {fieldValue}
                        </span>
                      )}
                    </div>
                  </div>
                  <div
                    className={`filter-content ${css`
                      visibility: hidden;
                    `}`}
                  >
                    <ContentItem
                      fieldName={fieldName}
                      fieldValue={fieldValue}
                      fieldType={fieldType}
                    />
                  </div>
                </div>
              );
            },
          };
        }),
      ];
    }
    return dynamicColumns;
  }, [
    currentTimeField,
    handleRemove,
    hasSelectedFields,
    openTracePeek,
    selectedFields,
    theme.isDark,
  ]);

  return (
    <div
      className={css`
        overflow-x: scroll;
      `}
    >
      {/* {
                loading.getTableDataCharts && <LoadingBar width={100} />
            } */}
      <SDCollapsibleTable
        className={css`
          width: 100%;
        `}
        data={fields}
        columns={columns}
        getRowCanExpand={() => true}
        renderSubComponent={renderSubComponent}
      />
      <div
        className={css`
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 0.5rem 1rem;
          padding-bottom: 20px;
        `}
      >
        <div>Total {tableTotalCount} rows</div>
        <Pagination
          currentPage={page}
          numberOfPages={Math.ceil(tableTotalCount / pageSize) || 1}
          onNavigate={(toPage) => {
            setPage(toPage);
          }}
        />
      </div>

      {surroundingLogsOpen && (
        <Drawer
          open={surroundingLogsOpen}
          onOpenChange={(open) => {
            if (open) {
              return;
            }
            setSurroundingTableData([]);
            setSurroundingDataFilter([]);
            setBeforeCount(0);
            setAfterCount(0);
            setSelectedSurroundingFields([]);
            setSurroundingLogsOpen(false);
          }}
        >
          <DrawerContent size="lg">
            <DrawerHeader className="border-border border-b">
              <DrawerTitle>Surrounding Logs</DrawerTitle>
            </DrawerHeader>
            <div className="min-h-0 flex-1 overflow-auto p-4">
              <SurroundingLogs />
            </div>
          </DrawerContent>
        </Drawer>
      )}

      {projectId ? (
        <TablePeekView
          peekView={{
            itemType: "TRACE",
            children: <PeekViewTraceDetail projectId={projectId} />,
            ...peekNavigationProps,
          }}
        />
      ) : null}
    </div>
  );
}
