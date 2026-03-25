// @ts-nocheck
import { toDataFrame } from "@grafana/data";
import { getBackendSrv, getDiscoverProjectId } from "../shims/grafana-runtime";
import { lastValueFrom } from "rxjs";
import { Observable } from "rxjs";

type GetColumnParams = {
  connectionId?: string;
  database: string;
  table: string;
  column: string;
  datasourceType?: string;
};

type ColumnMetadata = {
  name: string;
  dataType?: string;
  columnType?: string;
};

type GetIndexesParams = {
  connectionId?: string;
  database: string;
  table: string;
  datasourceType?: string;
};

const escapeSqlLiteral = (value: string) => value.replace(/'/g, "''");

const normalizeColumnType = ({
  dataType,
  columnType,
}: {
  dataType?: string;
  columnType?: string;
}): string => {
  const source = (columnType || dataType || "").trim();
  if (!source) {
    return "";
  }

  const lower = source.toLowerCase();

  if (lower.startsWith("nullable(") && lower.endsWith(")")) {
    const inner = source.slice(9, -1);
    const normalizedInner = normalizeColumnType({
      dataType: inner,
      columnType: undefined,
    });
    return normalizedInner ? `Nullable(${normalizedInner})` : source;
  }

  if (lower.startsWith("map")) {
    return source.replace(/^map/i, "Map");
  }

  if (lower.startsWith("array")) {
    return source.replace(/^array/i, "Array");
  }

  if (lower.startsWith("json") || lower.startsWith("variant")) {
    return "JSON";
  }

  if (
    lower === "bool" ||
    lower === "boolean" ||
    lower.startsWith("tinyint(1)")
  ) {
    return "Bool";
  }

  if (lower.startsWith("tinyint")) {
    return "Int8";
  }

  if (lower.startsWith("smallint")) {
    return "Int16";
  }

  if (lower.startsWith("mediumint")) {
    return "Int32";
  }

  if (
    lower.startsWith("bigint") ||
    lower.startsWith("int") ||
    lower.startsWith("integer")
  ) {
    return "Int64";
  }

  if (
    lower.startsWith("float") ||
    lower.startsWith("double") ||
    lower.startsWith("real")
  ) {
    return "Float64";
  }

  if (lower.startsWith("decimal") || lower.startsWith("numeric")) {
    return "Float64";
  }

  if (lower.startsWith("date")) {
    return source.replace(/^date/i, "Date");
  }

  if (lower.startsWith("timestamp") || lower.startsWith("datetime")) {
    return "DateTime";
  }

  if (lower.startsWith("enum")) {
    return source.replace(/^enum/i, "Enum");
  }

  if (lower.startsWith("uuid")) {
    return "UUID";
  }

  if (lower.startsWith("ipv4")) {
    return "IPv4";
  }

  if (lower.startsWith("ipv6")) {
    return "IPv6";
  }

  if (lower.startsWith("tuple")) {
    return source.replace(/^tuple/i, "Tuple");
  }

  if (lower.startsWith("struct")) {
    return source.replace(/^struct/i, "Tuple");
  }

  if (
    lower.startsWith("char") ||
    lower.startsWith("varchar") ||
    lower.startsWith("text") ||
    lower.startsWith("string")
  ) {
    return "String";
  }

  return source;
};

export async function getColumn({
  database,
  table,
  column,
}: GetColumnParams): Promise<
  (ColumnMetadata & { normalizedType: string }) | null
> {
  if (!database || !table || !column) {
    return null;
  }

  const query = `
SELECT
  COLUMN_NAME AS Field,
  DATA_TYPE AS DataType,
  COLUMN_TYPE AS ColumnType
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = '${escapeSqlLiteral(database)}'
  AND TABLE_NAME = '${escapeSqlLiteral(table)}'
  AND COLUMN_NAME = '${escapeSqlLiteral(column)}'
LIMIT 1;
`;

  const response$ = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getColumn",
          rawSql: query,
          format: "table",
        },
      ],
    },
  });

  try {
    const { data, ok } = await lastValueFrom(response$);
    if (!ok) {
      return null;
    }

    const resultData = data as { results?: Record<string, any> };
    const frame = resultData?.results?.getColumn?.frames?.[0];
    if (!frame) {
      return null;
    }

    const dataFrame = toDataFrame(frame);
    const nameField =
      dataFrame.fields.find((field) => field.name === "Field") ??
      dataFrame.fields[0];
    const dataTypeField = dataFrame.fields.find(
      (field) => field.name === "DataType",
    );
    const columnTypeField = dataFrame.fields.find(
      (field) => field.name === "ColumnType",
    );

    const name = nameField?.values?.get?.(0);
    if (!name) {
      return null;
    }

    const dataTypeValue = dataTypeField?.values?.get?.(0);
    const columnTypeValue = columnTypeField?.values?.get?.(0);

    const columnInfo: ColumnMetadata = {
      name: String(name),
      dataType: dataTypeValue != null ? String(dataTypeValue) : undefined,
      columnType: columnTypeValue != null ? String(columnTypeValue) : undefined,
    };

    const normalizedType = normalizeColumnType({
      dataType: columnInfo.dataType,
      columnType: columnInfo.columnType,
    });

    return {
      ...columnInfo,
      normalizedType,
    };
  } catch (error) {
    console.error("Failed to fetch column metadata", error);
    return null;
  }
}

export async function getInvertedIndexColumns({
  database,
  table,
}: GetIndexesParams): Promise<string[]> {
  if (!database || !table) {
    return [];
  }

  const query = `SHOW INDEXES FROM \`${database}\`.\`${table}\``;

  const response$ = getBackendSrv().fetch({
    url: "/api/ds/query",
    method: "POST",
    data: {
      queries: [
        {
          refId: "getInvertedIndexes",
          rawSql: query,
          format: "table",
        },
      ],
    },
  });

  try {
    const { data, ok } = await lastValueFrom(response$);
    if (!ok) {
      return [];
    }

    const resultData = data as { results?: Record<string, any> };
    const frame =
      resultData?.results?.getInvertedIndexes?.frames?.[0] ??
      resultData?.results?.getIndexes?.frames?.[0];

    if (!frame) {
      return [];
    }

    const dataFrame = toDataFrame(frame);
    const columnNameField =
      dataFrame.fields.find((field) => field.name === "Column_name") ??
      dataFrame.fields.find((field) => field.name === "COLUMN_NAME");
    const indexTypeField =
      dataFrame.fields.find((field) => field.name === "Index_type") ??
      dataFrame.fields.find((field) => field.name === "INDEX_TYPE");

    if (!columnNameField || !indexTypeField) {
      return [];
    }

    const columnNames = Array.from(columnNameField.values ?? []);
    const indexTypes = Array.from(indexTypeField.values ?? []);
    const indexedColumns = new Set<string>();

    for (let i = 0; i < columnNames.length; i += 1) {
      const columnName = columnNames[i];
      const indexType = indexTypes[i];
      if (typeof columnName !== "string" || columnName.length === 0) {
        continue;
      }
      if (typeof indexType !== "string") {
        continue;
      }

      if (indexType.toUpperCase().includes("INVERT")) {
        indexedColumns.add(columnName);
      }
    }

    return Array.from(indexedColumns);
  } catch (error) {
    console.error("Failed to fetch inverted index metadata", error);
    return [];
  }
}

export function getDatabases() {
  return metadataFetch({ action: "databases" });
}

export function getTablesService({ database }: { database: string }) {
  return metadataFetch({ action: "tables", database });
}

export function getFieldsService({
  database,
  table,
}: {
  database: string;
  table: string;
}) {
  return metadataFetch({ action: "fields", database, table });
}

export function getColumnFromFieldService() {
  // return getBackendSrv().fetch({
  //     url: '/api/ds/query',
  //     method: 'POST',
  //     data: {
  //         queries: [
  //             {
  //                 refId: 'getColumnFromFieldService',
  //                 rawSql: `SHOW COLUMNS FROM \`${database}\`.\`${table}\``,
  //                 format: 'table',
  //             },
  //         ],
  //     },
  // });
}

export function getIndexesService({
  database,
  table,
}: {
  database: string;
  table: string;
}) {
  return metadataFetch({ action: "indexes", database, table });
}

function metadataFetch(body: Record<string, unknown>) {
  return new Observable<{
    data: { rows: unknown[] };
    ok: boolean;
    status: number;
  }>((subscriber) => {
    const projectId = getDiscoverProjectId();

    fetch(`/api/project/${projectId}/discover-metadata`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(body),
    })
      .then(async (res) => {
        const json = await res.json();
        subscriber.next({ data: json, ok: res.ok, status: res.status });
        subscriber.complete();
      })
      .catch((err) => {
        subscriber.error(err);
      });
  });
}
