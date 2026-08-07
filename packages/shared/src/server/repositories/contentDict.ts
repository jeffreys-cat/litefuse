import { createHash } from "crypto";

import {
  CONTENT_DICT_HASH_BATCH_SIZE,
  type ContentDictInputSearchMatch,
} from "../queries/doris-sql/search";
import { tableFor } from "../doris/tableRouting";
import { parseDorisUTCDateTimeFormat, queryDoris } from "./doris";

export type ContentRecordInsertType = {
  start_time: string;
  content_hash: string;
  content: string;
};

type ContentDictInputRow = {
  input?: unknown;
  start_time?: string | Date | null;
  project_id?: string | null;
};

type ContentDictReadRecord = ContentRecordInsertType & {
  project_id: string;
};

const contentHashPattern = /^[a-f0-9]{64}$/i;

const sha256 = (content: string): string =>
  createHash("sha256").update(content).digest("hex");

const parseJson = (value: string): unknown => {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

const serializeContent = (value: unknown): string =>
  typeof value === "string" ? value : JSON.stringify(value);

const inputValues = (input: unknown): unknown[] => {
  if (typeof input === "string") {
    const parsed = parseJson(input);
    return Array.isArray(parsed) ? parsed : [parsed];
  }

  return Array.isArray(input) ? input : [input];
};

export const contentDictStartTimeForEventStartTime = (
  startTime: string | number | Date,
): string => {
  const date =
    typeof startTime === "string"
      ? parseDorisUTCDateTimeFormat(startTime)
      : new Date(startTime);
  return date.toISOString().slice(0, 10);
};

/**
 * Replaces an event input with space-separated references into content_dict.
 * Arrays are split at the top level; every other input becomes a one-element
 * text hash list.
 */
export const deduplicateEventInput = (
  input: unknown,
  startTime: string | number | Date,
): {
  input: string | null | undefined;
  contentEntries: ContentRecordInsertType[];
} => {
  if (input === null || input === undefined) {
    return { input, contentEntries: [] };
  }

  const contentStartTime = contentDictStartTimeForEventStartTime(startTime);
  const contentEntries: ContentRecordInsertType[] = [];
  const hashes = inputValues(input).map((value) => {
    const content = serializeContent(value);
    const content_hash = sha256(content);
    contentEntries.push({
      start_time: contentStartTime,
      content_hash,
      content,
    });
    return content_hash;
  });

  return { input: hashes.join(" "), contentEntries };
};

const parseHashList = (input: unknown): string[] | null => {
  if (typeof input !== "string") return null;
  const trimmedInput = input.trim();
  return trimmedInput === "" ? [] : trimmedInput.split(/\s+/);
};

/**
 * Resolves content_dict references after an events_full query. The first query
 * supplies each row's event day and space-separated hash list; this function
 * performs one grouped dictionary lookup and restores input in application code.
 */
export const resolveContentDictInputs = async <T extends ContentDictInputRow>(
  rows: T[],
  projectId?: string,
): Promise<T[]> => {
  const hashesByProjectAndStartTime = new Map<
    string,
    Map<string, Set<string>>
  >();

  for (const row of rows) {
    const rowProjectId = projectId ?? row.project_id ?? undefined;
    if (!rowProjectId) continue;
    if (!row.start_time) continue;
    const hashes = parseHashList(row.input);
    if (
      !hashes ||
      hashes.length === 0 ||
      !hashes.every((hash) => contentHashPattern.test(hash))
    ) {
      continue;
    }

    const hashesByStartTime =
      hashesByProjectAndStartTime.get(rowProjectId) ?? new Map();
    hashesByProjectAndStartTime.set(rowProjectId, hashesByStartTime);
    const startTime = contentDictStartTimeForEventStartTime(row.start_time);
    const startTimeHashes =
      hashesByStartTime.get(startTime) ?? new Set<string>();
    hashes.forEach((hash) => startTimeHashes.add(hash));
    hashesByStartTime.set(startTime, startTimeHashes);
  }

  if (hashesByProjectAndStartTime.size === 0) return rows;

  const contentRows: ContentDictReadRecord[] = [];
  for (const [rowProjectId, hashesByStartTime] of hashesByProjectAndStartTime) {
    for (const [startTime, hashes] of hashesByStartTime) {
      const hashList = Array.from(hashes);
      for (
        let offset = 0;
        offset < hashList.length;
        offset += CONTENT_DICT_HASH_BATCH_SIZE
      ) {
        const contentHashes = hashList.slice(
          offset,
          offset + CONTENT_DICT_HASH_BATCH_SIZE,
        );
        const rowsForProject = await queryDoris<ContentRecordInsertType>({
          query: `
            SELECT start_time, content_hash, content
            FROM ${tableFor(rowProjectId, "content_dict")}
            WHERE start_time = {contentStartTime: Date}
              AND content_hash IN ({contentHashes: Array(String)})
          `,
          params: {
            contentStartTime: startTime,
            contentHashes,
          },
          tags: {
            feature: "tracing",
            type: "content_dict",
            kind: "resolve",
            projectId: rowProjectId,
          },
        });
        contentRows.push(
          ...rowsForProject.map((contentRow) => ({
            ...contentRow,
            project_id: rowProjectId,
          })),
        );
      }
    }
  }

  return restoreContentDictInputs(rows, contentRows, projectId);
};

export const restoreContentDictInputs = <T extends ContentDictInputRow>(
  rows: T[],
  contentRows: Array<ContentRecordInsertType & { project_id?: string }>,
  projectId?: string,
): T[] => {
  const contentByReference = new Map(
    contentRows.map((row) => [
      `${row.project_id ?? projectId ?? ""}\u0000${contentDictStartTimeForEventStartTime(row.start_time)}\u0000${row.content_hash}`,
      row.content,
    ]),
  );

  return rows.map((row) => {
    const rowProjectId = projectId ?? row.project_id ?? undefined;
    if (
      !rowProjectId &&
      contentRows.some((contentRow) => contentRow.project_id)
    ) {
      return row;
    }
    if (!row.start_time) return row;
    const hashes = parseHashList(row.input);
    if (!hashes || !hashes.every((hash) => contentHashPattern.test(hash))) {
      return row;
    }
    if (hashes.length === 0) return { ...row, input: "[]" };

    const startTime = contentDictStartTimeForEventStartTime(row.start_time);
    const contents = hashes.map((hash) =>
      contentByReference.get(
        `${rowProjectId ?? ""}\u0000${startTime}\u0000${hash}`,
      ),
    );
    if (contents.some((content) => content === undefined)) return row;

    return {
      ...row,
      input: JSON.stringify(contents.map((content) => parseJson(content!))),
    };
  });
};

export async function* resolveContentDictInputStream<
  T extends ContentDictInputRow,
>(
  rows: AsyncIterable<T>,
  projectId?: string,
  batchSize = 500,
): AsyncGenerator<T> {
  let batch: T[] = [];
  for await (const row of rows) {
    batch.push(row);
    if (batch.length < batchSize) continue;
    yield* await resolveContentDictInputs(batch, projectId);
    batch = [];
  }

  if (batch.length > 0) {
    yield* await resolveContentDictInputs(batch, projectId);
  }
}

export const findContentDictInputMatches = async (
  projectId: string,
  phrase: string,
): Promise<ContentDictInputSearchMatch[]> => {
  const rows = await queryDoris<{
    start_time: string;
    content_hash: string;
  }>({
    query: `
      SELECT start_time, content_hash
      FROM ${tableFor(projectId, "content_dict")}
      WHERE content MATCH_PHRASE {contentPhrase: String}
      LIMIT 10000
    `,
    params: { contentPhrase: phrase },
    tags: {
      feature: "tracing",
      type: "content_dict",
      kind: "search",
      projectId,
    },
  });
  const hashesByStartTime = new Map<string, string[]>();
  for (const row of rows) {
    const startTime = contentDictStartTimeForEventStartTime(row.start_time);
    const hashes = hashesByStartTime.get(startTime) ?? [];
    if (!hashes.includes(row.content_hash)) hashes.push(row.content_hash);
    hashesByStartTime.set(startTime, hashes);
  }

  return Array.from(hashesByStartTime, ([start_time, contentHashes]) => ({
    start_time,
    contentHashes,
  }));
};
