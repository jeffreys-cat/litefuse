import { TracingSearchType } from "../../../interfaces/search";

export interface DorisSearchResult {
  query: string;
  params: Record<string, unknown>;
}

export interface DorisSearchContext {
  /** Query context type: traces or observations */
  type: "traces" | "observations";
  /** Whether the query joins the traces table */
  hasTracesJoin?: boolean;
}

export interface ContentDictInputSearchMatch {
  start_time: string;
  contentHashes: string[];
}

export const CONTENT_DICT_HASH_BATCH_SIZE = 100;

export const batchContentDictInputSearchMatches = (
  matches: ContentDictInputSearchMatch[],
): ContentDictInputSearchMatch[] =>
  matches.flatMap(({ start_time, contentHashes }) => {
    const batches: ContentDictInputSearchMatch[] = [];
    for (
      let offset = 0;
      offset < contentHashes.length;
      offset += CONTENT_DICT_HASH_BATCH_SIZE
    ) {
      batches.push({
        start_time,
        contentHashes: contentHashes.slice(
          offset,
          offset + CONTENT_DICT_HASH_BATCH_SIZE,
        ),
      });
    }
    return batches;
  });

/**
 * Generate Doris-compatible search conditions
 * Adapted for Doris syntax
 * @param query - Search query string
 * @param searchType - Types of search to perform
 * @param context - Context information for determining correct table prefixes
 */
export const dorisSearchCondition = (
  query?: string,
  searchType?: TracingSearchType[],
  context?: DorisSearchContext,
  inputContentMatches: ContentDictInputSearchMatch[] = [],
): DorisSearchResult => {
  if (!query) {
    return {
      query: "",
      params: {},
    };
  }

  // ID search uses a parameterized LIKE (substring match)
  const searchParam = `%${query}%`;
  const params: Record<string, unknown> = {
    searchQuery: searchParam,
  };

  const conditions = [];

  // ID search: column prefixes depend on the query context
  if (!searchType || searchType.includes("id")) {
    if (context?.type === "observations") {
      // observations context: in events_full the observation identifier column
      // is span_id (there is no `id` column).
      conditions.push(
        context.hasTracesJoin
          ? `o.span_id LIKE {searchQuery: String} OR o.name LIKE {searchQuery: String} OR t.user_id LIKE {searchQuery: String}`
          : `o.span_id LIKE {searchQuery: String} OR o.name LIKE {searchQuery: String}`,
      );
    } else {
      // traces context (default): events_full uses trace_id as the trace
      // identifier, and the trace name lives in trace_name (not the root span name).
      conditions.push(
        `t.trace_id LIKE {searchQuery: String} OR t.user_id LIKE {searchQuery: String} OR t.trace_name LIKE {searchQuery: String}`,
      );
    }
  }

  // Output remains inline and uses its inverted index. Input payloads live in
  // content_dict, so callers first find matches there. The second query
  // searches each matching events_full day with its own MATCH_ANY hash list.
  if (searchType && searchType.includes("content")) {
    const inputColumn = context?.type === "observations" ? "o.input" : "input";
    const inputStartTimeColumn =
      context?.type === "observations" ? "o.start_time" : "start_time";
    const inputConditions = batchContentDictInputSearchMatches(
      inputContentMatches,
    ).flatMap(({ start_time, contentHashes }, index) => {
      if (contentHashes.length === 0) return [];
      params[`contentStartTime${index}`] = start_time;
      params[`contentHashQuery${index}`] = contentHashes.join(" ");
      return [
        `(DATE(${inputStartTimeColumn}) = {contentStartTime${index}: Date} AND ${inputColumn} MATCH_ANY {contentHashQuery${index}: String})`,
      ];
    });
    const inputCondition =
      inputConditions.length > 0
        ? `(${inputConditions.join(" OR ")})`
        : "FALSE";
    if (context?.type === "observations") {
      conditions.push(
        `${inputCondition} OR o.output MATCH_PHRASE {searchPhrase: String}`,
      );
    } else {
      // traces queries usually don't search input/output, but it's supported
      // here if the query joins the observations rows.
      conditions.push(
        `${inputCondition} OR output MATCH_PHRASE {searchPhrase: String}`,
      );
    }
  }

  // MATCH_PHRASE takes the bare term (no % wildcards).
  if (searchType?.includes("content")) {
    params.searchPhrase = query;
  }

  return {
    query: conditions.length > 0 ? `AND (${conditions.join(" OR ")})` : "",
    params,
  };
};
