import { describe, expect, it } from "vitest";

import { dorisSearchCondition } from "../../../../src/server/queries/doris-sql/search";

describe("dorisSearchCondition", () => {
  it("splits input hash searches into batches of 100", () => {
    const hashes = Array.from({ length: 101 }, (_, index) =>
      index.toString(16).padStart(64, "0"),
    );

    const search = dorisSearchCondition(
      "Hello",
      ["content"],
      { type: "observations" },
      [{ start_time: "2026-07-28", contentHashes: hashes }],
    );

    expect(search.query).toContain(
      "o.input MATCH_ANY {contentHashQuery0: String}",
    );
    expect(search.query).toContain(
      "o.input MATCH_ANY {contentHashQuery1: String}",
    );
    expect((search.params.contentHashQuery0 as string).split(" ")).toHaveLength(
      100,
    );
    expect((search.params.contentHashQuery1 as string).split(" ")).toHaveLength(
      1,
    );
  });
});
