import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../../../src/server/repositories/doris", () => ({
  parseDorisUTCDateTimeFormat: (value: string) =>
    new Date(value.includes("T") || value.endsWith("Z") ? value : `${value}Z`),
  queryDoris: vi.fn(),
}));
vi.mock("../../../src/server/doris/tableRouting", () => ({
  tableFor: (projectId: string, table: string) => `${table}_${projectId}`,
}));

import {
  resolveContentDictInputs,
  restoreContentDictInputs,
  type ContentRecordInsertType,
} from "../../../src/server/repositories/contentDict";
import { queryDoris } from "../../../src/server/repositories/doris";

describe("restoreContentDictInputs", () => {
  beforeEach(() => {
    vi.mocked(queryDoris).mockReset();
  });

  it("normalizes the Date returned for a content_dict partition", () => {
    const hash = "a".repeat(64);
    const contentRows: ContentRecordInsertType[] = [
      {
        // JSONEachRow returns Doris DATE values as midnight UTC timestamps.
        start_time: "2026-07-28T00:00:00.000Z",
        content_hash: hash,
        content: '{"prompt":"Hello"}',
      },
    ];

    const [restored] = restoreContentDictInputs(
      [
        {
          start_time: "2026-07-28 11:13:02.305",
          input: hash,
        },
      ],
      contentRows,
    );

    expect(restored.input).toBe('[{"prompt":"Hello"}]');
  });

  it("loads dictionary rows in batches of 100 hashes", async () => {
    const hashes = Array.from({ length: 101 }, (_, index) =>
      index.toString(16).padStart(64, "0"),
    );
    vi.mocked(queryDoris).mockImplementation(async (opts) => {
      const contentHashes = opts.params?.contentHashes as string[];
      return contentHashes.map((content_hash) => ({
        start_time: "2026-07-28T00:00:00.000Z",
        content_hash,
        content: JSON.stringify({ content_hash }),
      })) as never;
    });

    const [restored] = await resolveContentDictInputs(
      [
        {
          start_time: "2026-07-28 11:13:02.305",
          input: hashes.join(" "),
        },
      ],
      "project-1",
    );

    expect(queryDoris).toHaveBeenCalledTimes(2);
    expect(vi.mocked(queryDoris).mock.calls[0][0].query).toContain(
      "FROM content_dict_project-1",
    );
    expect(vi.mocked(queryDoris).mock.calls[0][0].query).not.toContain(
      "project_id",
    );
    expect(vi.mocked(queryDoris).mock.calls[0][0].params).not.toHaveProperty(
      "projectId",
    );
    expect(
      vi
        .mocked(queryDoris)
        .mock.calls.map(
          ([{ params }]) => (params?.contentHashes as string[]).length,
        ),
    ).toEqual([100, 1]);
    expect(JSON.parse(restored.input as string)).toHaveLength(101);
  });
});
