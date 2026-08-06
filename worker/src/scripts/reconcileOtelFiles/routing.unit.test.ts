import { describe, it, expect } from "vitest";

import { candidateGroupingKeys, reinjectionGroupingKey } from "./routing";

const SHARDS = ["shard-0", "shard-1", "shard-2"];
const SPLIT = "cmqiwxsca0006pj070fdkn0vd";
const first = (s: string[]) => s[0] as string;

const resolveLane = (pid: string): string => `lane-${pid}`;

describe("candidateGroupingKeys", () => {
  it("returns only the project lane", () => {
    expect(candidateGroupingKeys(SPLIT, SHARDS, resolveLane)).toEqual([
      `lane-${SPLIT}`,
    ]);
  });

  it("does not fall back to shards", () => {
    expect(candidateGroupingKeys("other", SHARDS, resolveLane)).toEqual([
      "lane-other",
    ]);
  });
});

describe("reinjectionGroupingKey", () => {
  it("uses the project lane, never a shard", () => {
    expect(reinjectionGroupingKey(SPLIT, SHARDS, first, resolveLane)).toBe(
      `lane-${SPLIT}`,
    );
  });

  it("does not use the picked shard", () => {
    expect(reinjectionGroupingKey("other", SHARDS, first, resolveLane)).toBe(
      "lane-other",
    );
  });
});
