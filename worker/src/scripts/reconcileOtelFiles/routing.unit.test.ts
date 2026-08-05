import { describe, it, expect } from "vitest";

import { candidateGroupingKeys, reinjectionGroupingKey } from "./routing";

const SHARDS = ["shard-0", "shard-1", "shard-2"];
const SPLIT = "cmqiwxsca0006pj070fdkn0vd";
const first = (s: string[]) => s[0] as string;

// Split projects resolve to their lane; everything else resolves to null.
const resolveLane = (pid: string): string | null =>
  pid === SPLIT ? `lane-${pid}` : null;

describe("candidateGroupingKeys", () => {
  it("split project → lane first, then all shards (covers a pre-flip shard reg key)", () => {
    expect(candidateGroupingKeys(SPLIT, SHARDS, resolveLane)).toEqual([
      `lane-${SPLIT}`,
      ...SHARDS,
    ]);
  });

  it("non-split project → shards only (unchanged behaviour)", () => {
    expect(candidateGroupingKeys("other", SHARDS, resolveLane)).toEqual(SHARDS);
  });

  it("lane disabled (resolveLane → null for all) → shards only (mode=none parity)", () => {
    expect(candidateGroupingKeys(SPLIT, SHARDS, () => null)).toEqual(SHARDS);
  });
});

describe("reinjectionGroupingKey", () => {
  it("split project → its own lane, never a shard", () => {
    expect(reinjectionGroupingKey(SPLIT, SHARDS, first, resolveLane)).toBe(
      `lane-${SPLIT}`,
    );
  });

  it("non-split project → the picked shard", () => {
    expect(reinjectionGroupingKey("other", SHARDS, first, resolveLane)).toBe(
      "shard-0",
    );
  });

  it("lane disabled → the picked shard (mode=none parity)", () => {
    expect(reinjectionGroupingKey(SPLIT, SHARDS, first, () => null)).toBe(
      "shard-0",
    );
  });
});
