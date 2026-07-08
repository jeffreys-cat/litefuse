import { describe, it, expect } from "vitest";

import { truncateWellFormed } from "./truncateWellFormed";

describe("truncateWellFormed", () => {
  it("repairs a surrogate pair split exactly at the limit (the poison-batch case)", () => {
    // "🇪🇸" = two regional indicators = 4 UTF-16 code units. Cutting at an odd
    // offset inside splits a pair — a bare slice leaves a lone high surrogate
    // (exactly what wedged the events_full batch: "...| 待定 | 🇪\ud83c").
    const s = "| 待定 | 🇪🇸 西班牙";
    // Units: 0-6 ASCII/CJK, 7-8 = 🇪 (one pair), 9-10 = 🇸. Cutting at 10
    // keeps 🇸's high surrogate without its low half.
    const cut = truncateWellFormed(s, 10);
    expect(cut.isWellFormed()).toBe(true);
    expect(cut.endsWith("�")).toBe(true); // the cut half → replacement char
    // A bare slice at the same offset is NOT well-formed — the bug being fixed.
    expect(s.slice(0, 10).isWellFormed()).toBe(false);
  });

  it("repairs lone surrogates already present inside the kept window", () => {
    const s = `upstream sent half an emoji \uD83D and text`;
    const out = truncateWellFormed(s, 200);
    expect(out.isWellFormed()).toBe(true);
    expect(out).toContain("�");
  });

  it("leaves clean strings untouched (incl. complete emoji at the boundary)", () => {
    const s = "ok 📝 done";
    expect(truncateWellFormed(s, 200)).toBe(s);
    // Exact fit: "📝" is 2 units; cutting right AFTER a complete pair keeps it.
    const t = "ab📝";
    expect(truncateWellFormed(t, 4)).toBe("ab📝");
  });

  it("respects the max length in code units", () => {
    expect(truncateWellFormed("x".repeat(500), 200)).toHaveLength(200);
  });
});
