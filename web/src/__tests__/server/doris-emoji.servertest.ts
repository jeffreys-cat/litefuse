import { v4 } from "uuid";
import { commandDoris, queryDoris } from "@langfuse/shared/src/server";

// Regression test for emoji / 4-byte UTF-8 mojibake: Doris reports charset 33
// (utf8) but stores utf8mb4; mysql2 maps charset 33 to cesu8 (3-byte), turning
// 4-byte emoji into U+FFFD ('�') for non-JSON string columns (e.g. CAST(input AS
// STRING), name). client.ts overrides CharsetToEncoding[33] = "utf8" to fix it.
describe("Doris emoji / 4-byte UTF-8 round-trip", () => {
  const now = new Date();
  const dateStr = now.toISOString().slice(0, 10);
  const dtStr = now.toISOString().slice(0, 19).replace("T", " ");
  const EMOJI = "hello 😀 世界 🌍 ✅";

  it("preserves emoji through CAST(input AS STRING) and a plain string column", async () => {
    const projectId = v4();
    const traceId = v4();

    await commandDoris({
      query: `INSERT INTO events_full
        (project_id, trace_id, start_time_date, span_id, parent_span_id, is_root,
         start_time, name, environment, event_ts, is_deleted, input)
        VALUES
        ('${projectId}', '${traceId}', '${dateStr}', 't-${traceId}', '', 1,
         '${dtStr}', '${EMOJI}', 'default', '${dtStr}', 0,
         CAST('{"msg":"${EMOJI}"}' AS JSON))`,
    });

    const rows = await queryDoris<{ io: string; nm: string }>({
      query: `SELECT CAST(input AS STRING) AS io, name AS nm
              FROM events_full
              WHERE project_id = {projectId: String} AND trace_id = {traceId: String}`,
      params: { projectId, traceId },
    });

    expect(rows).toHaveLength(1);
    // No replacement characters.
    expect(rows[0].io).not.toContain("�");
    expect(rows[0].nm).not.toContain("�");
    // Emoji + CJK survive intact.
    expect(rows[0].io).toContain("😀");
    expect(rows[0].io).toContain("🌍");
    expect(rows[0].io).toContain("世界");
    expect(rows[0].io).toContain("✅");
    expect(rows[0].nm).toEqual(EMOJI);
  });
});
