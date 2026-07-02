import { describe, it, expect, vi } from "vitest";

// OtelIngestionProcessor imports env (zod-validated at module load) — stub the
// only fields it reads so the class can be constructed in isolation.
vi.mock("../../../env", () => ({
  env: {
    LITEFUSE_S3_EVENT_UPLOAD_PREFIX: "",
    LITEFUSE_S3_EVENT_UPLOAD_BUCKET: "test-bucket",
  },
}));
vi.mock("../../logger", () => ({
  logger: { info: vi.fn(), debug: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { OtelIngestionProcessor } from "../OtelIngestionProcessor";

const KEY = "langfuse.observation.completion_start_time";

const proc: any = new OtelIngestionProcessor({ projectId: "test-project" });
const call = (v: unknown, startISO?: string) =>
  proc.extractCompletionStartTime({ [KEY]: v }, startISO);

// Regression guard for the guarded JSON.parse (avoids a per-span SyntaxError
// throw when completion_start_time is absent). Output must be byte-identical to
// the pre-guard behavior for every value shape.
describe("OtelIngestionProcessor.extractCompletionStartTime", () => {
  it("absent attribute → null (no throw)", () => {
    expect(proc.extractCompletionStartTime({}, undefined)).toBeNull();
  });

  it("undefined → null", () => {
    expect(call(undefined)).toBeNull();
  });

  it("empty string → null", () => {
    expect(call("")).toBeNull();
  });

  it("plain valid date string → returned as-is", () => {
    expect(call("2025-10-01T08:45:26.112Z")).toBe("2025-10-01T08:45:26.112Z");
  });

  it("double-encoded date string → parsed value returned", () => {
    expect(call('"2025-10-01T08:45:26.112Z"')).toBe("2025-10-01T08:45:26.112Z");
  });

  it("non-JSON, non-date string → null (parse throws, caught)", () => {
    expect(call("hello")).toBeNull();
  });

  it("object → null (no throw)", () => {
    expect(call({ a: 1 })).toBeNull();
  });

  it("null → returned as-is (new Date(null) is valid; pre-existing behavior)", () => {
    expect(call(null)).toBeNull();
  });

  it("Vercel AI SDK msToFirstChunk → start + ms", () => {
    const start = "2025-10-01T00:00:00.000Z";
    expect(
      proc.extractCompletionStartTime(
        { "ai.response.msToFirstChunk": 500 },
        start,
      ),
    ).toBe(new Date(Date.parse(start) + 500).toISOString());
  });
});
