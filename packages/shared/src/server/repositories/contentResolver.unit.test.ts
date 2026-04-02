import { describe, it, expect, vi, beforeEach } from "vitest";
import { resolveContentReferences } from "./contentResolver";

// Mock queryDoris
vi.mock("./doris", () => ({
  queryDoris: vi.fn(),
}));

import { queryDoris } from "./doris";

const mockQueryDoris = vi.mocked(queryDoris);

describe("resolveContentReferences", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return unchanged when input is null", async () => {
    const observations = [{ input: null }];
    await resolveContentReferences(observations);
    expect(observations[0].input).toBeNull();
    expect(mockQueryDoris).not.toHaveBeenCalled();
  });

  it("should return unchanged when input is undefined", async () => {
    const observations = [{}];
    await resolveContentReferences(observations);
    expect(mockQueryDoris).not.toHaveBeenCalled();
  });

  it("should return unchanged when no content_hash found", async () => {
    const observations = [{ input: JSON.stringify({ foo: "bar" }) }];
    await resolveContentReferences(observations);
    expect(observations[0].input).toBe('{"foo":"bar"}');
    expect(mockQueryDoris).not.toHaveBeenCalled();
  });

  // ===== New top-level array format =====

  it("should resolve new top-level array format with hash strings", async () => {
    const hash1 = "a".repeat(64);
    const hash2 = "b".repeat(64);
    const content1 = JSON.stringify({
      role: "system",
      parts: [{ type: "text", content: "You are AI" }],
    });
    const content2 = JSON.stringify({
      role: "user",
      parts: [{ type: "text", content: "hello" }],
    });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
      { content_hash: hash2, content: content2 },
    ]);

    const observations = [{ input: JSON.stringify([hash1, hash2]) }];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed).toHaveLength(2);
    expect(parsed[0]).toMatchObject({
      role: "system",
      parts: [{ type: "text", content: "You are AI" }],
    });
    expect(parsed[1]).toMatchObject({
      role: "user",
      parts: [{ type: "text", content: "hello" }],
    });
  });

  it("should resolve top-level array with mixed content (some hashes, some objects)", async () => {
    const hash1 = "a".repeat(64);
    const content1 = JSON.stringify({
      role: "system",
      parts: [{ type: "text", content: "system" }],
    });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
    ]);

    const observations = [
      {
        input: JSON.stringify([
          hash1,
          { role: "user", parts: [{ type: "text", content: "keep" }] },
        ]),
      },
    ];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed[0]).toMatchObject({
      role: "system",
      parts: [{ type: "text", content: "system" }],
    });
    expect(parsed[1]).toEqual({
      role: "user",
      parts: [{ type: "text", content: "keep" }],
    });
  });

  it("should add unique id to resolved items without id", async () => {
    const hash1 = "a".repeat(64);
    const content1 = JSON.stringify({
      role: "system",
      parts: [{ type: "text", content: "You are AI" }],
    });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
    ]);

    const observations = [{ input: JSON.stringify([hash1]) }];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed[0]).toMatchObject({
      role: "system",
      parts: [{ type: "text", content: "You are AI" }],
    });
  });

  it("should preserve id when resolving top-level array items", async () => {
    const hash1 = "a".repeat(64);
    const content1 = JSON.stringify({
      id: "existing-id",
      role: "system",
      parts: [{ type: "text", content: "You are AI" }],
    });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
    ]);

    const observations = [{ input: JSON.stringify([hash1]) }];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed[0].id).toBe("existing-id");
  });

  // ===== Legacy format (systemPrompt + messages) =====

  it("should resolve hash array in systemPrompt (legacy)", async () => {
    const hash1 = "a".repeat(64);
    const hash2 = "b".repeat(64);
    const content1 = JSON.stringify({ type: "text", content: "You are AI" });
    const content2 = JSON.stringify({
      type: "image",
      url: "http://example.com",
    });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
      { content_hash: hash2, content: content2 },
    ]);

    const observations = [
      { input: JSON.stringify({ systemPrompt: [hash1, hash2] }) },
    ];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed.systemPrompt).toHaveLength(2);
    expect(parsed.systemPrompt[0]).toMatchObject({
      type: "text",
      content: "You are AI",
    });
    expect(parsed.systemPrompt[1]).toMatchObject({
      type: "image",
      url: "http://example.com",
    });
  });

  it("should resolve hash array in messages (legacy)", async () => {
    const hash1 = "c".repeat(64);
    const hash2 = "d".repeat(64);
    const content1 = JSON.stringify({
      role: "user",
      parts: [{ type: "text", content: "hello" }],
    });
    const content2 = JSON.stringify({
      role: "assistant",
      parts: [{ type: "text", content: "hi" }],
    });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
      { content_hash: hash2, content: content2 },
    ]);

    const observations = [
      { input: JSON.stringify({ messages: [hash1, hash2] }) },
    ];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed.messages).toHaveLength(2);
    expect(parsed.messages[0]).toMatchObject({
      role: "user",
      parts: [{ type: "text", content: "hello" }],
    });
    expect(parsed.messages[1]).toMatchObject({
      role: "assistant",
      parts: [{ type: "text", content: "hi" }],
    });
  });

  it("should resolve both systemPrompt and messages hash arrays (legacy)", async () => {
    const hash1 = "a".repeat(64);
    const hash2 = "c".repeat(64);
    const content1 = JSON.stringify({ type: "text", content: "system" });
    const content2 = JSON.stringify({ role: "user", content: "hello" });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
      { content_hash: hash2, content: content2 },
    ]);

    const observations = [
      {
        input: JSON.stringify({
          systemPrompt: [hash1],
          messages: [hash2],
        }),
      },
    ];

    await resolveContentReferences(observations);
    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed.systemPrompt[0]).toMatchObject({
      type: "text",
      content: "system",
    });
    expect(parsed.messages[0]).toMatchObject({
      role: "user",
      content: "hello",
    });
  });

  it("should handle mixed hash array and regular array (legacy)", async () => {
    const hash1 = "a".repeat(64);
    const content1 = JSON.stringify({ type: "text", content: "resolved" });

    mockQueryDoris.mockResolvedValue([
      { content_hash: hash1, content: content1 },
    ]);

    const observations = [
      {
        input: JSON.stringify({
          systemPrompt: [hash1, { type: "text", content: "keep" }],
        }),
      },
    ];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed.systemPrompt[0]).toMatchObject({
      type: "text",
      content: "resolved",
    });
    expect(parsed.systemPrompt[1]).toEqual({ type: "text", content: "keep" });
  });

  it("should handle old format with content_hash field in object (legacy)", async () => {
    const hash = "a".repeat(64);
    const content = "Hello world";

    mockQueryDoris.mockResolvedValue([{ content_hash: hash, content }]);

    const observations = [
      { input: JSON.stringify({ messages: [{ content_hash: hash }] }) },
    ];

    await resolveContentReferences(observations);

    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed.messages[0].content).toBe("Hello world");
    expect(parsed.messages[0].content_hash).toBeUndefined();
  });

  // ===== Edge cases =====

  it("should handle empty arrays", async () => {
    const observations = [{ input: JSON.stringify([]) }];

    await resolveContentReferences(observations);

    expect(mockQueryDoris).not.toHaveBeenCalled();
    expect(observations[0].input).toBe("[]");
  });

  it("should handle empty legacy format arrays", async () => {
    const observations = [
      { input: JSON.stringify({ systemPrompt: [], messages: [] }) },
    ];

    await resolveContentReferences(observations);

    expect(mockQueryDoris).not.toHaveBeenCalled();
    expect(observations[0].input).toBe('{"systemPrompt":[],"messages":[]}');
  });

  it("should query in batches when many hashes", async () => {
    const hashes = Array.from({ length: 600 }, (_, i) => {
      const num = i.toString(16).padStart(64, "0");
      return num;
    });
    const contentEntries = hashes.map((h) => ({
      content_hash: h,
      content: `{"index":"${h}"}`,
    }));

    mockQueryDoris.mockResolvedValue(contentEntries);

    // New format: top-level array
    const input = JSON.stringify(hashes);
    const observations = [{ input }];

    await resolveContentReferences(observations);

    // Should be called twice: 500 + 100
    expect(mockQueryDoris).toHaveBeenCalledTimes(2);
  });

  it("should gracefully handle missing content in dict", async () => {
    const hash = "a".repeat(64);

    // Return empty (hash not found)
    mockQueryDoris.mockResolvedValue([]);

    // New format
    const observations = [{ input: JSON.stringify([hash]) }];

    await resolveContentReferences(observations);

    // Hash should remain unchanged (graceful degradation)
    const parsed = JSON.parse(observations[0].input as string);
    expect(parsed[0]).toBe(hash);
  });

  // ===== Round-trip tests =====

  it("round-trip: dedup + resolve should restore new array format", async () => {
    // Use content that already has id fields to avoid auto-generated id mismatch
    const original = [
      {
        id: "msg-1",
        role: "system",
        parts: [{ type: "text", content: "You are a helpful assistant." }],
      },
      {
        id: "msg-2",
        role: "user",
        parts: [{ type: "text", content: "Hello" }],
      },
      {
        id: "msg-3",
        role: "assistant",
        parts: [{ type: "text", content: "Hi there" }],
      },
    ];

    // Simulate deduplication
    const sha256 = (s: string) => {
      let h = "";
      for (let i = 0; i < 64; i++) {
        h += s.charCodeAt(i % s.length).toString(16);
      }
      return h.padEnd(64, "0").slice(0, 64);
    };

    const contentMap = new Map<string, string>();
    const hashArray = (arr: any[]) =>
      arr.map((item) => {
        const serialized = JSON.stringify(item);
        const h = sha256(serialized);
        contentMap.set(h, serialized);
        return h;
      });

    const transformed = hashArray(original);

    // Simulate read path (resolveContentReferences)
    mockQueryDoris.mockImplementation(() =>
      Promise.resolve(
        Array.from(contentMap.entries()).map(([content_hash, content]) => ({
          content_hash,
          content,
        })),
      ),
    );

    const observations = [{ input: JSON.stringify(transformed) }];
    await resolveContentReferences(observations);

    const restored = JSON.parse(observations[0].input as string);
    expect(restored).toEqual(original);
  });

  it("round-trip: dedup + resolve should restore legacy format", async () => {
    // Simulate write path (using logic from contentDedup)
    // Use content that already has id fields to avoid auto-generated id mismatch
    const original = {
      systemPrompt: [
        { id: "sp-1", type: "text", content: "You are a helpful assistant." },
      ],
      messages: [
        {
          id: "msg-1",
          role: "user",
          parts: [{ type: "text", content: "Hello" }],
        },
        {
          id: "msg-2",
          role: "assistant",
          parts: [{ type: "text", content: "Hi there" }],
        },
      ],
    };

    // Simulate deduplication (hash each element)
    const sha256 = (s: string) => {
      let h = "";
      for (let i = 0; i < 64; i++) {
        h += s.charCodeAt(i % s.length).toString(16);
      }
      return h.padEnd(64, "0").slice(0, 64);
    };

    const contentMap = new Map<string, string>();
    const hashArray = (arr: any[]) =>
      arr.map((item) => {
        const serialized = JSON.stringify(item);
        const h = sha256(serialized);
        contentMap.set(h, serialized);
        return h;
      });

    const transformed = {
      systemPrompt: hashArray(original.systemPrompt),
      messages: hashArray(original.messages),
    };

    // Simulate read path (resolveContentReferences)
    mockQueryDoris.mockImplementation(() =>
      Promise.resolve(
        Array.from(contentMap.entries()).map(([content_hash, content]) => ({
          content_hash,
          content,
        })),
      ),
    );

    const observations = [{ input: JSON.stringify(transformed) }];
    await resolveContentReferences(observations);

    const restored = JSON.parse(observations[0].input as string);
    expect(restored).toEqual(original);
  });
});
