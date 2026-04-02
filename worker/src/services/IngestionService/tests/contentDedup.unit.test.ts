import { describe, it, expect } from "vitest";
import { deduplicateInputContent, sha256 } from "../contentDedup";

describe("deduplicateInputContent", () => {
  it("should return null input unchanged", () => {
    const result = deduplicateInputContent(null);
    expect(result.transformedInput).toBeNull();
    expect(result.contentEntries).toEqual([]);
  });

  it("should return undefined input unchanged", () => {
    const result = deduplicateInputContent(undefined);
    expect(result.transformedInput).toBeUndefined();
    expect(result.contentEntries).toEqual([]);
  });

  it("should hash plain string input as a single entry", () => {
    const input = "Hello, world!";
    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    expect(transformedInput).toEqual({ content_hash: sha256(input) });
    expect(contentEntries).toHaveLength(1);
    expect(contentEntries[0].content).toBe(input);
    expect(contentEntries[0].content_hash).toBe(sha256(input));
  });

  it("should hash non-structured object as a single serialized entry", () => {
    const input = { foo: "bar", baz: 42 };
    const serialized = JSON.stringify(input);
    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    expect(transformedInput).toEqual({ content_hash: sha256(serialized) });
    expect(contentEntries).toHaveLength(1);
    expect(contentEntries[0].content).toBe(serialized);
  });

  // ===== New top-level array format =====

  it("should dedup new top-level array format", () => {
    const original = [
      { role: "system", parts: [{ type: "text", content: "You are an AI." }] },
      { role: "user", parts: [{ type: "text", content: "hello" }] },
      { role: "assistant", parts: [{ type: "text", content: "hi there" }] },
    ];

    const { transformedInput, contentEntries } =
      deduplicateInputContent(original);

    // Should return array of hashes
    expect(Array.isArray(transformedInput)).toBe(true);
    expect(transformedInput).toHaveLength(3);
    expect(transformedInput[0]).toBe(sha256(JSON.stringify(original[0])));
    expect(transformedInput[1]).toBe(sha256(JSON.stringify(original[1])));
    expect(transformedInput[2]).toBe(sha256(JSON.stringify(original[2])));

    // Content entries should have serialized JSON
    expect(contentEntries).toHaveLength(3);
    expect(JSON.parse(contentEntries[0].content)).toEqual(original[0]);
    expect(JSON.parse(contentEntries[1].content)).toEqual(original[1]);
    expect(JSON.parse(contentEntries[2].content)).toEqual(original[2]);
  });

  it("should handle mixed content in top-level array", () => {
    const original = [
      { type: "text", content: "system prompt" },
      { type: "image", url: "http://example.com/image.jpg" },
    ];

    const { transformedInput, contentEntries } =
      deduplicateInputContent(original);

    expect(transformedInput).toHaveLength(2);
    expect(contentEntries).toHaveLength(2);
  });

  it("should handle non-object elements in top-level array", () => {
    const original = [
      { type: "text", content: "valid object" },
      "just a string",
      123,
    ];

    const { transformedInput, contentEntries } =
      deduplicateInputContent(original);

    // Non-object items are kept as-is, only objects get hashed
    expect(transformedInput[0]).toBe(sha256(JSON.stringify(original[0])));
    expect(transformedInput[1]).toBe("just a string");
    expect(transformedInput[2]).toBe(123);
    expect(contentEntries).toHaveLength(1); // Only 1 object entry
  });

  it("should produce deterministic hashes for same content", () => {
    const content1 = { type: "text", content: "Same content" };
    const content2 = { type: "text", content: "Same content" };

    const { contentEntries: entries1 } = deduplicateInputContent([content1]);
    const { contentEntries: entries2 } = deduplicateInputContent([content2]);

    // Same content should produce same hash
    expect(entries1[0].content_hash).toBe(entries2[0].content_hash);
  });

  it("should produce different hashes for different content", () => {
    const content1 = { type: "text", content: "Content A" };
    const content2 = { type: "text", content: "Content B" };

    const { contentEntries: entries1 } = deduplicateInputContent([content1]);
    const { contentEntries: entries2 } = deduplicateInputContent([content2]);

    expect(entries1[0].content_hash).not.toBe(entries2[0].content_hash);
  });

  // ===== Legacy structured format (systemPrompt + messages) =====

  it("should dedup structured input with systemPrompt and messages (legacy)", () => {
    const original = {
      systemPrompt: [{ type: "text", content: "You are an AI assistant." }],
      messages: [
        {
          role: "user",
          parts: [{ type: "text", content: "hello" }],
        },
        {
          role: "assistant",
          parts: [{ type: "text", content: "hi there" }],
        },
      ],
    };

    const { transformedInput, contentEntries } =
      deduplicateInputContent(original);

    // Verify arrays replaced with hash arrays
    expect(transformedInput.systemPrompt).toEqual([
      sha256(JSON.stringify(original.systemPrompt[0])),
    ]);

    expect(transformedInput.messages).toEqual([
      sha256(JSON.stringify(original.messages[0])),
      sha256(JSON.stringify(original.messages[1])),
    ]);

    // Verify content entries contain serialized JSON
    expect(contentEntries).toHaveLength(3);
    expect(contentEntries.map((e) => JSON.parse(e.content)).sort()).toEqual(
      [
        original.systemPrompt[0],
        original.messages[0],
        original.messages[1],
      ].sort(),
    );

    // Verify original is not mutated
    expect(original.systemPrompt[0].content).toBe("You are an AI assistant.");
  });

  it("should preserve non-string content fields in legacy format", () => {
    const input = {
      systemPrompt: [{ type: "image", content: { url: "http://example.com" } }],
      messages: [],
    };

    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    // SystemPrompt array is replaced with hash array
    expect(transformedInput.systemPrompt).toEqual([
      sha256(JSON.stringify(input.systemPrompt[0])),
    ]);
    expect(contentEntries).toHaveLength(1);
  });

  it("should handle empty messages array in legacy format", () => {
    const input = {
      systemPrompt: [{ type: "text", content: "system" }],
      messages: [],
    };

    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    expect(contentEntries).toHaveLength(1);
    expect(transformedInput.messages).toEqual([]);
  });

  // ===== Round-trip tests =====

  it("round-trip: dedup + resolve should restore new array format", () => {
    const original = [
      {
        role: "system",
        parts: [{ type: "text", content: "You are a helpful assistant." }],
      },
      { role: "user", parts: [{ type: "text", content: "What is 2+2?" }] },
      {
        role: "assistant",
        parts: [{ type: "text", content: "2+2 equals 4." }],
      },
      { role: "user", parts: [{ type: "text", content: "Thanks!" }] },
    ];

    // Write path: dedup
    const { transformedInput, contentEntries } =
      deduplicateInputContent(original);

    // After dedup, array contains hash strings
    expect(Array.isArray(transformedInput)).toBe(true);
    expect(transformedInput).toHaveLength(4);
    expect(typeof transformedInput[0]).toBe("string"); // hash
    expect(transformedInput[0]).toHaveLength(64); // SHA-256 length

    // Build dict for restore
    const dict = new Map(
      contentEntries.map((e) => [e.content_hash, e.content]),
    );

    // Read path: restore hash strings to original elements
    function restore(obj: any): void {
      if (!obj || typeof obj !== "object") return;
      if (Array.isArray(obj)) {
        const restored = [];
        for (const item of obj) {
          if (typeof item === "string" && dict.has(item)) {
            restored.push(JSON.parse(dict.get(item)!));
          } else {
            restored.push(item);
          }
        }
        obj.length = 0;
        obj.push(...restored);
        return;
      }
      Object.values(obj).forEach((val) => restore(val));
    }

    const restored = JSON.parse(JSON.stringify(transformedInput));
    restore(restored);

    expect(restored).toEqual(original);
  });

  it("round-trip: dedup + resolve should restore legacy format", () => {
    const original = {
      systemPrompt: [
        {
          type: "text",
          content:
            "You are a helpful assistant that helps users with their questions.",
        },
      ],
      messages: [
        { role: "user", parts: [{ type: "text", content: "What is 2+2?" }] },
        {
          role: "assistant",
          parts: [{ type: "text", content: "2+2 equals 4." }],
        },
        { role: "user", parts: [{ type: "text", content: "Thanks!" }] },
      ],
    };

    // Write path: dedup
    const { transformedInput, contentEntries } =
      deduplicateInputContent(original);

    // After dedup, systemPrompt and messages become hash arrays
    expect(Array.isArray(transformedInput.systemPrompt)).toBe(true);
    expect(Array.isArray(transformedInput.messages)).toBe(true);
    expect(transformedInput.systemPrompt.length).toBe(1);
    expect(transformedInput.messages.length).toBe(3);

    // Verify content entries are serialized JSON
    const dict = new Map(
      contentEntries.map((e) => [e.content_hash, e.content]),
    );

    // Read path: restore hash arrays to original elements
    function restore(obj: any): void {
      if (!obj || typeof obj !== "object") return;
      if (Array.isArray(obj)) {
        // Restore hash array to original elements
        const restored = [];
        for (const hash of obj) {
          if (dict.has(hash)) {
            restored.push(JSON.parse(dict.get(hash)!));
          }
        }
        obj.length = 0;
        obj.push(...restored);
        return;
      }
      Object.values(obj).forEach((val) => restore(val));
    }

    const restored = JSON.parse(JSON.stringify(transformedInput));
    restore(restored);

    expect(restored).toEqual(original);
  });
});
