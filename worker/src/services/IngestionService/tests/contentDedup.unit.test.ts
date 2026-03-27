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

  it("should dedup structured input with systemPrompt and messages", () => {
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

  it("should preserve non-string content fields", () => {
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

  it("should handle empty messages array", () => {
    const input = {
      systemPrompt: [{ type: "text", content: "system" }],
      messages: [],
    };

    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    expect(contentEntries).toHaveLength(1);
    expect(transformedInput.messages).toEqual([]);
  });

  it("should handle messages without parts array", () => {
    const input = {
      messages: [{ role: "user", text: "no parts here" }],
    };

    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    // Messages array replaced with hash array
    expect(transformedInput.messages).toEqual([
      sha256(JSON.stringify(input.messages[0])),
    ]);
    expect(contentEntries).toHaveLength(1);
  });

  it("should produce deterministic hashes for same content", () => {
    const content = "Same content repeated";
    const input = {
      systemPrompt: [{ type: "text", content }],
      messages: [{ role: "user", parts: [{ type: "text", content }] }],
    };

    const { contentEntries } = deduplicateInputContent(input);

    // Both entries should have different hashes (whole-element hash)
    expect(contentEntries).toHaveLength(2);
    expect(contentEntries[0].content_hash).not.toBe(
      contentEntries[1].content_hash,
    );
  });

  it("round-trip: dedup + resolve should restore original", () => {
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
