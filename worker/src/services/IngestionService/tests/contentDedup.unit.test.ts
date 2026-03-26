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

    // Verify content replaced with hashes
    expect(transformedInput.systemPrompt[0].content).toBeUndefined();
    expect(transformedInput.systemPrompt[0].content_hash).toBe(
      sha256("You are an AI assistant."),
    );

    expect(transformedInput.messages[0].parts[0].content).toBeUndefined();
    expect(transformedInput.messages[0].parts[0].content_hash).toBe(
      sha256("hello"),
    );

    expect(transformedInput.messages[1].parts[0].content).toBeUndefined();
    expect(transformedInput.messages[1].parts[0].content_hash).toBe(
      sha256("hi there"),
    );

    // Verify content entries
    expect(contentEntries).toHaveLength(3);
    expect(contentEntries.map((e) => e.content).sort()).toEqual(
      ["You are an AI assistant.", "hello", "hi there"].sort(),
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

    // Non-string content should not be hashed
    expect(transformedInput.systemPrompt[0].content).toEqual({
      url: "http://example.com",
    });
    expect(transformedInput.systemPrompt[0].content_hash).toBeUndefined();
    expect(contentEntries).toHaveLength(0);
  });

  it("should handle empty messages array", () => {
    const input = {
      systemPrompt: [{ type: "text", content: "system" }],
      messages: [],
    };

    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    expect(contentEntries).toHaveLength(1);
    expect(contentEntries[0].content).toBe("system");
    expect(transformedInput.messages).toEqual([]);
  });

  it("should handle messages without parts array", () => {
    const input = {
      messages: [{ role: "user", text: "no parts here" }],
    };

    const { transformedInput, contentEntries } = deduplicateInputContent(input);

    // Message without parts should be left unchanged
    expect(transformedInput.messages[0]).toEqual({
      role: "user",
      text: "no parts here",
    });
    expect(contentEntries).toHaveLength(0);
  });

  it("should produce deterministic hashes for same content", () => {
    const content = "Same content repeated";
    const input = {
      systemPrompt: [{ type: "text", content }],
      messages: [{ role: "user", parts: [{ type: "text", content }] }],
    };

    const { contentEntries } = deduplicateInputContent(input);

    // Both entries should have the same hash
    expect(contentEntries).toHaveLength(2);
    expect(contentEntries[0].content_hash).toBe(contentEntries[1].content_hash);
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

    // Simulate content_dict table
    const dict = new Map(
      contentEntries.map((e) => [e.content_hash, e.content]),
    );

    // Read path: restore (inline simulation without DB)
    function restore(obj: any): void {
      if (!obj || typeof obj !== "object") return;
      if (obj.content_hash && dict.has(obj.content_hash)) {
        obj.content = dict.get(obj.content_hash);
        delete obj.content_hash;
        return;
      }
      if (Array.isArray(obj)) {
        obj.forEach((item) => restore(item));
      } else {
        Object.values(obj).forEach((val) => restore(val));
      }
    }

    const restored = JSON.parse(JSON.stringify(transformedInput));
    restore(restored);

    expect(restored).toEqual(original);
  });
});
