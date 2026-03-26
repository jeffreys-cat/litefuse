import { createHash } from "crypto";

export interface ContentEntry {
  content_hash: string;
  content: string;
}

/**
 * Replace text content in observation input with SHA-256 hash references.
 * Supports three input formats:
 *   1. Structured: { systemPrompt: [...], messages: [...] } → per-part dedup
 *   2. Plain string → whole-string hash
 *   3. Other JSON object → serialized hash
 */
export function deduplicateInputContent(rawInput: any): {
  transformedInput: any;
  contentEntries: ContentEntry[];
} {
  if (rawInput == null) {
    return { transformedInput: rawInput, contentEntries: [] };
  }

  // Plain text input → hash the whole string
  if (typeof rawInput === "string") {
    const hash = sha256(rawInput);
    return {
      transformedInput: { content_hash: hash },
      contentEntries: [{ content_hash: hash, content: rawInput }],
    };
  }

  // Non-structured object → serialize and hash as one entry
  if (!rawInput.systemPrompt && !rawInput.messages) {
    const serialized = JSON.stringify(rawInput);
    const hash = sha256(serialized);
    return {
      transformedInput: { content_hash: hash },
      contentEntries: [{ content_hash: hash, content: serialized }],
    };
  }

  // Structured input → per-part dedup
  const entries: ContentEntry[] = [];
  const result = structuredClone(rawInput);

  function hashPart(part: any): void {
    if (part?.content && typeof part.content === "string") {
      const hash = sha256(part.content);
      entries.push({ content_hash: hash, content: part.content });
      part.content_hash = hash;
      delete part.content;
    }
  }

  // systemPrompt parts
  if (Array.isArray(result.systemPrompt)) {
    result.systemPrompt.forEach(hashPart);
  }

  // messages → parts
  if (Array.isArray(result.messages)) {
    for (const msg of result.messages) {
      if (Array.isArray(msg.parts)) {
        msg.parts.forEach(hashPart);
      }
    }
  }

  return { transformedInput: result, contentEntries: entries };
}

export function sha256(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}
