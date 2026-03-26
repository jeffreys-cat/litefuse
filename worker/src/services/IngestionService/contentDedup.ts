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

  // Structured input → replace arrays with hash arrays
  const entries: ContentEntry[] = [];
  const result = structuredClone(rawInput);

  // Hash each element in an array and replace with hash array
  function hashArrayElements(arr: any[]): string[] {
    const hashes: string[] = [];
    for (const item of arr) {
      if (item && typeof item === "object") {
        const serialized = JSON.stringify(item);
        const hash = sha256(serialized);
        entries.push({ content_hash: hash, content: serialized });
        hashes.push(hash);
      }
    }
    return hashes;
  }

  // Replace systemPrompt array with hash array
  if (Array.isArray(result.systemPrompt)) {
    result.systemPrompt = hashArrayElements(result.systemPrompt);
  }

  // Replace messages array with hash array
  if (Array.isArray(result.messages)) {
    result.messages = hashArrayElements(result.messages);
  }

  return { transformedInput: result, contentEntries: entries };
}

export function sha256(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}
