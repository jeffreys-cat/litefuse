import { createHash } from "crypto";

export interface ContentEntry {
  content_hash: string;
  content: string;
}

/**
 * Process an array input: hash each object element, keep non-objects as-is.
 */
function processArray(input: any[]): {
  hashes: string[];
  entries: ContentEntry[];
} {
  const entries: ContentEntry[] = [];
  const hashes: string[] = [];

  for (const item of input) {
    if (item && typeof item === "object") {
      const serialized = JSON.stringify(item);
      const hash = sha256(serialized);
      entries.push({ content_hash: hash, content: serialized });
      hashes.push(hash);
    } else {
      hashes.push(item);
    }
  }

  return { hashes, entries };
}

/**
 * Replace top-level content elements with SHA-256 hash references.
 *
 * Input formats:
 *   - Array: [{"role": "...", "content": "..."}, ...] → [hash1, hash2, ...]
 *   - JSON string of array: "[{\"role\":...}]" → [hash1, hash2, ...] (Python SDK format)
 *   - Plain string → [hash]
 *   - Object → [hash]
 *   - null/undefined → null
 */
export function deduplicateInputContent(rawInput: any): {
  transformedInput: any;
  contentEntries: ContentEntry[];
} {
  if (rawInput == null) {
    return { transformedInput: rawInput, contentEntries: [] };
  }

  // String input: try JSON parse first (Python SDK serializes objects to JSON strings)
  if (typeof rawInput === "string") {
    try {
      const parsed = JSON.parse(rawInput);
      if (Array.isArray(parsed)) {
        const { hashes, entries } = processArray(parsed);
        return { transformedInput: hashes, contentEntries: entries };
      } else if (typeof parsed === "object" && parsed !== null) {
        const serialized = JSON.stringify(parsed);
        const hash = sha256(serialized);
        return {
          transformedInput: [hash],
          contentEntries: [{ content_hash: hash, content: serialized }],
        };
      }
    } catch {
      // Not valid JSON, treat as plain string
    }
    // Plain text string → hash as single entry
    const hash = sha256(rawInput);
    return {
      transformedInput: [hash],
      contentEntries: [{ content_hash: hash, content: rawInput }],
    };
  }

  // Array input → hash each element
  if (Array.isArray(rawInput)) {
    const { hashes, entries } = processArray(rawInput);
    return { transformedInput: hashes, contentEntries: entries };
  }

  // Object input → hash as single entry
  const serialized = JSON.stringify(rawInput);
  const hash = sha256(serialized);
  return {
    transformedInput: [hash],
    contentEntries: [{ content_hash: hash, content: serialized }],
  };
}

export function sha256(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}
