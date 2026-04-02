import { createHash } from "crypto";

export interface ContentEntry {
  content_hash: string;
  content: string;
}

/**
 * Replace top-level content elements with SHA-256 hash references.
 * New format: {{...}, {...}, {...}} → directly hash each element at top level.
 * Old format: { systemPrompt: [...], messages: [...] } → still supported via fallback.
 */
export function deduplicateInputContent(rawInput: any): {
  transformedInput: any;
  contentEntries: ContentEntry[];
} {
  // 在 contentDedup.ts 的 deduplicateInputContent 函数开头加
  console.log(
    "[contentDedup] rawInput type:",
    typeof rawInput,
    Array.isArray(rawInput),
    "rawInput:",
    JSON.stringify(rawInput)?.substring(0, 200),
  );

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

  // New format: top-level array → hash each element
  // e.g., [{{...}, {...}, {...}] → [{...hash1...}, {...hash2...}, ...]
  if (Array.isArray(rawInput)) {
    const entries: ContentEntry[] = [];
    const hashes: string[] = [];

    for (const item of rawInput) {
      if (item && typeof item === "object") {
        const serialized = JSON.stringify(item);
        const hash = sha256(serialized);
        entries.push({ content_hash: hash, content: serialized });
        hashes.push(hash);
      } else {
        // Non-object elements (strings, numbers, etc.) are kept as-is
        hashes.push(item);
      }
    }

    return {
      transformedInput: hashes,
      contentEntries: entries,
    };
  }

  // Legacy structured format: { systemPrompt: [...], messages: [...] }
  if (rawInput.systemPrompt || rawInput.messages) {
    const entries: ContentEntry[] = [];
    const result = structuredClone(rawInput);

    // Hash each element in an array
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

    if (Array.isArray(result.systemPrompt)) {
      result.systemPrompt = hashArrayElements(result.systemPrompt);
    }

    if (Array.isArray(result.messages)) {
      result.messages = hashArrayElements(result.messages);
    }

    return { transformedInput: result, contentEntries: entries };
  }

  // Non-structured object → serialize and hash as one entry
  const serialized = JSON.stringify(rawInput);
  const hash = sha256(serialized);
  return {
    transformedInput: { content_hash: hash },
    contentEntries: [{ content_hash: hash, content: serialized }],
  };
}

export function sha256(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}
