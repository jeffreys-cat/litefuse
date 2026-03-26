import { queryDoris } from "./doris";
import { logger } from "../logger";

/**
 * Resolve content_hash references in observation input fields back to actual content.
 * Batch-queries the content_dict table, then restores content in-place.
 *
 * Compatible with both old format (inline content) and new format (content_hash references).
 * Old format records are left untouched.
 */
export async function resolveContentReferences(
  observations: Array<{ input?: string | null; [key: string]: any }>,
): Promise<void> {
  // 1. Collect all content_hash values from parsed inputs
  const allHashes = new Set<string>();
  const parsedInputs = new Map<number, any>();

  observations.forEach((obs, idx) => {
    if (!obs.input) return;
    try {
      const parsed =
        typeof obs.input === "string" ? JSON.parse(obs.input) : obs.input;
      parsedInputs.set(idx, parsed);
      collectHashes(parsed, allHashes);
    } catch {
      // Non-JSON input, skip
    }
  });

  if (allHashes.size === 0) return;

  // 2. Batch query content_dict
  const contentMap = await queryContentDict([...allHashes]);

  // 3. Replace hash → content in-place
  for (const [idx, parsed] of parsedInputs) {
    restoreContent(parsed, contentMap);
    observations[idx].input = JSON.stringify(parsed);
  }
}

function collectHashes(obj: any, hashes: Set<string>): void {
  if (!obj || typeof obj !== "object") return;

  // Handle hash array: ["hash1", "hash2"]
  if (Array.isArray(obj)) {
    for (const item of obj) {
      if (typeof item === "string" && isValidHash(item)) {
        hashes.add(item);
      } else {
        collectHashes(item, hashes);
      }
    }
    return;
  }

  // Handle object with content_hash field: { content_hash: "xxx" }
  if (obj.content_hash && typeof obj.content_hash === "string") {
    hashes.add(obj.content_hash);
    return;
  }

  // Recurse into object values
  Object.values(obj).forEach((val) => collectHashes(val, hashes));
}

// Check if string looks like a SHA-256 hash (64 hex chars)
function isValidHash(str: string): boolean {
  return /^[a-f0-9]{64}$/i.test(str);
}

function restoreContent(obj: any, contentMap: Map<string, string>): void {
  if (!obj || typeof obj !== "object") return;

  // Handle hash array: ["hash1", "hash2"] → [{...original element...}, ...]
  if (Array.isArray(obj)) {
    const restored = [];
    for (const item of obj) {
      if (typeof item === "string" && contentMap.has(item)) {
        // This is a hash string, restore from content_dict
        restored.push(JSON.parse(contentMap.get(item)!));
      } else {
        // Regular item, recursively restore if needed
        if (item && typeof item === "object") {
          restoreContent(item, contentMap);
        }
        restored.push(item);
      }
    }
    // Replace the array in place
    obj.length = 0;
    obj.push(...restored);
    return;
  }

  // Handle object with content_hash field: { content_hash: "xxx" } → { content: "..." }
  if (obj.content_hash && typeof obj.content_hash === "string") {
    if (contentMap.has(obj.content_hash)) {
      obj.content = contentMap.get(obj.content_hash);
      delete obj.content_hash;
    }
    return;
  }

  // Recurse into object values
  Object.values(obj).forEach((val) => restoreContent(val, contentMap));
}

async function queryContentDict(
  hashes: string[],
): Promise<Map<string, string>> {
  const result = new Map<string, string>();
  if (hashes.length === 0) return result;

  // Query in batches to avoid overly long IN clauses
  const BATCH_SIZE = 500;
  for (let i = 0; i < hashes.length; i += BATCH_SIZE) {
    const batch = hashes.slice(i, i + BATCH_SIZE);
    try {
      const rows = await queryDoris<{
        content_hash: string;
        content: string;
      }>({
        query: `
          SELECT content_hash, content
          FROM content_dict
          WHERE content_hash IN ({hashes: Array(String)})
        `,
        params: { hashes: batch },
        tags: {
          feature: "tracing",
          type: "content_dict",
          kind: "resolve",
        },
      });
      for (const row of rows) {
        result.set(row.content_hash, row.content);
      }
    } catch (error) {
      logger.error("Failed to query content_dict", {
        error: error instanceof Error ? error.message : String(error),
        hashCount: batch.length,
      });
      // Don't throw — gracefully degrade by leaving content_hash in place
    }
  }

  return result;
}
