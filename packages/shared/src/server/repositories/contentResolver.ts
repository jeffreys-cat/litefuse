import { queryDoris } from "./doris";
import { logger } from "../logger";

/**
 * Resolve content_hash references in observation input fields back to actual content.
 * Batch-queries the content_dict table, then restores content in-place.
 *
 * Compatible with three formats:
 *   1. New top-level array: [{...}, {...}, {...}] — each element is either a hash or content
 *   2. Legacy structured: { systemPrompt: [...], messages: [...] }
 *   3. Inline content: { content: "..." } — left untouched
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

  // Handle hash array: ["hash1", "hash2"] → [{...content1...}, {...content2...}, ...]
  // This handles both:
  //   - New format: top-level array of hash strings
  //   - Legacy format: systemPrompt: ["hash"], messages: ["hash"]
  if (Array.isArray(obj)) {
    const restored = [];
    for (let i = 0; i < obj.length; i++) {
      const item = obj[i];
      if (typeof item === "string" && contentMap.has(item)) {
        // This is a hash string, restore from content_dict
        const contentStr = contentMap.get(item)!;
        try {
          const parsed = JSON.parse(contentStr);
          // Ensure message has an id for React key prop
          if (!parsed.id) {
            parsed.id = `restored-${i}-${item.substring(0, 8)}`;
          }
          restored.push(parsed);
        } catch {
          logger.warn("Failed to parse content for hash", {
            hash: item.substring(0, 16),
          });
          restored.push(item);
        }
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
      // Ensure the object has an id for React key prop
      if (!obj.id) {
        obj.id = `restored-${obj.content_hash?.substring(0, 8)}`;
      }
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
      // Build IN clause directly with quoted strings to avoid parameter processor issues
      const inClause = batch.map((h) => `'${h}'`).join(", ");

      const query = `
        SELECT content_hash, content
        FROM content_dict
        WHERE content_hash IN (${inClause})
      `;

      const rows = await queryDoris<{
        content_hash: string;
        content: string;
      }>({
        query,
        tags: {
          feature: "tracing",
          type: "content_dict",
          kind: "resolve",
        },
      });

      for (const row of rows) {
        if (row.content) {
          // Content can be string or object (already parsed JSON). Convert to string for storage.
          const contentStr =
            typeof row.content === "string"
              ? row.content
              : JSON.stringify(row.content);
          result.set(row.content_hash, contentStr);
        }
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
