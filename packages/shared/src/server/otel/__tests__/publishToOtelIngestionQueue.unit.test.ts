import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * publishToOtelIngestionQueue always uses the all-split grouping path:
 * upload the exact serialized string, then idempotently register the file in
 * the project's dedicated lane.
 */

// vi.mock factories are hoisted above imports — everything they close over
// must be hoisted too.
const {
  uploadJsonString,
  queueAdd,
  registerOtelFile,
  addLaneToIndex,
  laneForIngestion,
  envMock,
} = vi.hoisted(() => ({
  uploadJsonString: vi.fn(async () => {}),
  queueAdd: vi.fn(async () => ({})),
  registerOtelFile: vi.fn(async () => true),
  addLaneToIndex: vi.fn(async () => {}),
  laneForIngestion: vi.fn(async () => "lane-p1"),
  // env is module-level-parsed; tests mutate this holder per case.
  envMock: {} as Record<string, unknown>,
}));

// NB: paths resolve relative to THIS test file — src/env.ts is three levels up.
vi.mock("../../../env", () => ({ env: envMock }));
vi.mock("../../s3", () => ({
  getS3EventStorageClient: vi.fn(() => ({ uploadJsonString })),
}));
vi.mock("../../redis/redis", () => ({
  redis: { fake: "redis-handle" },
  // Registration must go through the keyPrefix-free connection, never the
  // (potentially prefixed) singleton.
  getUnprefixedRedis: vi.fn(() => ({ fake: "unprefixed-redis-handle" })),
}));
vi.mock("../../redis/otelPendingGroups", () => ({
  registerOtelFile,
  addLaneToIndex,
}));
vi.mock("../../doris/tableRouting", () => ({ laneForIngestion }));
vi.mock("../../doris/tableSplitCache", () => ({
  isSplitCacheReady: vi.fn(() => true),
}));
vi.mock("../../redis/otelIngestionQueue", () => ({}));

import { OtelIngestionProcessor } from "../OtelIngestionProcessor";

const resourceSpans = [
  {
    resource: { attributes: [] },
    scopeSpans: [
      {
        scope: { name: "test" },
        spans: [
          { traceId: "dHJhY2U=", spanId: "c3Bhbg==", name: "s1" },
          { traceId: "dHJhY2U=", spanId: "c3BhbjI=", name: "s2" },
        ],
      },
    ],
  },
] as any;

const makeProcessor = () =>
  new OtelIngestionProcessor({
    projectId: "p1",
    publicKey: "pk-test",
    orgId: "org1",
    sdkName: "python",
    sdkVersion: "4.0.0",
  });

beforeEach(() => {
  vi.clearAllMocks();
  for (const k of Object.keys(envMock)) delete envMock[k];
  Object.assign(envMock, {
    LITEFUSE_S3_EVENT_UPLOAD_BUCKET: "test-bucket",
    LITEFUSE_S3_EVENT_UPLOAD_PREFIX: "events/",
    LITEFUSE_OTEL_REGISTERED_TTL_MS: 345_600_000,
  });
});

describe("publishToOtelIngestionQueue", () => {
  it("registers the project lane, no legacy queue job", async () => {
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);

    expect(uploadJsonString).toHaveBeenCalledTimes(1);
    expect(queueAdd).not.toHaveBeenCalled();
    expect(addLaneToIndex).toHaveBeenCalledWith(
      { fake: "unprefixed-redis-handle" },
      "lane-p1",
    );
    expect(registerOtelFile).toHaveBeenCalledWith(
      expect.objectContaining({ groupingKey: "lane-p1" }),
    );
  });

  it("uploads the SAME serialized string it measures, registers, no queue.add", async () => {
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);

    expect(queueAdd).not.toHaveBeenCalled();
    expect(registerOtelFile).toHaveBeenCalledTimes(1);

    const [{ groupingKey, ttlMs, entry }] = registerOtelFile.mock
      .calls[0] as any[];
    expect(groupingKey).toBe("lane-p1");
    expect(ttlMs).toBe(345_600_000);
    // Entry mirrors the request context…
    expect(entry).toMatchObject({
      v: 1,
      projectId: "p1",
      publicKey: "pk-test",
      orgId: "org1",
      sdkName: "python",
      sdkVersion: "4.0.0",
      spanCount: 2,
    });
    expect(entry.fileKey).toMatch(/^events\/otel\/p1\/.*\.json$/);
    // …and size is the byte length of EXACTLY the uploaded string.
    const [uploadedKey, uploadedBody] = uploadJsonString.mock.calls[0] as any[];
    expect(uploadedKey).toBe(entry.fileKey);
    expect(entry.size).toBe(Buffer.byteLength(uploadedBody, "utf8"));
    expect(uploadedBody).toBe(JSON.stringify(resourceSpans));
  });

  it("an already-registered file (idempotent 0) is still success", async () => {
    registerOtelFile.mockResolvedValueOnce(false);
    await expect(
      makeProcessor().publishToOtelIngestionQueue(resourceSpans),
    ).resolves.toBeUndefined();
  });

  it("registration failure propagates (route → 5xx → SDK re-send)", async () => {
    registerOtelFile.mockRejectedValueOnce(new Error("redis down"));
    await expect(
      makeProcessor().publishToOtelIngestionQueue(resourceSpans),
    ).rejects.toThrow("redis down");
  });

  it("upload precedes the pointer (durability first)", async () => {
    const order: string[] = [];
    uploadJsonString.mockImplementationOnce(async () => {
      order.push("upload");
    });
    registerOtelFile.mockImplementationOnce(async () => {
      order.push("register");
      return true;
    });
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);
    expect(order).toEqual(["upload", "register"]);

    const order2: string[] = [];
    uploadJsonString.mockImplementationOnce(async () => {
      order2.push("upload");
    });
    registerOtelFile.mockImplementationOnce(async () => {
      order2.push("register");
      return true;
    });
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);
    expect(order2).toEqual(["upload", "register"]);
  });
});
