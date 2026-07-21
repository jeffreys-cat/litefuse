import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * Stage-2 behavior of publishToOtelIngestionQueue (exactly-once design §3.1):
 * grouping OFF → legacy one-job-per-file; grouping ON → S3 upload of the
 * exact serialized string, then idempotent registration (no queue.add).
 */

// vi.mock factories are hoisted above imports — everything they close over
// must be hoisted too.
const { uploadJsonString, queueAdd, registerOtelFile, envMock } = vi.hoisted(
  () => ({
    uploadJsonString: vi.fn(async () => {}),
    queueAdd: vi.fn(async () => ({})),
    registerOtelFile: vi.fn(async () => true),
    // env is module-level-parsed; tests mutate this holder per case.
    envMock: {} as Record<string, unknown>,
  }),
);

// NB: paths resolve relative to THIS test file — src/env.ts is three levels up.
vi.mock("../../../env", () => ({ env: envMock }));
vi.mock("../../s3", () => ({
  getS3EventStorageClient: vi.fn(() => ({ uploadJsonString })),
}));
vi.mock("../../redis/redis", () => ({ redis: { fake: "redis-handle" } }));
vi.mock("../../redis/otelPendingGroups", () => ({ registerOtelFile }));
vi.mock("../../redis/otelIngestionQueue", () => ({
  OtelIngestionQueue: {
    getInstance: vi.fn(() => ({ add: queueAdd })),
    getShardNames: vi.fn(() => ["otel-ingestion-queue"]),
  },
}));

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
    LITEFUSE_OTEL_GROUPING_ENABLED: "false",
    LITEFUSE_OTEL_REGISTERED_TTL_MS: 345_600_000,
  });
});

describe("publishToOtelIngestionQueue", () => {
  it("grouping OFF: uploads then enqueues one legacy job, no registration", async () => {
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);

    expect(uploadJsonString).toHaveBeenCalledTimes(1);
    expect(queueAdd).toHaveBeenCalledTimes(1);
    expect(registerOtelFile).not.toHaveBeenCalled();

    const [, job] = queueAdd.mock.calls[0] as any[];
    expect(job.payload.authCheck.scope.projectId).toBe("p1");
    expect(job.payload.data.fileKey).toMatch(/^events\/otel\/p1\//);
  });

  it("grouping ON: uploads the SAME serialized string it measures, registers, no queue.add", async () => {
    envMock.LITEFUSE_OTEL_GROUPING_ENABLED = "true";
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);

    expect(queueAdd).not.toHaveBeenCalled();
    expect(registerOtelFile).toHaveBeenCalledTimes(1);

    const [{ shard, ttlMs, entry }] = registerOtelFile.mock.calls[0] as any[];
    expect(shard).toBe("otel-ingestion-queue");
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

  it("grouping ON: an already-registered file (idempotent 0) is still success", async () => {
    envMock.LITEFUSE_OTEL_GROUPING_ENABLED = "true";
    registerOtelFile.mockResolvedValueOnce(false);
    await expect(
      makeProcessor().publishToOtelIngestionQueue(resourceSpans),
    ).resolves.toBeUndefined();
  });

  it("grouping ON: registration failure propagates (route → 5xx → SDK re-send)", async () => {
    envMock.LITEFUSE_OTEL_GROUPING_ENABLED = "true";
    registerOtelFile.mockRejectedValueOnce(new Error("redis down"));
    await expect(
      makeProcessor().publishToOtelIngestionQueue(resourceSpans),
    ).rejects.toThrow("redis down");
  });

  it("upload precedes the pointer in BOTH modes (durability first)", async () => {
    const order: string[] = [];
    uploadJsonString.mockImplementationOnce(async () => {
      order.push("upload");
    });
    queueAdd.mockImplementationOnce(async () => {
      order.push("enqueue");
      return {};
    });
    await makeProcessor().publishToOtelIngestionQueue(resourceSpans);
    expect(order).toEqual(["upload", "enqueue"]);

    envMock.LITEFUSE_OTEL_GROUPING_ENABLED = "true";
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
