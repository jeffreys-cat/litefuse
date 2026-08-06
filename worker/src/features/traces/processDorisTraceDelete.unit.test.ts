import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  deleteEventsByTraceIds,
  deleteObservationsByTraceIds,
  deleteScoresByTraceIds,
  deleteTraces,
} from "@langfuse/shared/src/server";
import { processDorisTraceDelete } from "./processDorisTraceDelete";

vi.mock("@langfuse/shared/src/server", () => ({
  deleteEventsByTraceIds: vi.fn(),
  deleteObservationsByTraceIds: vi.fn(),
  deleteScoresByTraceIds: vi.fn(),
  deleteTraces: vi.fn(),
  getS3MediaStorageClient: vi.fn(),
  logger: {
    info: vi.fn(),
    error: vi.fn(),
  },
  removeIngestionEventsFromS3AndDeleteDorisRefsForTraces: vi.fn(),
  traceException: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    traceMedia: {
      findMany: vi.fn().mockResolvedValue([]),
      deleteMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    observationMedia: {
      findMany: vi.fn().mockResolvedValue([]),
      deleteMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
  },
}));

vi.mock("../../env", () => ({
  env: {
    LITEFUSE_ENABLE_BLOB_STORAGE_FILE_LOG: "false",
    LITEFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE: "false",
    LITEFUSE_S3_MEDIA_UPLOAD_BUCKET: undefined,
  },
}));

describe("processDorisTraceDelete", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("deletes unified trace data without issuing a legacy observation delete", async () => {
    const projectId = "project-id";
    const traceIds = ["trace-id"];

    await processDorisTraceDelete(projectId, traceIds);

    expect(deleteTraces).toHaveBeenCalledWith(projectId, traceIds);
    expect(deleteScoresByTraceIds).toHaveBeenCalledWith(projectId, traceIds);
    expect(deleteObservationsByTraceIds).not.toHaveBeenCalled();
    expect(deleteEventsByTraceIds).not.toHaveBeenCalled();
  });
});
