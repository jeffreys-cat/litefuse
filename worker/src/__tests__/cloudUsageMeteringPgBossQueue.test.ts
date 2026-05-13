import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const redisAddMock = vi.hoisted(() => vi.fn());
const sharedServerMocks = vi.hoisted(() => ({
  enqueuePgBossJob: vi.fn(),
  CloudUsageMeteringQueue: {
    getInstance: vi.fn(() => ({
      add: redisAddMock,
    })),
  },
}));

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@langfuse/shared/src/server")>();
  return {
    ...actual,
    CloudUsageMeteringQueue: sharedServerMocks.CloudUsageMeteringQueue,
    enqueuePgBossJob: sharedServerMocks.enqueuePgBossJob,
  };
});

import { QueueJobs, QueueName } from "@langfuse/shared/src/server";
import { env } from "../env";
import { enqueueCloudUsageMeteringJob } from "../ee/cloudUsageMetering/handleCloudUsageMeteringJob";

const originalEnv = {
  LANGFUSE_PG_BOSS_ENABLED: env.LANGFUSE_PG_BOSS_ENABLED,
};

afterEach(() => {
  Object.assign(env, originalEnv);
});

beforeEach(() => {
  vi.clearAllMocks();
});

describe("enqueueCloudUsageMeteringJob", () => {
  it("uses pg-boss when pg-boss scheduling is enabled", async () => {
    env.LANGFUSE_PG_BOSS_ENABLED = "true";

    await enqueueCloudUsageMeteringJob();

    expect(sharedServerMocks.enqueuePgBossJob).toHaveBeenCalledWith(
      QueueName.CloudUsageMeteringQueue,
      QueueJobs.CloudUsageMeteringJob,
      {},
    );
    expect(
      sharedServerMocks.CloudUsageMeteringQueue.getInstance,
    ).not.toHaveBeenCalled();
    expect(redisAddMock).not.toHaveBeenCalled();
  });

  it("uses the Redis BullMQ queue when pg-boss scheduling is disabled", async () => {
    env.LANGFUSE_PG_BOSS_ENABLED = "false";

    await enqueueCloudUsageMeteringJob();

    expect(sharedServerMocks.enqueuePgBossJob).not.toHaveBeenCalled();
    expect(
      sharedServerMocks.CloudUsageMeteringQueue.getInstance,
    ).toHaveBeenCalled();
    expect(redisAddMock).toHaveBeenCalledWith(
      QueueJobs.CloudUsageMeteringJob,
      {},
    );
  });
});
