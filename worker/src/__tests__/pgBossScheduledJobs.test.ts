import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const sharedServerMocks = vi.hoisted(() => ({
  enqueuePgBossJob: vi.fn(),
  ensurePgBossSchedules: vi.fn(),
  registerPgBossWorker: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@langfuse/shared/src/server")>();
  return {
    ...actual,
    enqueuePgBossJob: sharedServerMocks.enqueuePgBossJob,
    ensurePgBossSchedules: sharedServerMocks.ensurePgBossSchedules,
    registerPgBossWorker: sharedServerMocks.registerPgBossWorker,
  };
});

import {
  PG_BOSS_SCHEDULE_DEFINITIONS,
  QueueJobs,
  QueueName,
} from "@langfuse/shared/src/server";
import { env } from "../env";
import {
  CLOUD_USAGE_METERING_BOOTSTRAP_JOB_ID,
  getEnabledPgBossSchedules,
  startPgBossScheduledJobs,
} from "../queues/pgBossScheduledJobs";

const schedule = (queueName: QueueName) =>
  PG_BOSS_SCHEDULE_DEFINITIONS.find(
    (definition) => definition.queueName === queueName,
  );

const originalEnv = {
  LANGFUSE_PG_BOSS_ENABLED: env.LANGFUSE_PG_BOSS_ENABLED,
  QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_BLOB_STORAGE_INTEGRATION_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_BLOB_STORAGE_INTEGRATION_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_DATA_RETENTION_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_DATA_RETENTION_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_DEAD_LETTER_RETRY_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_DEAD_LETTER_RETRY_QUEUE_IS_ENABLED,
  QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED:
    env.QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED,
  LANGFUSE_S3_CORE_DATA_EXPORT_IS_ENABLED:
    env.LANGFUSE_S3_CORE_DATA_EXPORT_IS_ENABLED,
  LANGFUSE_POSTGRES_METERING_DATA_EXPORT_IS_ENABLED:
    env.LANGFUSE_POSTGRES_METERING_DATA_EXPORT_IS_ENABLED,
  LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE:
    env.LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE,
  NEXT_PUBLIC_LANGFUSE_CLOUD_REGION: env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION,
  STRIPE_SECRET_KEY: env.STRIPE_SECRET_KEY,
};

const disableAllSchedules = () => {
  env.LANGFUSE_PG_BOSS_ENABLED = "true";
  env.QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_BLOB_STORAGE_INTEGRATION_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_DATA_RETENTION_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_DEAD_LETTER_RETRY_QUEUE_IS_ENABLED = "false";
  env.QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED = "false";
  env.LANGFUSE_S3_CORE_DATA_EXPORT_IS_ENABLED = "false";
  env.LANGFUSE_POSTGRES_METERING_DATA_EXPORT_IS_ENABLED = "false";
  env.LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE = "false";
  env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION = undefined;
  env.STRIPE_SECRET_KEY = undefined;
};

afterEach(() => {
  Object.assign(env, originalEnv);
});

beforeEach(() => {
  vi.clearAllMocks();
});

describe("PG_BOSS_SCHEDULE_DEFINITIONS", () => {
  it("maps migrated BullMQ repeatable jobs to pg-boss schedules", () => {
    expect(schedule(QueueName.CloudUsageMeteringQueue)).toMatchObject({
      jobName: QueueJobs.CloudUsageMeteringJob,
      cron: "5 * * * *",
      key: "cloud-usage-metering-recurring",
    });
    expect(schedule(QueueName.CloudFreeTierUsageThresholdQueue)).toMatchObject({
      jobName: QueueJobs.CloudFreeTierUsageThresholdJob,
      cron: "35 * * * *",
      data: { type: "recurring" },
    });
    expect(schedule(QueueName.EventPropagationQueue)).toMatchObject({
      jobName: QueueJobs.EventPropagationJob,
      cron: "* * * * *",
    });
    expect(schedule(QueueName.DeadLetterRetryQueue)).toMatchObject({
      jobName: QueueJobs.DeadLetterRetryJob,
      cron: "*/10 * * * *",
    });
  });
});

describe("getEnabledPgBossSchedules", () => {
  it("disables pg-boss schedules when the pg-boss backend is disabled", () => {
    disableAllSchedules();

    env.LANGFUSE_PG_BOSS_ENABLED = "false";
    env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED = "true";

    expect(getEnabledPgBossSchedules()).toEqual([]);
  });

  it("honors worker env gating for schedules", () => {
    disableAllSchedules();
    expect(getEnabledPgBossSchedules()).toEqual([]);

    env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED = "true";
    expect(getEnabledPgBossSchedules().map((item) => item.queueName)).toEqual([
      QueueName.PostHogIntegrationQueue,
    ]);
  });

  it("requires cloud-only dependencies for cloud schedules", () => {
    disableAllSchedules();

    env.QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED = "true";
    env.QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED = "true";
    expect(getEnabledPgBossSchedules()).toEqual([]);

    env.STRIPE_SECRET_KEY = "sk_test";
    env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION = "DEV";
    expect(getEnabledPgBossSchedules().map((item) => item.queueName)).toEqual([
      QueueName.CloudUsageMeteringQueue,
      QueueName.CloudFreeTierUsageThresholdQueue,
    ]);
  });
});

describe("startPgBossScheduledJobs", () => {
  it("enqueues a cloud usage metering bootstrap job when its pg-boss schedule is enabled", async () => {
    disableAllSchedules();
    env.QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED = "true";
    env.STRIPE_SECRET_KEY = "sk_test";

    await startPgBossScheduledJobs({
      [QueueName.CloudUsageMeteringQueue]: vi.fn(),
    });

    expect(sharedServerMocks.ensurePgBossSchedules).toHaveBeenCalledWith([
      expect.objectContaining({
        queueName: QueueName.CloudUsageMeteringQueue,
      }),
    ]);
    expect(sharedServerMocks.enqueuePgBossJob).toHaveBeenCalledWith(
      QueueName.CloudUsageMeteringQueue,
      QueueJobs.CloudUsageMeteringJob,
      {},
      { id: CLOUD_USAGE_METERING_BOOTSTRAP_JOB_ID },
    );
  });

  it("does not enqueue a cloud usage metering bootstrap job when its schedule is not enabled", async () => {
    disableAllSchedules();
    env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED = "true";

    await startPgBossScheduledJobs({
      [QueueName.PostHogIntegrationQueue]: vi.fn(),
    });

    expect(sharedServerMocks.ensurePgBossSchedules).toHaveBeenCalledWith([
      expect.objectContaining({
        queueName: QueueName.PostHogIntegrationQueue,
      }),
    ]);
    expect(sharedServerMocks.enqueuePgBossJob).not.toHaveBeenCalled();
  });
});
