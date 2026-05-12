import { afterEach, describe, expect, it } from "vitest";
import {
  PG_BOSS_SCHEDULE_DEFINITIONS,
  QueueJobs,
  QueueName,
} from "@langfuse/shared/src/server";
import { env } from "../env";
import { getEnabledPgBossSchedules } from "../queues/pgBossScheduledJobs";

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
