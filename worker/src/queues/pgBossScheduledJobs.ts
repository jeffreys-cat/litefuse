import type { Job, Processor } from "bullmq";
import {
  enqueuePgBossJob,
  ensurePgBossSchedules,
  logger,
  PG_BOSS_SCHEDULE_DEFINITIONS,
  QueueJobs,
  QueueName,
  registerPgBossWorker,
  type PgBossScheduleDefinition,
} from "@langfuse/shared/src/server";
import { env } from "../env";

type ScheduledPgBossData = {
  id?: string;
  name?: QueueJobs;
  timestamp?: string;
  payload?: Record<string, unknown>;
};

type ScheduledPgBossJob = {
  id: string;
  name: string;
  data: ScheduledPgBossData;
};

const toBullJob = (job: ScheduledPgBossJob) =>
  ({
    id: job.id,
    name: job.data.name ?? job.name,
    data: {
      id: job.data.id ?? job.id,
      name: job.data.name ?? job.name,
      timestamp: job.data.timestamp ? new Date(job.data.timestamp) : new Date(),
      payload: job.data.payload ?? {},
    },
    opts: {
      repeat: undefined,
      jobId: job.data.id ?? job.id,
    },
    updateProgress: async (progress: number) => {
      logger.debug("pg-boss scheduled job progress", {
        queueName: job.name,
        jobId: job.id,
        progress,
      });
    },
  }) as unknown as Job;

const scheduleByQueueName = new Map<QueueName, PgBossScheduleDefinition>(
  PG_BOSS_SCHEDULE_DEFINITIONS.map((definition) => [
    definition.queueName,
    definition,
  ]),
);

export const CLOUD_USAGE_METERING_BOOTSTRAP_JOB_ID =
  "cloud-usage-metering-bootstrap";

const enqueueCloudUsageMeteringBootstrapJob = async (
  schedules: PgBossScheduleDefinition[],
): Promise<void> => {
  if (
    !schedules.some(
      (schedule) => schedule.queueName === QueueName.CloudUsageMeteringQueue,
    )
  ) {
    return;
  }

  await enqueuePgBossJob(
    QueueName.CloudUsageMeteringQueue,
    QueueJobs.CloudUsageMeteringJob,
    {},
    { id: CLOUD_USAGE_METERING_BOOTSTRAP_JOB_ID },
  );

  logger.info("pg-boss bootstrap job enqueued", {
    queueName: QueueName.CloudUsageMeteringQueue,
    jobName: QueueJobs.CloudUsageMeteringJob,
    jobId: CLOUD_USAGE_METERING_BOOTSTRAP_JOB_ID,
  });
};

export const getEnabledPgBossSchedules = (): PgBossScheduleDefinition[] => {
  if (env.LANGFUSE_PG_BOSS_ENABLED !== "true") {
    return [];
  }

  const schedules: PgBossScheduleDefinition[] = [];

  const add = (queueName: QueueName) => {
    const schedule = scheduleByQueueName.get(queueName);
    if (schedule) schedules.push(schedule);
  };

  if (
    env.QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED === "true" &&
    env.STRIPE_SECRET_KEY
  ) {
    add(QueueName.CloudUsageMeteringQueue);
  }

  if (
    env.QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED === "true" &&
    env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION &&
    env.STRIPE_SECRET_KEY
  ) {
    add(QueueName.CloudFreeTierUsageThresholdQueue);
  }

  if (env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED === "true") {
    add(QueueName.PostHogIntegrationQueue);
  }

  if (env.QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED === "true") {
    add(QueueName.MixpanelIntegrationQueue);
  }

  if (env.QUEUE_CONSUMER_BLOB_STORAGE_INTEGRATION_QUEUE_IS_ENABLED === "true") {
    add(QueueName.BlobStorageIntegrationQueue);
  }

  if (env.QUEUE_CONSUMER_DATA_RETENTION_QUEUE_IS_ENABLED === "true") {
    add(QueueName.DataRetentionQueue);
  }

  if (env.LANGFUSE_S3_CORE_DATA_EXPORT_IS_ENABLED === "true") {
    add(QueueName.CoreDataS3ExportQueue);
  }

  if (env.LANGFUSE_POSTGRES_METERING_DATA_EXPORT_IS_ENABLED === "true") {
    add(QueueName.MeteringDataPostgresExportQueue);
  }

  if (env.QUEUE_CONSUMER_DEAD_LETTER_RETRY_QUEUE_IS_ENABLED === "true") {
    add(QueueName.DeadLetterRetryQueue);
  }

  if (
    env.QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED === "true" &&
    env.LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE === "true"
  ) {
    add(QueueName.EventPropagationQueue);
  }

  return schedules;
};

export const registerPgBossScheduledProcessor = async (
  queueName: QueueName,
  processor: Processor,
  options: { localConcurrency?: number } = {},
): Promise<void> => {
  await registerPgBossWorker<ScheduledPgBossData>(
    queueName,
    {
      localConcurrency: options.localConcurrency ?? 1,
      pollingIntervalSeconds: 2,
    },
    async (jobs) => {
      for (const job of jobs) {
        await processor(toBullJob(job));
      }
    },
  );
};

export const startPgBossScheduledJobs = async (
  processors: Partial<Record<QueueName, Processor>>,
): Promise<void> => {
  const schedules = getEnabledPgBossSchedules();

  if (schedules.length === 0) {
    logger.info("No pg-boss schedules enabled");
    return;
  }

  await ensurePgBossSchedules(schedules);
  await enqueueCloudUsageMeteringBootstrapJob(schedules);

  for (const schedule of schedules) {
    const processor = processors[schedule.queueName];
    if (!processor) {
      logger.warn("No pg-boss scheduled processor registered", {
        queueName: schedule.queueName,
      });
      continue;
    }

    await registerPgBossScheduledProcessor(schedule.queueName, processor);
  }

  logger.info("pg-boss schedules started", {
    schedules: schedules.map((schedule) => ({
      queueName: schedule.queueName,
      cron: schedule.cron,
      key: schedule.key,
    })),
  });
};
