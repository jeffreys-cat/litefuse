import { PgBoss } from "pg-boss";
import type {
  QueuePolicy,
  QueueResult,
  Schedule,
  ScheduleOptions,
  SendOptions,
  WorkHandler,
  WorkOptions,
} from "pg-boss";
import { env } from "../../env";
import { logger } from "../logger";
import { QueueJobs, QueueName } from "../queues";

export type PgBossScheduleDefinition = {
  queueName: QueueName;
  jobName: QueueJobs;
  cron: string;
  key: string;
  data?: Record<string, unknown>;
  queueOptions: {
    policy?: QueuePolicy;
    retryLimit: number;
    retryDelay: number;
    retryBackoff: boolean;
    deleteAfterSeconds: number;
    expireInSeconds?: number;
  };
  scheduleOptions?: Omit<ScheduleOptions, "key" | "tz">;
};

export const PG_BOSS_SCHEDULE_DEFINITIONS = [
  {
    queueName: QueueName.CloudUsageMeteringQueue,
    jobName: QueueJobs.CloudUsageMeteringJob,
    cron: "5 * * * *",
    key: "cloud-usage-metering-recurring",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 30 * 60,
    },
  },
  {
    queueName: QueueName.CloudFreeTierUsageThresholdQueue,
    jobName: QueueJobs.CloudFreeTierUsageThresholdJob,
    cron: "35 * * * *",
    key: "free-tier-usage-threshold-hourly",
    data: { type: "recurring" },
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 30 * 60,
    },
  },
  {
    queueName: QueueName.PostHogIntegrationQueue,
    jobName: QueueJobs.PostHogIntegrationJob,
    cron: "30 * * * *",
    key: "posthog-integration-hourly",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 30 * 60,
    },
  },
  {
    queueName: QueueName.MixpanelIntegrationQueue,
    jobName: QueueJobs.MixpanelIntegrationJob,
    cron: "30 * * * *",
    key: "mixpanel-integration-hourly",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 30 * 60,
    },
  },
  {
    queueName: QueueName.BlobStorageIntegrationQueue,
    jobName: QueueJobs.BlobStorageIntegrationJob,
    cron: "20 * * * *",
    key: "blob-storage-integration-hourly",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 30 * 60,
    },
  },
  {
    queueName: QueueName.DataRetentionQueue,
    jobName: QueueJobs.DataRetentionJob,
    cron: "15 3 * * *",
    key: "data-retention-daily",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 60 * 60,
    },
  },
  {
    queueName: QueueName.CoreDataS3ExportQueue,
    jobName: QueueJobs.CoreDataS3ExportJob,
    cron: "15 3 * * *",
    key: "core-data-s3-export-daily",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 2 * 60 * 60,
    },
  },
  {
    queueName: QueueName.MeteringDataPostgresExportQueue,
    jobName: QueueJobs.MeteringDataPostgresExportJob,
    cron: "30 2 * * *",
    key: "metering-data-postgres-export-daily",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 2 * 60 * 60,
    },
  },
  {
    queueName: QueueName.EventPropagationQueue,
    jobName: QueueJobs.EventPropagationJob,
    cron: "* * * * *",
    key: "event-propagation-minutely",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 2,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 10 * 60,
    },
  },
  {
    queueName: QueueName.DeadLetterRetryQueue,
    jobName: QueueJobs.DeadLetterRetryJob,
    cron: "*/10 * * * *",
    key: "dead-letter-retry-10-minutely",
    data: {},
    queueOptions: {
      policy: "singleton",
      retryLimit: 4,
      retryDelay: 5,
      retryBackoff: true,
      deleteAfterSeconds: 24 * 60 * 60,
      expireInSeconds: 10 * 60,
    },
  },
] satisfies PgBossScheduleDefinition[];

declare global {
  var pgBossGlobal: PgBoss | undefined;
}

const getDatabaseUrl = () => {
  // eslint-disable-next-line turbo/no-undeclared-env-vars
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required to initialize pg-boss");
  }
  return databaseUrl;
};

const createPgBoss = () => {
  const boss = new PgBoss({
    connectionString: getDatabaseUrl(),
    schema: env.LANGFUSE_PG_BOSS_SCHEMA,
    migrate: env.LANGFUSE_PG_BOSS_MIGRATE === "true",
    createSchema: env.LANGFUSE_PG_BOSS_MIGRATE === "true",
    schedule: true,
    max: env.LANGFUSE_PG_BOSS_POOL_MAX,
    connectionTimeoutMillis: env.LANGFUSE_PG_BOSS_CONNECTION_TIMEOUT_MS,
    application_name: "litefuse-pg-boss",
  });

  boss.on("error", (error) => {
    logger.error("pg-boss error", error);
  });
  boss.on("warning", (warning) => {
    logger.warn("pg-boss warning", warning);
  });

  return boss;
};

let startPromise: Promise<PgBoss> | null = null;

export const getPgBoss = (): PgBoss => {
  if (env.LANGFUSE_PG_BOSS_ENABLED !== "true") {
    throw new Error("pg-boss is disabled via LANGFUSE_PG_BOSS_ENABLED");
  }

  if (!globalThis.pgBossGlobal) {
    globalThis.pgBossGlobal = createPgBoss();
  }

  return globalThis.pgBossGlobal;
};

export const startPgBoss = async (): Promise<PgBoss> => {
  const boss = getPgBoss();
  if (!startPromise) {
    startPromise = boss.start();
  }
  return startPromise;
};

export const stopPgBoss = async (): Promise<void> => {
  if (!globalThis.pgBossGlobal) return;

  await globalThis.pgBossGlobal.stop({ graceful: true, timeout: 30_000 });
  globalThis.pgBossGlobal = undefined;
  startPromise = null;
};

export const ensurePgBossSchedule = async (
  definition: PgBossScheduleDefinition,
): Promise<void> => {
  const boss = await startPgBoss();

  await boss.createQueue(definition.queueName, definition.queueOptions);
  await boss.schedule(
    definition.queueName,
    definition.cron,
    {
      id: definition.key,
      name: definition.jobName,
      timestamp: new Date().toISOString(),
      payload: definition.data ?? {},
    },
    {
      ...definition.queueOptions,
      ...definition.scheduleOptions,
      key: definition.key,
      tz: env.LANGFUSE_PG_BOSS_SCHEDULE_TZ,
    },
  );
};

export const ensurePgBossSchedules = async (
  definitions: readonly PgBossScheduleDefinition[],
): Promise<void> => {
  await Promise.all(
    definitions.map((definition) => ensurePgBossSchedule(definition)),
  );
};

export const registerPgBossWorker = async <ReqData extends object>(
  queueName: QueueName,
  options: WorkOptions,
  handler: WorkHandler<ReqData>,
): Promise<string> => {
  const boss = await startPgBoss();
  return boss.work<ReqData>(queueName, options, handler);
};

export const getPgBossAdminSnapshot = async (
  queueNames: QueueName[] = PG_BOSS_SCHEDULE_DEFINITIONS.map(
    (definition) => definition.queueName,
  ),
): Promise<{
  queues: (QueueResult | null)[];
  schedules: Schedule[];
}> => {
  const boss = await startPgBoss();
  const [queues, schedules] = await Promise.all([
    Promise.all(queueNames.map((queueName) => boss.getQueue(queueName))),
    boss.getSchedules(),
  ]);

  return { queues, schedules };
};

export const retryPgBossJob = async (queueName: string, jobIds: string[]) => {
  const boss = await startPgBoss();
  return boss.retry(queueName, jobIds);
};

export const cancelPgBossJob = async (queueName: string, jobIds: string[]) => {
  const boss = await startPgBoss();
  return boss.cancel(queueName, jobIds);
};

export const deletePgBossJob = async (queueName: string, jobIds: string[]) => {
  const boss = await startPgBoss();
  return boss.deleteJob(queueName, jobIds);
};

export const unschedulePgBossJob = async (queueName: string, key: string) => {
  const boss = await startPgBoss();
  return boss.unschedule(queueName, key);
};

export const enqueuePgBossJob = async (
  queueName: QueueName,
  jobName: QueueJobs,
  data: Record<string, unknown> = {},
  options: SendOptions = {},
): Promise<string | null> => {
  const boss = await startPgBoss();
  return boss.send(
    queueName,
    {
      id: options.id ?? jobName,
      name: jobName,
      timestamp: new Date().toISOString(),
      payload: data,
    },
    options,
  );
};
