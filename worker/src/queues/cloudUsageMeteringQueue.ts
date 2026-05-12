import { Processor } from "bullmq";
import {
  CloudUsageMeteringQueue,
  enqueuePgBossJob,
  logger,
  QueueJobs,
  QueueName,
} from "@langfuse/shared/src/server";
import { handleCloudUsageMeteringJob } from "../ee/cloudUsageMetering/handleCloudUsageMeteringJob";
import { cloudUsageMeteringDbCronJobName } from "../ee/cloudUsageMetering/constants";
import { CloudUsageMeteringDbCronJobStates } from "../ee/cloudUsageMetering/constants";
import { prisma } from "@langfuse/shared/src/db";
import { env } from "../env";

export const cloudUsageMeteringQueueProcessor: Processor = async (job) => {
  if (job.name === QueueJobs.CloudUsageMeteringJob) {
    logger.info(
      "[CloudUsageMeteringQueue] Executing Cloud Usage Metering Job",
      {
        jobId: job.id,
        jobName: job.name,
        jobData: job.data,
        timestamp: new Date().toISOString(),
        opts: {
          repeat: job.opts.repeat,
          jobId: job.opts.jobId,
        },
      },
    );
    try {
      return await handleCloudUsageMeteringJob(job);
    } catch (error) {
      logger.error(
        "[CloudUsageMeteringQueue] Error executing Cloud Usage Metering Job",
        {
          jobId: job.id,
          error: error,
          timestamp: new Date().toISOString(),
        },
      );
      // adding another job to the queue to process again.
      await prisma.cronJobs.update({
        where: {
          name: cloudUsageMeteringDbCronJobName,
        },
        data: {
          state: CloudUsageMeteringDbCronJobStates.Queued,
          jobStartedAt: null,
        },
      });

      logger.info("Re-queuing Cloud Usage Metering Job after error", {
        timestamp: new Date().toISOString(),
      });
      if (env.LANGFUSE_PG_BOSS_ENABLED === "true") {
        await enqueuePgBossJob(
          QueueName.CloudUsageMeteringQueue,
          QueueJobs.CloudUsageMeteringJob,
          {},
        );
      } else {
        await CloudUsageMeteringQueue.getInstance()?.add(
          QueueJobs.CloudUsageMeteringJob,
          {},
        );
      }
      throw error;
    }
  }
};
