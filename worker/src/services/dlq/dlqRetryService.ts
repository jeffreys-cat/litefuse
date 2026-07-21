import {
  logger,
  QueueName,
  recordHistogram,
} from "@langfuse/shared/src/server";
import { getQueue } from "@langfuse/shared/src/server";
import { redriveOtelFailedJobs } from "./otelDlqRedrive";

export class DlqRetryService {
  private static retryQueues = [
    QueueName.ProjectDelete,
    QueueName.TraceDelete,
    QueueName.ScoreDelete,
    QueueName.BatchActionQueue,
  ] as const;

  // called each 10 minutes, defined by the bull cron job
  public static async retryDeadLetterQueue() {
    logger.info(
      `Retrying dead letter queues for queues: ${DlqRetryService.retryQueues.join(
        ", ",
      )}`,
    );
    const retryQueues = DlqRetryService.retryQueues;
    for (const queueName of retryQueues) {
      const queue = getQueue(queueName);

      if (!queue) {
        logger.error(`Queue ${queueName} not found`);
        continue;
      }

      // Find failed jobs
      const failedJobs = await queue.getFailed();
      logger.info(
        `Found ${failedJobs.length} failed jobs in queue ${queueName}`,
      );
      for (const job of failedJobs) {
        try {
          const projectId = job.data.payload.projectId;
          const ts = job.data.timestamp;

          const dlxDelay = Date.now() - ts;

          recordHistogram("langfuse.dlq_retry_delay", dlxDelay, {
            unit: "milliseconds",
            projectId,
            queueName,
          });

          await job.retry();
          logger.info(
            `Retried job ${JSON.stringify(job)} in queue ${queueName}`,
          );
        } catch (error) {
          logger.error(
            `Failed to retry job ${JSON.stringify(job)} in queue ${queueName}:`,
            error,
          );
        }
      }
    }

    // Otel ingestion shards (exactly-once pipeline) use their own redrive:
    // remove+re-add (job.retry() would leave attempts exhausted — one bare
    // attempt per cycle), age guard against the FE label retention window,
    // and a PG poison ledger before any job leaves automatic recovery.
    try {
      await redriveOtelFailedJobs();
    } catch (error) {
      logger.error("Failed to redrive otel failed jobs:", error);
    }
  }
}
