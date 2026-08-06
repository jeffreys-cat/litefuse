import { createHash } from "crypto";
import { Job, Processor } from "bullmq";
import {
  dorisClient,
  logger,
  QueueName,
  redis,
  TQueueJobTypes,
  type OtelGroupIngestionEventType,
  traceException,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";
import { IngestionService } from "../services/IngestionService";
import { convertEventRecordToObservationForEval } from "@langfuse/shared";
import {
  createObservationEvalSchedulerDeps,
  fetchObservationEvalConfigs,
  scheduleObservationEvals,
} from "../features/evaluation/observationEval";
import {
  buildGroupJobDeps,
  buildTransformFile,
  processOtelGroupJob,
  wrapGroupJobError,
} from "./otelGroupJobProcessor";

const isGroupPayload = (
  payload: TQueueJobTypes[QueueName.OtelIngestionQueue]["payload"],
): payload is OtelGroupIngestionEventType => "shape" in payload;

/**
 * Old queues can contain a per-file payload from before the unconditional
 * per-project grouping pipeline. Convert it to a one-file group rather than
 * retaining the old direct writer, which targeted shared Doris tables.
 */
const toGroupPayload = (
  job: Job<TQueueJobTypes[QueueName.OtelIngestionQueue]>,
): OtelGroupIngestionEventType => {
  const { payload } = job.data;
  if (isGroupPayload(payload)) return payload;

  const { projectId } = payload.authCheck.scope;
  const { fileKey, publicKey } = payload.data;
  const groupId = createHash("sha1").update(fileKey).digest("hex");

  logger.warn(
    `Converting legacy OTel queue job ${job.id ?? job.data.id} to single-file group ${groupId}`,
  );

  return {
    shape: "group-v1",
    groupId,
    entries: [
      {
        v: 1,
        fileKey,
        size: 0,
        spanCount: 0,
        ts: job.data.timestamp.getTime(),
        projectId,
        publicKey,
        orgId: payload.authCheck.scope.orgId,
        sdkName: payload.sdkName,
        sdkVersion: payload.sdkVersion,
        ingestionVersion: payload.ingestionVersion,
        propagatedHeaders: payload.propagatedHeaders,
      },
    ],
  };
};

/**
 * Self-contained group job wiring. Every processed group writes only to the
 * project's physical events_full, traces_scalar, and content_dict tables.
 */
const processGroupShapedJob = async (
  payload: OtelGroupIngestionEventType,
): Promise<void> => {
  if (!redis) throw new Error("Redis not available");
  if (!prisma) throw new Error("Prisma not available");

  const ingestionService = new IngestionService(
    redis,
    prisma,
    null,
    dorisClient(),
  );

  const evalConfigCache = new Map<
    string,
    Awaited<ReturnType<typeof fetchObservationEvalConfigs>>
  >();
  const getEvalConfigs = async (projectId: string) => {
    if (!evalConfigCache.has(projectId)) {
      evalConfigCache.set(
        projectId,
        await fetchObservationEvalConfigs(projectId).catch((error) => {
          traceException(error);
          logger.warn(
            `Failed to fetch observation eval configs for project ${projectId}`,
            error,
          );
          return [];
        }),
      );
    }
    return evalConfigCache.get(projectId)!;
  };

  const deps = buildGroupJobDeps({
    transformFile: buildTransformFile({
      createEventRecord: (input, fileKey) =>
        ingestionService.createEventRecord(input as never, fileKey),
    }),
  });

  deps.scheduleEvals = async (files) => {
    let schedulerDeps: ReturnType<
      typeof createObservationEvalSchedulerDeps
    > | null = null;
    for (const file of files) {
      const configs = await getEvalConfigs(file.entry.projectId);
      if (configs.length === 0) continue;
      schedulerDeps = schedulerDeps ?? createObservationEvalSchedulerDeps();
      for (const record of file.eventRecords) {
        try {
          const observation = convertEventRecordToObservationForEval(record);
          await scheduleObservationEvals({
            observation,
            configs,
            schedulerDeps,
          });
        } catch (error) {
          traceException(error);
          logger.error(
            `Failed to schedule observation evals for project ${file.entry.projectId} and span ${record.span_id}`,
            error,
          );
        }
      }
    }
  };

  try {
    await processOtelGroupJob(payload, deps);
  } catch (error) {
    logger.error(
      `Failed otel group job ${payload.groupId} (${payload.entries.length} files)`,
      error,
    );
    throw wrapGroupJobError(payload.groupId, error);
  }
};

export const otelIngestionQueueProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.OtelIngestionQueue]>,
): Promise<void> => processGroupShapedJob(toGroupPayload(job));
