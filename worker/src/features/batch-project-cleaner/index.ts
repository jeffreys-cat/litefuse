import {
  getDeletedProjects,
  logger,
  recordIncrement,
  traceException,
} from "@langfuse/shared/src/server";
import { env } from "../../env";
import { PeriodicExclusiveRunner } from "../../utils/PeriodicExclusiveRunner";
import { cleanupDorisProjectData } from "../doris-project-cleanup";

export const BATCH_PROJECT_CLEANER_LOCK_PREFIX =
  "langfuse:batch-project-cleaner";
/** @deprecated Project cleanup is no longer instantiated per Doris table. */
export const BATCH_DELETION_TABLES = ["project"] as const;

/**
 * One project-level cleaner replaces the old per-table cleaners. This matters
 * for soft-deleted projects whose queue job was lost: split tables are dropped
 * before shared residue is swept, and retries are idempotent.
 */
export class BatchProjectCleaner extends PeriodicExclusiveRunner {
  protected get defaultIntervalMs(): number {
    return env.LITEFUSE_BATCH_PROJECT_CLEANER_SLEEP_ON_EMPTY_MS;
  }

  constructor(_legacyTableName?: string) {
    const lockTtlSeconds =
      Math.ceil(env.LITEFUSE_BATCH_PROJECT_CLEANER_DELETE_TIMEOUT_MS / 1000) +
      300;
    super({
      name: "BatchProjectCleaner",
      lockKey: BATCH_PROJECT_CLEANER_LOCK_PREFIX,
      lockTtlSeconds,
    });
  }

  public override start(): void {
    logger.info(`Starting ${this.instanceName}`, {
      checkIntervalMs: env.LITEFUSE_BATCH_PROJECT_CLEANER_CHECK_INTERVAL_MS,
      projectLimit: env.LITEFUSE_BATCH_PROJECT_CLEANER_PROJECT_LIMIT,
    });
    super.start();
  }

  protected override async execute(): Promise<number> {
    let deletedProjects: Array<{ id: string }>;
    try {
      deletedProjects = await getDeletedProjects(
        env.LITEFUSE_BATCH_PROJECT_CLEANER_PROJECT_LIMIT,
      );
    } catch (error) {
      logger.error(`${this.instanceName}: Failed to query deleted projects`, {
        error,
      });
      traceException(error);
      return env.LITEFUSE_BATCH_PROJECT_CLEANER_SLEEP_ON_EMPTY_MS;
    }

    if (deletedProjects.length === 0) {
      return env.LITEFUSE_BATCH_PROJECT_CLEANER_SLEEP_ON_EMPTY_MS;
    }

    return (
      (await this.withLock(
        async () => {
          for (const project of deletedProjects) {
            await cleanupDorisProjectData(project.id);
          }
          recordIncrement(
            "langfuse.batch_project_cleaner.projects_processed",
            deletedProjects.length,
          );
          return env.LITEFUSE_BATCH_PROJECT_CLEANER_CHECK_INTERVAL_MS;
        },
        (error) => {
          recordIncrement("langfuse.batch_project_cleaner.delete_failures");
          traceException(error);
          return env.LITEFUSE_BATCH_PROJECT_CLEANER_CHECK_INTERVAL_MS;
        },
      )) ?? env.LITEFUSE_BATCH_PROJECT_CLEANER_SLEEP_ON_EMPTY_MS
    );
  }
}
