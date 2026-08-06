import "./instrumentation"; // instrumenting the application
import type { Server } from "node:http";
import type { Express } from "express";
import { env } from "./env";
import {
  initializeSplitCache,
  logger,
  startSplitCacheRefresh,
} from "@langfuse/shared/src/server";

export let server: Server | undefined;

const startWorker = async (): Promise<void> => {
  // Do not import app (which registers queue consumers) before routing is ready.
  await initializeSplitCache();
  startSplitCacheRefresh();
  const appModule = await import("./app.js");
  const moduleDefault = appModule.default as unknown;
  const app = (
    (typeof moduleDefault === "object" ||
      typeof moduleDefault === "function") &&
    moduleDefault !== null &&
    "listen" in moduleDefault
      ? moduleDefault
      : (moduleDefault as { default: Express }).default
  ) as Express;
  server = app.listen(env.PORT, env.HOSTNAME, () => {
    logger.info(`Listening: http://${env.HOSTNAME}:${env.PORT}`);
  });
};

void startWorker().catch((error) => {
  logger.error("Worker startup failed", { error });
  process.exitCode = 1;
});
