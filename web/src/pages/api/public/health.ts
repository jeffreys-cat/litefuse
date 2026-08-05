import { VERSION } from "@/src/constants";
import { cors, runMiddleware } from "@/src/features/public-api/server/cors";
import { telemetry } from "@/src/features/telemetry";
import { prisma } from "@langfuse/shared/src/db";
import { logger, traceException } from "@langfuse/shared/src/server";
import { type NextApiRequest, type NextApiResponse } from "next";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    await runMiddleware(req, res, cors);
    await telemetry();
    const failIfNoRecentEvents = req.query.failIfNoRecentEvents === "true";
    const failIfDatabaseUnavailable =
      req.query.failIfDatabaseUnavailable === "true";

    try {
      if (failIfDatabaseUnavailable) {
        await prisma.$queryRaw`SELECT 1;`;
      }
    } catch (e) {
      logger.error("Couldn't connect to database", e);
      traceException(e);
      return res.status(503).json({
        status: "Database not available",
        version: VERSION.replace("v", ""),
      });
    }

    try {
      if (failIfNoRecentEvents) {
        // Liveness of the ingestion pipeline, read from the PG completion
        // ledger (otel_file_ledger) — NOT a cross-project Doris scan. Under
        // table split a project's data lives in its own events_full_<pid>, so a
        // scan of the shared events_full would false-negative once active
        // projects are split. The ledger records every completed group across
        // ALL projects with created_at, so "most recent row within 3 minutes"
        // is a cross-project-safe liveness signal that touches no Doris table.
        const cutoff = new Date(Date.now() - 3 * 60_000);
        const recent = await prisma.otelFileLedger.findFirst({
          where: { createdAt: { gte: cutoff } },
          select: { id: true },
        });
        if (!recent) {
          return res.status(503).json({
            status: "No otel ingestion completed within the last 3 minutes",
            version: VERSION.replace("v", ""),
          });
        }
      }
    } catch (e) {
      logger.error("Couldn't check recent ingestion", e);
      traceException(e);
      return res.status(503).json({
        status: "Couldn't check recent ingestion",
        version: VERSION.replace("v", ""),
      });
    }
  } catch (e) {
    traceException(e);
    logger.error("Health check failed", e);
    return res.status(503).json({
      status: "Health check failed",
      version: VERSION.replace("v", ""),
    });
  }
  return res.status(200).json({
    status: "OK",
    version: VERSION.replace("v", ""),
  });
}
