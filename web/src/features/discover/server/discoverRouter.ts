import { z } from "zod/v4";
import {
  createTRPCRouter,
  protectedProjectProcedure,
} from "@/src/server/api/trpc";
import { dorisClient } from "@langfuse/shared/src/server";

function escapeIdentifier(value: string) {
  return value.replace(/`/g, "``");
}

export const discoverRouter = createTRPCRouter({
  databases: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
      }),
    )
    .query(async () => {
      const rows = await dorisClient({ database: "" }).query("SHOW DATABASES");
      return { rows };
    }),

  tables: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        database: z.string(),
      }),
    )
    .query(async ({ input }) => {
      const rows = await dorisClient({ database: input.database }).query(
        `SHOW TABLES FROM \`${escapeIdentifier(input.database)}\``,
      );
      return { rows };
    }),

  fields: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        database: z.string(),
        table: z.string(),
      }),
    )
    .query(async ({ input }) => {
      const rows = await dorisClient({ database: input.database }).query(
        `SHOW COLUMNS FROM \`${escapeIdentifier(input.database)}\`.\`${escapeIdentifier(input.table)}\``,
      );
      return { rows };
    }),

  indexes: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        database: z.string(),
        table: z.string(),
      }),
    )
    .query(async ({ input }) => {
      const rows = await dorisClient({ database: input.database }).query(
        `SHOW INDEXES FROM \`${escapeIdentifier(input.database)}\`.\`${escapeIdentifier(input.table)}\``,
      );
      return { rows };
    }),

  query: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        rawSql: z.string(),
        database: z.string().optional(),
      }),
    )
    .mutation(async ({ input }) => {
      const trimmed = input.rawSql.trim();
      const useMatch = trimmed.match(/^USE\s+`?([^`;]+)`?\s*;\s*/i);

      let database = input.database;
      let sql = trimmed;

      if (useMatch) {
        database = useMatch[1];
        sql = trimmed.slice(useMatch[0].length).trim();
      }

      const client = database ? dorisClient({ database }) : dorisClient();
      const rows = (await client.query(sql)) as Record<string, unknown>[];
      return { rows };
    }),
});
