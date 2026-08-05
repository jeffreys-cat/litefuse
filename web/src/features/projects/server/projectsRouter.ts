import {
  createTRPCRouter,
  protectedOrganizationProcedure,
  protectedProjectProcedure,
} from "@/src/server/api/trpc";
import * as z from "zod/v4";
import { throwIfNoProjectAccess } from "@/src/features/rbac/utils/checkProjectAccess";
import { TRPCError } from "@trpc/server";
import { projectNameSchema } from "@/src/features/auth/lib/projectNameSchema";
import { auditLog } from "@/src/features/audit-logs/auditLog";
import { throwIfNoOrganizationAccess } from "@/src/features/rbac/utils/checkOrganizationAccess";
import { ApiAuthService } from "@/src/features/public-api/server/apiAuth";
import {
  QueueJobs,
  redis,
  ProjectDeleteQueue,
  getEnvironmentsForProject,
  provisionSplitForNewProject,
  enqueueDorisSplitTableProvisioning,
  logger,
} from "@langfuse/shared/src/server";
import { randomUUID } from "crypto";
import { StringNoHTMLNonEmpty } from "@langfuse/shared";

export const projectsRouter = createTRPCRouter({
  create: protectedOrganizationProcedure
    .input(
      z.object({
        name: StringNoHTMLNonEmpty,
        orgId: z.string(),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      throwIfNoOrganizationAccess({
        session: ctx.session,
        organizationId: input.orgId,
        scope: "projects:create",
      });

      const existingProject = await ctx.prisma.project.findFirst({
        where: {
          name: input.name,
          orgId: input.orgId,
          deletedAt: null,
        },
      });

      if (existingProject) {
        throw new TRPCError({
          code: "CONFLICT",
          message:
            "A project with this name already exists in your organization",
        });
      }

      const project = await ctx.prisma.project.create({
        data: {
          name: input.name,
          orgId: input.orgId,
        },
      });

      // Universal Doris table split: EVERY new project gets its own tables
      // (billing-independent; retention TTL stays paid-differentiated, derived
      // at provisioning). The client gets the project id only after this
      // returns, so designating it BEFORE then guarantees its first rows can
      // never leak to the shared table (the write path resolves the designation
      // with a PG fallback). RELIABLE + compensating: if designation fails
      // (a rare PG blip on the control-row write), delete the just-created
      // project so the mutation fails cleanly instead of leaving an undesignated
      // project that could ingest to the shared table. The provisioning
      // enqueue/propagation inside upsert are best-effort.
      try {
        await provisionSplitForNewProject(project.id);
      } catch (e) {
        await ctx.prisma.project
          .delete({ where: { id: project.id } })
          .catch(() => undefined);
        throw e;
      }

      await auditLog({
        session: ctx.session,
        resourceType: "project",
        resourceId: project.id,
        action: "create",
        after: project,
      });

      return {
        id: project.id,
        name: project.name,
        role: "OWNER",
      };
    }),

  update: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        newName: projectNameSchema.shape.name,
      }),
    )
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "project:update",
      });

      // check if the project name is already taken by another project
      const otherProjectWithSameName = await ctx.prisma.project.findFirst({
        where: {
          name: input.newName,
          orgId: ctx.session.orgId,
          deletedAt: null,
          id: {
            not: input.projectId,
          },
        },
      });
      if (otherProjectWithSameName) {
        throw new TRPCError({
          code: "CONFLICT",
          message:
            "A project with this name already exists in your organization",
        });
      }

      const project = await ctx.prisma.project.update({
        where: {
          id: input.projectId,
          orgId: ctx.session.orgId,
        },
        data: {
          name: input.newName,
        },
      });
      await auditLog({
        session: ctx.session,
        resourceType: "project",
        resourceId: input.projectId,
        action: "update",
        after: project,
      });
      return true;
    }),

  setRetention: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        retention: z.number().int().gte(3).nullable(),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "project:update",
      });

      const project = await ctx.prisma.project.update({
        where: {
          id: input.projectId,
          orgId: ctx.session.orgId,
        },
        data: {
          retentionDays: input.retention,
        },
      });
      await auditLog({
        session: ctx.session,
        resourceType: "project",
        resourceId: input.projectId,
        action: "update",
        after: project,
      });

      // Retention is single-sourced on Project.retentionDays. If this project is
      // split, its dynamic_partition TTL must follow — re-enqueue provisioning,
      // which idempotently ALTERs the split tables' TTL to the new value (and
      // no-ops for a non-split project). Best-effort — never fail the setting.
      await enqueueDorisSplitTableProvisioning(input.projectId).catch((e) =>
        logger.error(
          `[table-split] TTL re-provision enqueue for ${input.projectId} failed`,
          e,
        ),
      );
      return true;
    }),

  delete: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: ctx.session.projectId,
        scope: "project:delete",
      });

      // API keys need to be deleted from cache. Otherwise, they will still be valid.
      await new ApiAuthService(
        ctx.prisma,
        redis,
      ).invalidateCachedProjectApiKeys(input.projectId);

      // Delete API keys from DB
      await ctx.prisma.apiKey.deleteMany({
        where: {
          projectId: input.projectId,
          scope: "PROJECT",
        },
      });

      const project = await ctx.prisma.project.update({
        where: {
          id: input.projectId,
          orgId: ctx.session.orgId,
        },
        data: {
          deletedAt: new Date(),
        },
      });

      await auditLog({
        session: ctx.session,
        resourceType: "project",
        resourceId: input.projectId,
        before: project,
        action: "delete",
      });

      const projectDeleteQueue = ProjectDeleteQueue.getInstance();
      if (!projectDeleteQueue) {
        throw new TRPCError({
          code: "INTERNAL_SERVER_ERROR",
          message:
            "ProjectDeleteQueue is not available. Please try again later.",
        });
      }

      await projectDeleteQueue.add(QueueJobs.ProjectDelete, {
        timestamp: new Date(),
        id: randomUUID(),
        payload: {
          projectId: input.projectId,
          orgId: ctx.session.orgId,
        },
        name: QueueJobs.ProjectDelete,
      });

      return true;
    }),

  transfer: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        targetOrgId: z.string(),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      // source org
      throwIfNoOrganizationAccess({
        session: ctx.session,
        organizationId: ctx.session.orgId,
        scope: "projects:transfer_org",
      });
      // destination org
      throwIfNoOrganizationAccess({
        session: ctx.session,
        organizationId: input.targetOrgId,
        scope: "projects:transfer_org",
      });

      const project = await ctx.prisma.project.findUnique({
        where: {
          id: input.projectId,
          deletedAt: null,
        },
      });
      if (!project) {
        throw new TRPCError({
          code: "NOT_FOUND",
          message: "Project not found",
        });
      }

      await auditLog({
        session: ctx.session,
        resourceType: "project",
        resourceId: input.projectId,
        action: "transfer",
        before: { orgId: ctx.session.orgId },
        after: { orgId: input.targetOrgId },
      });

      await ctx.prisma.$transaction([
        ctx.prisma.projectMembership.deleteMany({
          where: {
            projectId: input.projectId,
          },
        }),
        ctx.prisma.project.update({
          where: {
            id: input.projectId,
            orgId: ctx.session.orgId,
          },
          data: {
            orgId: input.targetOrgId,
          },
        }),
      ]);

      // API keys need to be deleted from cache. Otherwise, they will still be valid.
      // It has to be called after the db is done to prevent new API keys from being cached.
      await new ApiAuthService(
        ctx.prisma,
        redis,
      ).invalidateCachedProjectApiKeys(input.projectId);
    }),

  environmentFilterOptions: protectedProjectProcedure
    .input(
      z.object({ projectId: z.string(), fromTimestamp: z.date().optional() }),
    )
    .query(async ({ input }) => getEnvironmentsForProject(input)),
});
