import {
  createTRPCRouter,
  protectedOrganizationProcedure,
} from "@/src/server/api/trpc";
import { z } from "zod/v4";
import {
  assertCanManageBilling,
  createCheckoutSession,
  createPortalSession,
  getBillingStatus,
} from "./billingService";
import { billingIntervalSchema } from "./billingCatalogue";

const orgInput = z.object({
  orgId: z.string(),
});

export const billingRouter = createTRPCRouter({
  getBillingStatus: protectedOrganizationProcedure
    .input(orgInput)
    .query(async ({ input, ctx }) => {
      await assertCanManageBilling({
        session: ctx.session,
        orgId: input.orgId,
      });
      return getBillingStatus(input.orgId);
    }),
  createCheckoutSession: protectedOrganizationProcedure
    .input(
      orgInput.extend({
        interval: billingIntervalSchema,
      }),
    )
    .mutation(async ({ input, ctx }) => {
      await assertCanManageBilling({
        session: ctx.session,
        orgId: input.orgId,
      });
      return createCheckoutSession({
        orgId: input.orgId,
        userId: ctx.session.user.id,
        userEmail: ctx.session.user.email,
        interval: input.interval,
      });
    }),
  createPortalSession: protectedOrganizationProcedure
    .input(orgInput)
    .mutation(async ({ input, ctx }) => {
      await assertCanManageBilling({
        session: ctx.session,
        orgId: input.orgId,
      });
      return createPortalSession({ orgId: input.orgId });
    }),
});
