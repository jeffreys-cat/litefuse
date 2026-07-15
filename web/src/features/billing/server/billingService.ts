import { env } from "@/src/env.mjs";
import { getOrganizationPlanServerSide } from "@/src/features/entitlements/server/getPlan";
import { ApiAuthService } from "@/src/features/public-api/server/apiAuth";
import { throwIfNoOrganizationAccess } from "@/src/features/rbac/utils/checkOrganizationAccess";
import {
  prisma,
  type Organization,
  type Prisma,
} from "@langfuse/shared/src/db";
import {
  CloudConfigSchema,
  type CloudConfigSchema as CloudConfig,
} from "@langfuse/shared";
import { logger, redis } from "@langfuse/shared/src/server";
import { TRPCError } from "@trpc/server";
import type { Session } from "next-auth";
import Stripe from "stripe";
import {
  getBillingCatalogue,
  getInvalidBillingCatalogueEntries,
  getProPriceById,
  getProPriceForInterval,
  isBillingCatalogueConfigured,
  type BillingInterval,
} from "./billingCatalogue";

type StripeConfigPurpose = "checkout" | "portal" | "webhook";

let stripeClient: Stripe | null = null;

export function getStripeClient(): Stripe {
  if (!env.STRIPE_SECRET_KEY) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: "Stripe is not configured.",
    });
  }

  stripeClient ??= new Stripe(env.STRIPE_SECRET_KEY);
  return stripeClient;
}

export function ensureStripeConfigured(purpose: StripeConfigPurpose): void {
  if (!env.STRIPE_SECRET_KEY) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: "Stripe secret key is not configured.",
    });
  }

  if (
    purpose === "checkout" &&
    !isBillingCatalogueConfigured() &&
    getInvalidBillingCatalogueEntries().length === 0
  ) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: "Stripe Pro price ids are not configured.",
    });
  }

  if (purpose === "webhook" && !env.STRIPE_WEBHOOK_SECRET) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: "Stripe webhook secret is not configured.",
    });
  }
}

export function parseCloudConfig(
  cloudConfig: Prisma.JsonValue | null,
): CloudConfig | null {
  if (!cloudConfig) return null;
  const parsed = CloudConfigSchema.safeParse(cloudConfig);
  return parsed.success ? parsed.data : null;
}

function jsonString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function stripeId(
  value: string | { id?: string } | null | undefined,
): string | null {
  if (!value) return null;
  if (typeof value === "string") return value;
  return value.id ?? null;
}

function timestampSecondsToDate(value: unknown): Date | null {
  return typeof value === "number" ? new Date(value * 1000) : null;
}

function patchOrCurrent<T>(
  patch: T | null | undefined,
  current: T | null | undefined,
): T | null {
  return patch !== undefined ? patch : (current ?? null);
}

function cloudConfigToJson(
  current: CloudConfig | null,
  stripePatch: {
    customerId?: string | null;
    activeSubscriptionId?: string | null;
    activeProductId?: string | null;
    subscriptionStatus?: string | null;
  },
  plan: "Pro" | "clear-pro" | "keep",
): Prisma.InputJsonObject {
  const next: Record<string, Prisma.InputJsonValue> = {};

  if (current?.monthlyObservationLimit !== undefined) {
    next.monthlyObservationLimit = current.monthlyObservationLimit;
  }
  if (current?.defaultLookBackDays !== undefined) {
    next.defaultLookBackDays = current.defaultLookBackDays;
  }
  if (current?.rateLimitOverrides !== undefined) {
    next.rateLimitOverrides =
      current.rateLimitOverrides as Prisma.InputJsonValue;
  }

  if (plan === "Pro") {
    next.plan = "Pro";
  } else if (plan === "keep" && current?.plan) {
    next.plan = current.plan;
  } else if (plan === "clear-pro" && current?.plan && current.plan !== "Pro") {
    next.plan = current.plan;
  }

  next.stripe = {
    customerId: patchOrCurrent(
      stripePatch.customerId,
      current?.stripe?.customerId,
    ),
    activeSubscriptionId: patchOrCurrent(
      stripePatch.activeSubscriptionId,
      current?.stripe?.activeSubscriptionId,
    ),
    activeProductId: patchOrCurrent(
      stripePatch.activeProductId,
      current?.stripe?.activeProductId,
    ),
    activeUsageProductId: current?.stripe?.activeUsageProductId ?? null,
    subscriptionStatus: patchOrCurrent(
      stripePatch.subscriptionStatus,
      current?.stripe?.subscriptionStatus,
    ),
  };

  return next as Prisma.InputJsonObject;
}

function hasManualNonProPlan(cloudConfig: CloudConfig | null): boolean {
  return Boolean(cloudConfig?.plan && cloudConfig.plan !== "Pro");
}

export async function getBillingStatus(orgId: string) {
  const org = await prisma.organization.findUniqueOrThrow({
    where: { id: orgId },
  });
  const cloudConfig = parseCloudConfig(org.cloudConfig);
  const plan = getOrganizationPlanServerSide(cloudConfig ?? undefined);
  const billingConfigurationIssues = getInvalidBillingCatalogueEntries().map(
    (entry) =>
      `${entry.envVar} must be a Stripe Price ID starting with price_. Current value starts with ${entry.value.slice(0, 5)}.`,
  );

  return {
    plan,
    isCloudBillingConfigured: Boolean(env.STRIPE_SECRET_KEY),
    isCheckoutConfigured: isBillingCatalogueConfigured(),
    billingConfigurationIssues,
    proPrices: getBillingCatalogue().map((entry) => ({
      interval: entry.interval,
      priceId: entry.priceId,
    })),
    stripe: {
      customerId: cloudConfig?.stripe?.customerId ?? null,
      activeSubscriptionId: cloudConfig?.stripe?.activeSubscriptionId ?? null,
      activeProductId: cloudConfig?.stripe?.activeProductId ?? null,
      subscriptionStatus: cloudConfig?.stripe?.subscriptionStatus ?? null,
    },
    billingCycle: {
      anchor: org.cloudBillingCycleAnchor,
      updatedAt: org.cloudBillingCycleUpdatedAt,
    },
  };
}

async function ensureStripeCustomer(params: {
  org: Organization;
  userEmail?: string | null;
}): Promise<string> {
  const cloudConfig = parseCloudConfig(params.org.cloudConfig);
  const existingCustomerId = cloudConfig?.stripe?.customerId;
  if (existingCustomerId) return existingCustomerId;

  const customer = await getStripeClient().customers.create({
    name: params.org.name,
    email: params.userEmail ?? undefined,
    metadata: {
      orgId: params.org.id,
    },
  });

  await prisma.organization.update({
    where: { id: params.org.id },
    data: {
      cloudConfig: cloudConfigToJson(
        cloudConfig,
        { customerId: customer.id },
        "keep",
      ),
    },
  });

  return customer.id;
}

export async function createCheckoutSession(params: {
  orgId: string;
  userId: string;
  userEmail?: string | null;
  interval: BillingInterval;
}) {
  ensureStripeConfigured("checkout");

  const invalidPrice = getInvalidBillingCatalogueEntries().find(
    (entry) => entry.interval === params.interval,
  );
  if (invalidPrice) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: `${invalidPrice.envVar} must be a Stripe Price ID starting with price_, not a Product ID starting with prod_.`,
    });
  }

  const price = getProPriceForInterval(params.interval);
  if (!price) {
    throw new TRPCError({
      code: "BAD_REQUEST",
      message: "Selected Pro price is not configured.",
    });
  }

  const org = await prisma.organization.findUniqueOrThrow({
    where: { id: params.orgId },
  });
  const cloudConfig = parseCloudConfig(org.cloudConfig);
  if (hasManualNonProPlan(cloudConfig)) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: "This organization has a manual plan override.",
    });
  }

  const customerId = await ensureStripeCustomer({
    org,
    userEmail: params.userEmail,
  });
  const baseUrl = env.NEXTAUTH_URL.replace(/\/$/, "");
  const billingUrl = `${baseUrl}/organization/${encodeURIComponent(params.orgId)}/settings/billing`;

  const session = await getStripeClient().checkout.sessions.create({
    mode: "subscription",
    customer: customerId,
    client_reference_id: params.orgId,
    line_items: [{ price: price.priceId, quantity: 1 }],
    metadata: {
      orgId: params.orgId,
      userId: params.userId,
      targetPlan: "Pro",
      interval: params.interval,
    },
    subscription_data: {
      metadata: {
        orgId: params.orgId,
        targetPlan: "Pro",
      },
    },
    success_url: `${billingUrl}?checkout=success`,
    cancel_url: `${billingUrl}?checkout=cancelled`,
  });

  if (!session.url) {
    throw new TRPCError({
      code: "INTERNAL_SERVER_ERROR",
      message: "Stripe did not return a checkout URL.",
    });
  }

  return { url: session.url };
}

export async function createPortalSession(params: { orgId: string }) {
  ensureStripeConfigured("portal");

  const org = await prisma.organization.findUniqueOrThrow({
    where: { id: params.orgId },
  });
  const customerId = parseCloudConfig(org.cloudConfig)?.stripe?.customerId;
  if (!customerId) {
    throw new TRPCError({
      code: "PRECONDITION_FAILED",
      message: "This organization does not have a Stripe customer.",
    });
  }

  const baseUrl = env.NEXTAUTH_URL.replace(/\/$/, "");
  const returnUrl = `${baseUrl}/organization/${encodeURIComponent(params.orgId)}/settings/billing`;
  const session = await getStripeClient().billingPortal.sessions.create({
    customer: customerId,
    return_url: returnUrl,
  });

  return { url: session.url };
}

export async function syncSubscriptionToOrganization(
  subscription: Stripe.Subscription,
  forceClear: boolean = false,
): Promise<{ orgId: string | null; planChanged: boolean }> {
  const customerId = stripeId(subscription.customer);
  const metadataOrgId = jsonString(subscription.metadata?.orgId);
  const org =
    metadataOrgId !== null
      ? await prisma.organization.findUnique({ where: { id: metadataOrgId } })
      : customerId
        ? await prisma.organization.findFirst({
            where: {
              cloudConfig: {
                path: ["stripe", "customerId"],
                equals: customerId,
              },
            },
          })
        : null;

  if (!org) {
    logger.warn("Stripe subscription webhook did not match an organization", {
      subscriptionId: subscription.id,
      customerId,
      metadataOrgId,
    });
    return { orgId: null, planChanged: false };
  }

  const cloudConfig = parseCloudConfig(org.cloudConfig);
  const matchingItem = subscription.items.data.find((item) =>
    Boolean(getProPriceById(item.price.id)),
  );
  const shouldHavePro =
    !forceClear &&
    Boolean(matchingItem) &&
    ["active", "trialing", "past_due"].includes(subscription.status);
  const activeProductId = matchingItem
    ? stripeId(matchingItem.price.product)
    : null;
  const previousPlan = cloudConfig?.plan ?? null;
  const nextPlan = shouldHavePro ? "Pro" : "clear-pro";

  const billingCycleAnchor =
    timestampSecondsToDate(
      (subscription as { current_period_start?: unknown }).current_period_start,
    ) ?? timestampSecondsToDate(matchingItem?.current_period_start);

  await prisma.organization.update({
    where: { id: org.id },
    data: {
      cloudConfig: cloudConfigToJson(
        cloudConfig,
        {
          customerId,
          activeSubscriptionId: shouldHavePro ? subscription.id : null,
          activeProductId: shouldHavePro ? activeProductId : null,
          subscriptionStatus: subscription.status,
        },
        nextPlan,
      ),
      cloudBillingCycleAnchor: billingCycleAnchor ?? undefined,
      cloudBillingCycleUpdatedAt: new Date(),
    },
  });

  await new ApiAuthService(prisma, redis).invalidateCachedOrgApiKeys(org.id);

  return {
    orgId: org.id,
    planChanged:
      (shouldHavePro && previousPlan !== "Pro") ||
      (!shouldHavePro && previousPlan === "Pro"),
  };
}

export async function assertCanManageBilling(params: {
  session: Session | null;
  orgId: string;
}) {
  throwIfNoOrganizationAccess({
    session: params.session,
    organizationId: params.orgId,
    scope: "langfuseCloudBilling:CRUD",
  });
}
