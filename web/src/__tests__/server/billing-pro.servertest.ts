/** @jest-environment node */

import {
  getBillingCatalogue,
  getInvalidBillingCatalogueEntries,
} from "@/src/features/billing/server/billingCatalogue";
import {
  parseCloudConfig,
  syncSubscriptionToOrganization,
} from "@/src/features/billing/server/billingService";
import { handleStripeWebhook } from "@/src/features/billing/server/stripeWebhookHandler";
import { getOrganizationPlanServerSide } from "@/src/features/entitlements/server/getPlan";
import { env } from "@/src/env.mjs";
import { appRouter } from "@/src/server/api/root";
import { createInnerTRPCContext } from "@/src/server/api/trpc";
import { prisma, Role } from "@langfuse/shared/src/db";
import type { Plan } from "@langfuse/shared";
import type { Session } from "next-auth";
import Stripe from "stripe";
import { v4 as uuidv4 } from "uuid";

const STRIPE_SECRET_KEY = "sk_test_litefuse";
const STRIPE_WEBHOOK_SECRET = "whsec_litefuse";
const STRIPE_PRO_MONTHLY_PRICE_ID = "price_litefuse_pro_monthly";
const STRIPE_PRO_YEARLY_PRICE_ID = "price_litefuse_pro_yearly";

beforeAll(() => {
  Object.assign(env, {
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    STRIPE_PRO_MONTHLY_PRICE_ID,
    STRIPE_PRO_YEARLY_PRICE_ID,
  });
});

async function createBillingOrg(role: Role = Role.OWNER) {
  const org = await prisma.organization.create({
    data: {
      id: uuidv4(),
      name: `Billing Org ${uuidv4().slice(0, 8)}`,
      cloudConfig: {
        stripe: {
          customerId: `cus_${uuidv4()}`,
        },
      },
    },
  });
  const user = await prisma.user.create({
    data: {
      id: uuidv4(),
      email: `billing-${uuidv4().slice(0, 8)}@example.com`,
      name: "Billing Test User",
    },
  });
  await prisma.organizationMembership.create({
    data: {
      orgId: org.id,
      userId: user.id,
      role,
    },
  });

  return { org, user };
}

function createSession(params: {
  user: { id: string; email: string | null; name: string | null };
  org: { id: string; name: string };
  role: Role;
  plan?: Plan;
}): Session {
  return {
    expires: "1",
    user: {
      id: params.user.id,
      email: params.user.email,
      name: params.user.name,
      canCreateOrganizations: true,
      organizations: [
        {
          id: params.org.id,
          name: params.org.name,
          role: params.role,
          plan: params.plan ?? "cloud:hobby",
          cloudConfig: undefined,
          metadata: {},
          aiFeaturesEnabled: false,
          projects: [],
        },
      ],
      featureFlags: {
        excludeClickhouseRead: false,
        templateFlag: true,
      },
      admin: false,
    },
    environment: {
      enableExperimentalFeatures: false,
      selfHostedInstancePlan: null,
    },
  };
}

function createCaller(session: Session) {
  const ctx = createInnerTRPCContext({ session, headers: {} });
  return appRouter.createCaller({ ...ctx, prisma });
}

function createSubscription(params: {
  orgId: string;
  customerId: string;
  status: Stripe.Subscription.Status;
  subscriptionId?: string;
}): Stripe.Subscription {
  return {
    id: params.subscriptionId ?? `sub_${uuidv4()}`,
    customer: params.customerId,
    status: params.status,
    metadata: {
      orgId: params.orgId,
    },
    current_period_start: 1_700_000_000,
    items: {
      data: [
        {
          current_period_start: 1_700_000_000,
          price: {
            id: STRIPE_PRO_MONTHLY_PRICE_ID,
            product: "prod_litefuse_pro",
          },
        },
      ],
    },
  } as unknown as Stripe.Subscription;
}

describe("Litefuse Pro billing", () => {
  it("builds the Pro catalogue from configured price ids", () => {
    expect(getBillingCatalogue()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          plan: "cloud:pro",
          interval: "month",
          priceId: STRIPE_PRO_MONTHLY_PRICE_ID,
        }),
        expect.objectContaining({
          plan: "cloud:pro",
          interval: "year",
          priceId: STRIPE_PRO_YEARLY_PRICE_ID,
        }),
      ]),
    );
  });

  it("rejects product ids in Pro price configuration", () => {
    const originalMonthlyPriceId = env.STRIPE_PRO_MONTHLY_PRICE_ID;
    const originalYearlyPriceId = env.STRIPE_PRO_YEARLY_PRICE_ID;

    Object.assign(env, {
      STRIPE_PRO_MONTHLY_PRICE_ID: "prod_litefuse_pro",
      STRIPE_PRO_YEARLY_PRICE_ID: undefined,
    });

    try {
      expect(getBillingCatalogue()).toEqual([]);
      expect(getInvalidBillingCatalogueEntries()).toEqual([
        {
          interval: "month",
          envVar: "STRIPE_PRO_MONTHLY_PRICE_ID",
          value: "prod_litefuse_pro",
        },
      ]);
    } finally {
      Object.assign(env, {
        STRIPE_PRO_MONTHLY_PRICE_ID: originalMonthlyPriceId,
        STRIPE_PRO_YEARLY_PRICE_ID: originalYearlyPriceId,
      });
    }
  });

  it("syncs active and canceled Stripe subscriptions into organization cloudConfig", async () => {
    const { org } = await createBillingOrg();
    const customerId = parseCloudConfig(org.cloudConfig)?.stripe?.customerId!;

    await syncSubscriptionToOrganization(
      createSubscription({
        orgId: org.id,
        customerId,
        status: "active",
        subscriptionId: "sub_active_pro",
      }),
    );

    const activeOrg = await prisma.organization.findUniqueOrThrow({
      where: { id: org.id },
    });
    const activeCloudConfig = parseCloudConfig(activeOrg.cloudConfig);
    expect(activeCloudConfig?.plan).toBe("Pro");
    expect(activeCloudConfig?.stripe?.activeSubscriptionId).toBe(
      "sub_active_pro",
    );
    expect(activeCloudConfig?.stripe?.activeProductId).toBe(
      "prod_litefuse_pro",
    );
    expect(getOrganizationPlanServerSide(activeCloudConfig ?? undefined)).toBe(
      "cloud:pro",
    );

    await syncSubscriptionToOrganization(
      createSubscription({
        orgId: org.id,
        customerId,
        status: "canceled",
        subscriptionId: "sub_active_pro",
      }),
      true,
    );

    const canceledOrg = await prisma.organization.findUniqueOrThrow({
      where: { id: org.id },
    });
    const canceledCloudConfig = parseCloudConfig(canceledOrg.cloudConfig);
    expect(canceledCloudConfig?.plan).toBeUndefined();
    expect(canceledCloudConfig?.stripe?.activeSubscriptionId).toBeNull();
    expect(
      getOrganizationPlanServerSide(canceledCloudConfig ?? undefined),
    ).toBe("cloud:hobby");
  });

  it("keeps Pro on past_due subscriptions", async () => {
    const { org } = await createBillingOrg();
    const customerId = parseCloudConfig(org.cloudConfig)?.stripe?.customerId!;

    await syncSubscriptionToOrganization(
      createSubscription({
        orgId: org.id,
        customerId,
        status: "past_due",
      }),
    );

    const updatedOrg = await prisma.organization.findUniqueOrThrow({
      where: { id: org.id },
    });
    const cloudConfig = parseCloudConfig(updatedOrg.cloudConfig);
    expect(cloudConfig?.plan).toBe("Pro");
    expect(cloudConfig?.stripe?.subscriptionStatus).toBe("past_due");
  });

  it("processes Stripe webhooks idempotently", async () => {
    const { org } = await createBillingOrg();
    const customerId = parseCloudConfig(org.cloudConfig)?.stripe?.customerId!;
    const event = {
      id: `evt_${uuidv4()}`,
      object: "event",
      api_version: "2025-01-27.acacia",
      created: 1_700_000_000,
      data: {
        object: createSubscription({
          orgId: org.id,
          customerId,
          status: "active",
        }),
      },
      livemode: false,
      pending_webhooks: 1,
      request: null,
      type: "customer.subscription.updated",
    };
    const payload = JSON.stringify(event);
    const signature = Stripe.webhooks.generateTestHeaderString({
      payload,
      secret: STRIPE_WEBHOOK_SECRET,
    });

    await expect(
      handleStripeWebhook({ rawBody: payload, signature }),
    ).resolves.toEqual({ received: true, duplicate: false });
    await expect(
      handleStripeWebhook({ rawBody: payload, signature }),
    ).resolves.toEqual({ received: true, duplicate: true });

    await expect(
      prisma.stripeWebhookEvent.count({
        where: { stripeEventId: event.id },
      }),
    ).resolves.toBe(1);
  });

  it("allows owners and rejects members for billing status", async () => {
    const ownerFixture = await createBillingOrg(Role.OWNER);
    const ownerCaller = createCaller(
      createSession({ ...ownerFixture, role: Role.OWNER }),
    );
    await expect(
      ownerCaller.billing.getBillingStatus({ orgId: ownerFixture.org.id }),
    ).resolves.toMatchObject({
      plan: "cloud:hobby",
    });

    const memberFixture = await createBillingOrg(Role.MEMBER);
    const memberCaller = createCaller(
      createSession({ ...memberFixture, role: Role.MEMBER }),
    );
    await expect(
      memberCaller.billing.getBillingStatus({ orgId: memberFixture.org.id }),
    ).rejects.toThrow("Forbidden");
  });
});
