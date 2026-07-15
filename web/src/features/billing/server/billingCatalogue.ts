import { env } from "@/src/env.mjs";
import { z } from "zod/v4";

export const billingIntervalSchema = z.enum(["month", "year"]);
export type BillingInterval = z.infer<typeof billingIntervalSchema>;

export type BillingCatalogueEntry = {
  plan: "cloud:pro";
  planName: "Pro";
  interval: BillingInterval;
  priceId: string;
};

export type InvalidBillingCatalogueEntry = {
  interval: BillingInterval;
  envVar: "STRIPE_PRO_MONTHLY_PRICE_ID" | "STRIPE_PRO_YEARLY_PRICE_ID";
  value: string;
};

function getConfiguredProPrices() {
  return [
    {
      interval: "month" as const,
      envVar: "STRIPE_PRO_MONTHLY_PRICE_ID" as const,
      priceId: env.STRIPE_PRO_MONTHLY_PRICE_ID,
    },
    {
      interval: "year" as const,
      envVar: "STRIPE_PRO_YEARLY_PRICE_ID" as const,
      priceId: env.STRIPE_PRO_YEARLY_PRICE_ID,
    },
  ];
}

export function isStripePriceId(value: string): boolean {
  return value.startsWith("price_");
}

export function getBillingCatalogue(): BillingCatalogueEntry[] {
  return getConfiguredProPrices()
    .filter(
      (entry): entry is typeof entry & { priceId: string } =>
        typeof entry.priceId === "string" && isStripePriceId(entry.priceId),
    )
    .map((entry) => ({
      plan: "cloud:pro" as const,
      planName: "Pro" as const,
      interval: entry.interval,
      priceId: entry.priceId,
    }));
}

export function getInvalidBillingCatalogueEntries(): InvalidBillingCatalogueEntry[] {
  return getConfiguredProPrices()
    .filter(
      (entry): entry is typeof entry & { priceId: string } =>
        typeof entry.priceId === "string" && !isStripePriceId(entry.priceId),
    )
    .map((entry) => ({
      interval: entry.interval,
      envVar: entry.envVar,
      value: entry.priceId,
    }));
}

export function getProPriceForInterval(
  interval: BillingInterval,
): BillingCatalogueEntry | null {
  return (
    getBillingCatalogue().find((entry) => entry.interval === interval) ?? null
  );
}

export function getProPriceById(priceId: string): BillingCatalogueEntry | null {
  return (
    getBillingCatalogue().find((entry) => entry.priceId === priceId) ?? null
  );
}

export function isBillingCatalogueConfigured(): boolean {
  return getBillingCatalogue().length > 0;
}
