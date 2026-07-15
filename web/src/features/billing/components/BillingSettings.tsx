import Header from "@/src/components/layouts/header";
import { Alert, AlertDescription, AlertTitle } from "@/src/components/ui/alert";
import { Badge } from "@/src/components/ui/badge";
import { Button } from "@/src/components/ui/button";
import { Skeleton } from "@/src/components/ui/skeleton";
import { api } from "@/src/utils/api";
import { planLabels } from "@langfuse/shared";
import { AlertCircle, CreditCard, ExternalLink } from "lucide-react";
import { useRouter } from "next/router";
import { useState } from "react";
import { toast } from "sonner";

type BillingSettingsProps = {
  orgId: string;
};

const statusCopy: Record<string, string> = {
  active: "Active",
  trialing: "Trialing",
  past_due: "Past due",
  canceled: "Canceled",
  unpaid: "Unpaid",
  incomplete: "Incomplete",
  incomplete_expired: "Incomplete expired",
  paused: "Paused",
};

export function BillingSettings({ orgId }: BillingSettingsProps) {
  const router = useRouter();
  const [pendingInterval, setPendingInterval] = useState<
    "month" | "year" | null
  >(null);
  const billingStatus = api.billing.getBillingStatus.useQuery(
    { orgId },
    { refetchOnWindowFocus: false },
  );
  const checkoutMutation = api.billing.createCheckoutSession.useMutation({
    onSuccess: ({ url }) => {
      window.location.assign(url);
    },
    onError: () => setPendingInterval(null),
  });
  const portalMutation = api.billing.createPortalSession.useMutation({
    onSuccess: ({ url }) => {
      window.location.assign(url);
    },
  });

  const status = billingStatus.data?.stripe.subscriptionStatus;
  const isPastDue = status === "past_due";
  const hasCustomer = Boolean(billingStatus.data?.stripe.customerId);
  const checkoutSuccess = router.query.checkout === "success";
  const checkoutCancelled = router.query.checkout === "cancelled";

  const startCheckout = (interval: "month" | "year") => {
    setPendingInterval(interval);
    checkoutMutation.mutate({ orgId, interval });
  };

  const openPortal = () => {
    if (!hasCustomer) {
      toast.error("No Stripe customer exists for this organization yet.");
      return;
    }
    portalMutation.mutate({ orgId });
  };

  if (billingStatus.isLoading) {
    return (
      <div className="flex flex-col gap-6">
        <Header title="Billing" />
        <Skeleton className="h-32 w-full" />
        <Skeleton className="h-40 w-full" />
      </div>
    );
  }

  const plan = billingStatus.data?.plan ?? "cloud:hobby";
  const planLabel = planLabels[plan];
  const proPrices = billingStatus.data?.proPrices ?? [];
  const billingConfigurationIssues =
    billingStatus.data?.billingConfigurationIssues ?? [];
  const hasMonthly = proPrices.some((price) => price.interval === "month");
  const hasYearly = proPrices.some((price) => price.interval === "year");

  return (
    <div className="flex flex-col gap-6">
      <Header title="Billing" />

      {checkoutSuccess ? (
        <Alert>
          <CreditCard className="h-4 w-4" />
          <AlertTitle>Checkout completed</AlertTitle>
          <AlertDescription>
            Stripe will confirm the subscription by webhook shortly.
          </AlertDescription>
        </Alert>
      ) : null}

      {checkoutCancelled ? (
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Checkout cancelled</AlertTitle>
          <AlertDescription>
            No billing changes were made to this organization.
          </AlertDescription>
        </Alert>
      ) : null}

      {isPastDue ? (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Payment needs attention</AlertTitle>
          <AlertDescription>
            Pro access remains enabled for now. Update the payment method in
            Stripe to avoid subscription cancellation.
          </AlertDescription>
        </Alert>
      ) : null}

      {!billingStatus.data?.isCloudBillingConfigured ? (
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Stripe is not configured</AlertTitle>
          <AlertDescription>
            Configure Stripe environment variables before using Pro billing.
          </AlertDescription>
        </Alert>
      ) : null}

      {billingConfigurationIssues.length > 0 ? (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Stripe price configuration is invalid</AlertTitle>
          <AlertDescription>
            {billingConfigurationIssues.join(" ")}
          </AlertDescription>
        </Alert>
      ) : null}

      <section className="bg-background rounded-lg border">
        <div className="flex flex-col gap-3 p-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h3 className="text-base font-semibold">Current plan</h3>
            <p className="text-muted-foreground text-sm">
              Billing is managed per organization.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={plan === "cloud:pro" ? "success" : "secondary"}>
              {planLabel}
            </Badge>
            {status ? (
              <Badge variant={isPastDue ? "warning" : "outline-solid"}>
                {statusCopy[status] ?? status}
              </Badge>
            ) : null}
          </div>
        </div>
        <div className="border-t p-4">
          <Button
            variant="outline"
            onClick={openPortal}
            loading={portalMutation.isPending}
            disabled={
              !hasCustomer || !billingStatus.data?.isCloudBillingConfigured
            }
          >
            <ExternalLink className="mr-2 h-4 w-4" />
            Manage billing
          </Button>
        </div>
      </section>

      <section className="bg-background rounded-lg border">
        <div className="p-4">
          <h3 className="text-base font-semibold">Pro</h3>
          <p className="text-muted-foreground text-sm">
            Upgrade this organization to unlock Pro entitlements.
          </p>
        </div>
        <div className="grid gap-3 border-t p-4 sm:grid-cols-2">
          <div className="rounded-md border p-4">
            <div className="mb-4">
              <h4 className="text-sm font-semibold">Monthly</h4>
              <p className="text-muted-foreground text-sm">
                Flexible monthly subscription.
              </p>
            </div>
            <Button
              onClick={() => startCheckout("month")}
              loading={
                checkoutMutation.isPending && pendingInterval === "month"
              }
              disabled={
                !hasMonthly || !billingStatus.data?.isCloudBillingConfigured
              }
            >
              <CreditCard className="mr-2 h-4 w-4" />
              Upgrade monthly
            </Button>
          </div>
          <div className="rounded-md border p-4">
            <div className="mb-4">
              <h4 className="text-sm font-semibold">Yearly</h4>
              <p className="text-muted-foreground text-sm">
                Annual subscription for Pro access.
              </p>
            </div>
            <Button
              onClick={() => startCheckout("year")}
              loading={checkoutMutation.isPending && pendingInterval === "year"}
              disabled={
                !hasYearly || !billingStatus.data?.isCloudBillingConfigured
              }
            >
              <CreditCard className="mr-2 h-4 w-4" />
              Upgrade yearly
            </Button>
          </div>
        </div>
      </section>
    </div>
  );
}
