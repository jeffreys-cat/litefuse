import { handleStripeWebhook } from "@/src/features/billing/server/stripeWebhookHandler";
import { logger } from "@langfuse/shared/src/server";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const rawBody = await request.text();
    const result = await handleStripeWebhook({
      rawBody,
      signature: request.headers.get("stripe-signature"),
    });
    return NextResponse.json(result);
  } catch (error) {
    logger.error("Stripe webhook failed", error);
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Stripe webhook failed",
      },
      { status: 400 },
    );
  }
}
