/**
 * Next.js Pages Router route: /project/[projectId]/logging
 *
 * Entry point for the Logging (log explorer) feature.
 */
import React from "react";
import { useRouter } from "next/router";
import Page from "@/src/components/layouts/page";
import PageDiscover from "@/src/features/discover/views/PageDiscover";

export default function LoggingPage() {
  const router = useRouter();
  const projectId = router.query.projectId as string;

  return (
    <Page headerProps={{ title: "Logging" }}>
      {projectId ? <PageDiscover /> : null}
    </Page>
  );
}
