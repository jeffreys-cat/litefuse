/**
 * Next.js route: /project/[projectId]/discover/traces
 *
 * Entry point for the Traces (distributed tracing explorer) view.
 * Injects the projectId into the shared grafana-runtime shim then renders
 * the PageTrace component inside the project layout.
 */
import React, { useEffect } from "react";
import { useRouter } from "next/router";
import dynamic from "next/dynamic";
import Page from "@/src/components/layouts/page";
import { setDiscoverProjectId } from "@/src/features/discover/shims/grafana-runtime";

const PageTrace = dynamic(() => import("@/src/features/discover/views/PageTrace"), {
  ssr: false,
  loading: () => (
    <div className="text-muted-foreground flex h-full items-center justify-center">
      Loading…
    </div>
  ),
});

export default function DiscoverTracesPage() {
  const router = useRouter();
  const projectId = router.query.projectId as string;

  if (projectId) {
    setDiscoverProjectId(projectId);
  }

  useEffect(() => {
    if (projectId) {
      setDiscoverProjectId(projectId);
    }
  }, [projectId]);

  return (
    <Page headerProps={{ title: "Traces" }} scrollable>
      {projectId ? <PageTrace /> : null}
    </Page>
  );
}
