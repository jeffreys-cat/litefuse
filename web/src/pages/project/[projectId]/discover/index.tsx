/**
 * Next.js Pages Router route: /project/[projectId]/discover
 *
 * Entry point for the Discover (log explorer) feature.
 * Injects the projectId into the shared grafana-runtime shim then renders
 * the PageDiscover component inside the project layout.
 */
import React, { useEffect } from "react";
import { useRouter } from "next/router";
import Page from "@/src/components/layouts/page";
import { setDiscoverProjectId } from "@/src/features/discover/shims/grafana-runtime";
import PageDiscover from "@/src/features/discover/views/PageDiscover";

// const PageDiscover = dynamic(() => import("./views/PageDiscover"), {
//   ssr: false,
//   loading: () => (
//     <div className="text-muted-foreground flex h-full items-center justify-center">
//       Loading…
//     </div>
//   ),
// });

export default function DiscoverPage() {
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
    <Page headerProps={{ title: "Logging" }} scrollable>
      {projectId ? <PageDiscover /> : null}
      {/* {projectId ? <PageDiscover /> : null} */}
    </Page>
  );
}
