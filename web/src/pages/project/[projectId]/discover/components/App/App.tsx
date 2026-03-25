/**
 * App.tsx — formerly the React-Router-based root for the Grafana plugin.
 *
 * Routing is now handled by Next.js file-based routes:
 *   - src/pages/project/[projectId]/discover/index.tsx  → PageDiscover
 *   - src/pages/project/[projectId]/discover/traces.tsx  → PageTrace
 *
 * This file is kept as a thin wrapper in case any component still imports from it.
 */
import React, { Suspense } from "react";

const Discover = React.lazy(() => import("../../views/PageDiscover"));
const PageTrace = React.lazy(() => import("../../views/PageTrace"));
// const PageDashboard = React.lazy(() => import('../../views/PageDashboard'));

export { Discover, PageTrace };

function App() {
  return (
    <Suspense fallback={null}>
      <Discover />
    </Suspense>
  );
}

export default App;
