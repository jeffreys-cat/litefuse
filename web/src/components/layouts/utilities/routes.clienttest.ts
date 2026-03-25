import { processNavigation } from "@/src/components/layouts/utilities/routes";
import { RouteGroup, RouteSection } from "@/src/components/layouts/routes";

describe("processNavigation", () => {
  it("includes logging under the observability group in main navigation", () => {
    const { mainNavigation } = processNavigation((route) => ({
      ...route,
      url: route.pathname,
      isActive: false,
    }));

    expect(mainNavigation.grouped?.[RouteGroup.Observability]).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: "Logging",
          section: RouteSection.Main,
          group: RouteGroup.Observability,
          url: "/project/[projectId]/discover",
        }),
      ]),
    );
  });
});
