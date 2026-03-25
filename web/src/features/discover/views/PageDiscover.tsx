// @ts-nocheck
import React from "react";
import { css } from "@emotion/css";
import { Card } from "@/src/components/ui/card";
import { LoadingBar } from "components/ui/loading-bar";
import DiscoverFilter from "components/discover-filter";
import DiscoverSidebar from "components/discover-sidebar";
import { DiscoverHistogram } from "components/discover-histogram";
import DiscoverContent from "components/discover-content";
import DiscoverHeader from "../components/discover-header";
import { testIds } from "../components/testIds";
import { useDiscoverData } from "./PageDiscover/useDiscoverData";

export default function PageDiscover() {
  const { loading, onQuerying } = useDiscoverData();
  const shellClassName = css`
    min-height: 100%;
    background:
      radial-gradient(
        circle at top left,
        hsl(var(--primary) / 0.08),
        transparent 28%
      ),
      linear-gradient(180deg, hsl(var(--muted) / 0.45), transparent 18rem);

    [data-discover-controls] > div {
      background: transparent !important;
      border: 0 !important;
      box-shadow: none !important;
    }

    [data-discover-controls] > div:first-of-type {
      padding: 0.85rem 1rem 0.7rem !important;
      border-bottom: 1px solid hsl(var(--border) / 0.85) !important;
      flex-wrap: wrap;
      gap: 0.5rem;
    }

    [data-discover-controls] > div:nth-of-type(2) {
      padding: 0.75rem 1rem 0 !important;
    }

    [data-discover-controls] > div:last-of-type {
      margin-top: 0 !important;
      padding: 0.7rem 1rem 0.9rem !important;
      border-radius: 0 !important;
      border-top: 1px solid hsl(var(--border) / 0.6);
    }

    [data-discover-sidebar] > div > div {
      background: transparent !important;
      box-shadow: none !important;
    }

    [data-discover-sidebar] > div > div:first-of-type {
      padding: 0 0 0.9rem !important;
      border-bottom: 1px solid hsl(var(--border) / 0.8);
      gap: 0.75rem;
    }

    [data-discover-sidebar] > div > div:last-of-type {
      margin-top: 0 !important;
      padding: 0.9rem 0 0 !important;
    }

    [data-discover-content] > div:first-of-type {
      overflow-x: auto;
    }

    [data-discover-content] table {
      min-width: 100%;
    }
  `;

  return (
    <div className={shellClassName}>
      <div className="flex min-h-full flex-col gap-4 px-3 py-4 lg:px-4">
        <Card className="border-border/70 overflow-hidden shadow-sm">
          <div
            data-discover-controls
            data-testid={testIds.pageTwo.container}
            className="bg-card"
          >
            <div className="bg-muted/30 px-4 py-2.5">
              <div className="text-foreground text-sm font-semibold tracking-tight">
                Query Builder
              </div>
              <div className="text-muted-foreground mt-0.5 text-xs">
                Select a Doris table, define a time window, and inspect raw log
                events.
              </div>
            </div>
            <DiscoverHeader
              onQuerying={onQuerying}
              loading={loading.getTableData || loading.getTableDataCharts}
            />
            <DiscoverFilter />
          </div>
        </Card>

        <section className="grid min-h-[calc(100vh-15rem)] flex-1 gap-4 xl:grid-cols-[18rem_minmax(0,1fr)]">
          <Card className="border-border/70 overflow-hidden shadow-sm">
            <div data-discover-sidebar className="h-full p-4">
              <DiscoverSidebar />
            </div>
          </Card>

          <Card className="border-border/70 relative flex min-h-0 flex-col overflow-hidden shadow-sm">
            <div className="pointer-events-none absolute inset-x-0 top-0 z-10">
              {loading.getTableDataCharts && <LoadingBar width={100} />}
            </div>
            <div className="border-border/80 border-b px-4 py-4 sm:px-5">
              <DiscoverHistogram />
            </div>
            <div
              data-discover-content
              className="min-h-0 flex-1 overflow-hidden px-2 pb-2 sm:px-3"
            >
              <div className="h-full overflow-auto">
                <DiscoverContent fetchNextPage={() => {}} />
              </div>
            </div>
          </Card>
        </section>
      </div>
    </div>
  );
}
