// @ts-nocheck
import React, { useState } from "react";
import { useAtom } from "jotai";
import { FilterContent } from "./filter-content";
import { surroundingDataFilterAtom } from "store/discover";
import { getFilterSQL } from "utils/data";
import { Toggletip } from "components/ui/toggletip";
import { DiscoverFilterProps } from "../types";
import { Badge } from "@/src/components/ui/badge";
import { Button } from "@/src/components/ui/button";
import { X, Plus } from "lucide-react";

export default function SurroundingDiscoverFilter(props: DiscoverFilterProps) {
  const [surroundingDataFilter, setSurroundingDataFilter] = useAtom(
    surroundingDataFilterAtom,
  );
  const [open, setOpen] = useState<boolean>(false);
  const [dataFilterOpen, setDataFilterOpen] = useState<any>({});

  return (
    <div className="flex flex-wrap items-center gap-2 px-4 py-2">
      <span className="text-muted-foreground text-xs font-medium">Filter</span>
      {surroundingDataFilter.map((dataFilterValue, index) => (
        <Toggletip
          key={index.toString()}
          show={dataFilterOpen[dataFilterValue.id]}
          onOpen={() =>
            setDataFilterOpen({ ...dataFilterOpen, [dataFilterValue.id]: true })
          }
          onClose={() =>
            setDataFilterOpen({
              ...dataFilterOpen,
              [dataFilterValue.id]: false,
            })
          }
          closeButton={true}
          content={
            <FilterContent
              onHide={() =>
                setDataFilterOpen({
                  ...dataFilterOpen,
                  [dataFilterValue.id]: false,
                })
              }
              dataFilterValue={dataFilterValue}
            />
          }
          placement="bottom"
        >
          <Badge
            variant="secondary"
            className="hover:bg-secondary cursor-pointer gap-1 pr-1"
          >
            <span className="max-w-50 truncate">
              {dataFilterValue.label
                ? dataFilterValue.label
                : getFilterSQL(dataFilterValue)}
            </span>
            <button
              type="button"
              className="text-muted-foreground hover:text-foreground ml-1"
              onClick={(e) => {
                e.stopPropagation();
                setSurroundingDataFilter(
                  surroundingDataFilter.filter((f) => f !== dataFilterValue),
                );
              }}
            >
              <X className="h-3 w-3" />
            </button>
          </Badge>
        </Toggletip>
      ))}
      <Toggletip
        show={open}
        closeButton={false}
        onOpen={() => setOpen(true)}
        content={<FilterContent onHide={() => setOpen(false)} />}
        placement="bottom"
      >
        <Button variant="outline" size="sm" className="h-6 px-2 text-xs">
          <Plus className="h-3 w-3" />
          Add filter
        </Button>
      </Toggletip>
    </div>
  );
}
