// @ts-nocheck
import React from "react";
import { getFieldIcon } from "utils/icon";
import { IconButton } from "components/ui/icon-button";
import { Toggletip } from "components/ui/toggletip";
import { TopData } from "./top-data/top-data";

interface FieldItemProps {
  field: any;
  onAdd?: (field: any) => void;
  onRemove?: (field: any) => void;
  type: "add" | "remove";
}

export default function FieldItem(props: FieldItemProps) {
  const { field } = props;
  field.key = field.Field;
  if (field.children) {
    field.icon = <div className="text-n4 w-4 text-sm leading-8">{}</div>;
    return (
      <div className="-ml-3 flex">
        Tree
        {/* <Tree showIcon className={`${TreeStyle} ${DiscoverTreeStyle}`} treeData={[field]} switcherIcon={<SDIcon type="icon-arrow-down" className="dark:text-n6" />} /> */}
      </div>
    );
  }
  return (
    <div>
      <Toggletip placement="right" content={<TopData field={field} />}>
        <div className="group hover:bg-muted/50 flex min-h-9 w-full cursor-pointer items-center justify-between gap-2 px-2 text-left">
          <div className="flex min-w-0 items-center gap-2">
            <div className="text-muted-foreground inline-flex h-6 w-6 shrink-0 items-center justify-center">
              {getFieldIcon(field["Type"])}
            </div>
            <div className="min-w-0 flex-1 overflow-hidden text-ellipsis whitespace-nowrap">
              {field["Field"]}
            </div>
          </div>
          <div className="text-muted-foreground hover:text-foreground ml-auto flex items-center opacity-0 transition-opacity group-hover:opacity-100">
            {props.type === "add" ? (
              <IconButton
                name="plus"
                size="sm"
                tooltip="添加到表格"
                onClick={(e) => {
                  props?.onAdd?.(field);
                  e.stopPropagation();
                }}
              />
            ) : (
              <IconButton
                name="minus"
                size="sm"
                tooltip="从表格删除"
                onClick={(e: any) => {
                  props?.onRemove?.(field);
                  e.stopPropagation();
                }}
              />
            )}
          </div>
        </div>
      </Toggletip>
    </div>
  );
}
