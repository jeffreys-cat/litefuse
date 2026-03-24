import React from "react";
import { getFieldIcon } from "utils/icon";
import { IconButton } from "components/ui/icon-button";
import { useDiscoverTheme } from "components/ui/theme";
import { Toggletip } from "components/ui/toggletip";
import { css } from "@emotion/css";
import { cn } from "utils/tailwind";
import { TopData } from "./top-data/top-data";

interface FieldItemProps {
  field: any;
  onAdd?: (field: any) => void;
  onRemove?: (field: any) => void;
  type: "add" | "remove";
}

export default function FieldItem(props: FieldItemProps) {
  const theme = useDiscoverTheme();
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
        <div
          className={css`
            width: 100%;
            text-align: left;
            display: flex;
            align-items: center;
            justify-content: space-between;
            min-height: 36px;
            padding: 0 8px;
            gap: 8px;
            cursor: pointer;
            &:hover .icon-wrapper {
              opacity: 1;
            }
            &:hover {
              background-color: ${theme.colors.background.secondary};
            }
          `}
        >
          <div className="flex min-w-0 items-center gap-2">
            <div
              className={css`
                display: inline-flex;
                height: 1.5rem;
                width: 1.5rem;
                flex-shrink: 0;
                align-items: center;
                justify-content: center;
                color: ${theme.colors.text.secondary};
              `}
            >
              {getFieldIcon(field["Type"])}
            </div>
            <div
              className={css`
                display: flex;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
                min-width: 0;
                flex: 1 1 auto;
              `}
            >
              {field["Field"]}
            </div>
          </div>
          <div
            className={cn(
              "icon-wrapper",
              css`
                opacity: 0;
                transition: opacity 0.2s;
                margin-left: auto;
                display: flex;
                align-items: center;
                color: ${theme.colors.text.secondary};
                &:hover {
                  color: ${theme.colors.text.primary};
                }
              `,
            )}
          >
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
