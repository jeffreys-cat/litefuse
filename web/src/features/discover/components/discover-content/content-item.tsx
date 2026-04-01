// @ts-nocheck
import { useAtom } from "jotai";
import { css } from "@emotion/css";
import { nanoid } from "nanoid";
import { dataFilterAtom } from "store/discover";
import React from "react";
import { isComplexType, isValidTimeFieldType } from "utils/data";
import { IconButton } from "components/ui/icon-button";
import dayjs from "dayjs";

interface ContentItemProps {
  fieldName: string;
  fieldValue: string | number;
  fieldType: string;
}

function toFilterValue(
  value: string | number,
  fieldType: string,
): string | number {
  if (typeof value === "string" && isValidTimeFieldType(fieldType)) {
    const d = dayjs(value);
    if (d.isValid()) {
      const msPart = value.includes(".") ? value.split(".")[1] : null;
      const fmt = msPart
        ? `YYYY-MM-DD HH:mm:ss.${"S".repeat(msPart.length)}`
        : "YYYY-MM-DD HH:mm:ss";
      return d.utc().format(fmt);
    }
  }
  return value;
}

export function ContentItem({
  fieldName,
  fieldValue,
  fieldType,
}: ContentItemProps) {
  const [dataFilter, setDataFilter] = useAtom(dataFilterAtom);
  return (
    <div>
      {!isComplexType(fieldType) && (
        <div
          className={css`
            display: flex;
            alignitems: "center";
            margin-left: 10px;
          `}
        >
          <IconButton
            name="plus-circle"
            onClick={(e) => {
              setDataFilter([
                ...dataFilter,
                {
                  fieldName,
                  operator: "=",
                  value: [toFilterValue(fieldValue, fieldType)],
                  id: nanoid(),
                },
              ]);
              e.stopPropagation();
            }}
            tooltip="Equivalent filtration"
          />
          <IconButton
            name="minus-circle"
            style={{ marginLeft: "4px" }}
            onClick={(e) => {
              setDataFilter([
                ...dataFilter,
                {
                  fieldName,
                  operator: "!=",
                  value: [toFilterValue(fieldValue, fieldType)],
                  id: nanoid(),
                },
              ]);
              e.stopPropagation();
            }}
            tooltip="Nonequivalent filtration"
          />
        </div>
      )}
    </div>
  );
}
