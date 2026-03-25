import { useAtom, useAtomValue } from "jotai";
import React, { useState } from "react";
import FieldItem from "./field-item/field-item";
import { FilterContent } from "./filter-content/filter-content";
import {
  selectedFieldsAtom,
  tableFieldsAtom,
  searchableAtom,
  aggregatableAtom,
  fieldTypeAtom,
  indexesAtom,
} from "store/discover";
import {
  AggregatableEnum,
  getFieldType,
  SearchableEnum,
  FieldTypeEnum,
} from "utils/data";
import { Button } from "components/ui/button";
import { CollapsableSection } from "components/ui/collapsible-section";
import { Icon } from "components/ui/icon";
import { Input } from "components/ui/input";
import { useDiscoverTheme } from "components/ui/theme";
import { Toggletip } from "components/ui/toggletip";
import { css } from "@emotion/css";

export default function DiscoverSidebar() {
  const [selectedFields, setSelectedFields] = useAtom(selectedFieldsAtom);
  const tableFields = useAtomValue(tableFieldsAtom);
  const [searchable, _setSearchable] = useAtom(searchableAtom);
  const [aggregatable, _setAggregatable] = useAtom(aggregatableAtom);
  const [fieldType, _setFieldType] = useAtom(fieldTypeAtom);
  const [searchValue, setSearchValue] = useState("");
  const indexes = useAtomValue(indexesAtom);
  const theme = useDiscoverTheme();
  const filteredFields = tableFields
    .filter((field) => {
      if (aggregatable === AggregatableEnum.ANY) {
        return true;
      }
      if (aggregatable === AggregatableEnum.YES) {
        return getFieldType(field.Type) === "NUMBER";
      }
      if (aggregatable === AggregatableEnum.NO) {
        return getFieldType(field.Type) !== "NUMBER";
      }
      return false;
    })
    .filter((field: any) => {
      if (searchable === SearchableEnum.ANY) {
        return true;
      }
      if (searchable === SearchableEnum.YES) {
        return indexes.some((index) => field.Field === index.Field);
      }
      if (searchable === SearchableEnum.NO) {
        return !indexes.some((index) => field.Field === index.Field);
      }
      return false;
    })
    .filter((field) => {
      if (fieldType === FieldTypeEnum.ANY) {
        return true;
      }
      return getFieldType(field.Type) === fieldType;
    });
  const hasSelectedFields = selectedFields.length > 0;
  const availableFields = filteredFields.filter((filed) => {
    if (selectedFields.find((item) => filed["Field"] === item["Field"])) {
      return false;
    }
    return true;
  });

  function handleAdd(field: any) {
    setSelectedFields([...selectedFields, field] as any);
  }

  function handleRemove(field: any) {
    const index = selectedFields.findIndex(
      (item: any) => item.Field === field.Field,
    );
    selectedFields.splice(index, 1);
    setSelectedFields([...selectedFields]);
  }

  return (
    <div
      className={css`
        display: flex;
        flex-direction: column;
        height: 100%;
      `}
    >
      <div
        className={css`
          display: flex;
          background-color: ${theme.isDark ? "rgb(24, 27, 31)" : "#FFF"};
          min-height: 40px;
          padding: 4px 8px;
          border-radius: 0.25rem 0.25rem 0 0;
          align-items: center;
          column-gap: 0.5rem;
        `}
      >
        <span
          className={css`
            display: inline-flex;
            height: 1.75rem;
            width: 1.75rem;
            flex-shrink: 0;
            align-items: center;
            justify-content: center;
            color: ${theme.colors.text.secondary};
          `}
        >
          <Icon name="search" size="md" />
        </span>
        <Input
          placeholder={`Search`}
          className={css`
            border: none;
            padding-left: 0;
            padding-right: 0;
          `}
          value={searchValue}
          onChange={(e: any) => setSearchValue(e.target.value)}
        />
        <Toggletip content={<FilterContent />}>
          <button
            type="button"
            className={css`
              display: inline-flex;
              height: 1.75rem;
              width: 1.75rem;
              flex-shrink: 0;
              align-items: center;
              justify-content: center;
              border-radius: 0.375rem;
              color: ${theme.colors.text.secondary};
              transition:
                background-color 0.2s ease,
                color 0.2s ease;
              &:hover {
                background-color: ${theme.colors.background.secondary};
                color: ${theme.colors.text.primary};
              }
            `}
          >
            <Icon name="filter" size="md" />
          </button>
        </Toggletip>
      </div>
      <div
        className={css`
          margin-top: 1px;
          flex: 1;
          padding: 0;
          background-color: ${theme.isDark ? "rgb(24, 27, 31)" : "#FFF"};
          height: 100%;
          overflow: auto;
        `}
      >
        <CollapsableSection
          label={
            <span
              className={css`
                margin-left: 4px;
                font-size: 14px;
                line-height: 32px;
              `}
            >
              Selected fields
            </span>
          }
          isOpen={true}
        >
          <div
            className={css`
              width: 100%;
            `}
          >
            {hasSelectedFields ? (
              <div
                className={css`
                  width: 100%;
                `}
              >
                {selectedFields
                  .filter((field: any) => {
                    return field["Field"].includes(searchValue);
                  })
                  .map((field: any, index) => (
                    <FieldItem
                      type="remove"
                      key={index}
                      field={field}
                      onRemove={(field) => handleRemove(field)}
                    />
                  ))}
              </div>
            ) : (
              <Button
                icon="arrow"
                size="sm"
                variant="secondary"
                fill="text"
                fullWidth={true}
                className={css`
                  min-height: 36px;
                  width: 100%;
                  text-align: left;
                  justify-content: flex-start;
                  gap: 0.5rem;
                  padding-left: 0.5rem;
                `}
              >
                _source
              </Button>
            )}
          </div>
        </CollapsableSection>
        <CollapsableSection
          label={
            <span
              className={css`
                margin-left: 4px;
                font-size: 14px;
                line-height: 32px;
              `}
            >
              Available fields
            </span>
          }
          isOpen={true}
        >
          <div
            className={css`
              width: 100%;
            `}
          >
            {availableFields
              .filter((field: any) => {
                return field["Field"].includes(searchValue);
              })
              .map((field: any, index) => (
                <FieldItem
                  type="add"
                  field={field}
                  key={index}
                  onAdd={(field) => handleAdd(field)}
                />
              ))}
          </div>
        </CollapsableSection>
      </div>
    </div>
  );
}
