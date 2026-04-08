// @ts-nocheck
import { useAtom, useAtomValue } from "jotai";
import React from "react";
import { nanoid } from "nanoid";
import { Button } from "components/ui/button";
import {
  Field,
  InlineField,
  InlineFieldRow,
  InlineSwitch,
} from "components/ui/form";
import { Input } from "components/ui/input";
import { Select } from "components/ui/select";
import {
  tableFieldsAtom,
  dataFilterAtom,
  tableFieldValuesAtom,
  surroundingDataFilterAtom,
} from "store/discover";
import { Operator } from "types/type";
import { OPERATORS } from "utils/data";
import { Controller, useForm } from "react-hook-form";
import {
  containerStyle,
  rowStyle,
  colStyle,
  footerStyle,
} from "./discover-filter.style";
import { FilterContentProps } from "../types";

export function FilterContent(props: FilterContentProps) {
  const { onHide, dataFilterValue } = props;
  const [surroundingDataFilter, setSurroundingDataFilter] = useAtom(
    surroundingDataFilterAtom,
  );
  const tableFields = useAtomValue(tableFieldsAtom);
  if (process.env.NODE_ENV !== "production") {
    dataFilterAtom.debugLabel = "dataFilter";
  }
  const tableFieldValue = useAtomValue(tableFieldValuesAtom);

  const {
    control,
    handleSubmit,
    watch,
    register,
    formState: { errors },
  } = useForm({
    defaultValues: {
      field: {
        label: dataFilterValue?.fieldName,
        value: dataFilterValue?.fieldName,
      },
      operator: {
        label: dataFilterValue?.operator,
        value: dataFilterValue?.operator,
      },
      value: dataFilterValue?.value,
      minValue: Array.isArray(dataFilterValue?.value)
        ? dataFilterValue?.value[0]
        : "",
      maxValue: Array.isArray(dataFilterValue?.value)
        ? dataFilterValue?.value[1]
        : "",
      label: dataFilterValue?.label || "",
      showLabel: !!dataFilterValue?.label, // Initialize based on dataFilterValue
    },
  });
  const operator = watch("operator");
  const showLabel: any = watch("showLabel");

  const getValue = (value: string): string | number =>
    isNaN(+value) ? value : +value;

  const onSubmit = (formValues: any) => {
    const { field, operator, value, minValue, maxValue, label } = formValues;
    const current = surroundingDataFilter.find(
      (f) => f.id === dataFilterValue?.id,
    );
    const id = dataFilterValue?.id || nanoid();

    let newValue: any[] = [];

    if (operator.value === "between" || operator.value === "not between") {
      if (minValue && maxValue) {
        newValue = [getValue(minValue), getValue(maxValue)];
      }
    } else if (value || typeof value === "number") {
      if (
        (operator.value === "like" || operator.value === "not like") &&
        typeof value === "string" &&
        !value.includes("%")
      ) {
        newValue = [`%${value}%`];
      } else {
        newValue = [value];
      }
    }

    const newItem = {
      id,
      fieldName: field.value,
      operator: operator.value,
      label,
      value: newValue,
    };

    if (current) {
      const updated = surroundingDataFilter.map((f) =>
        f.id === id ? newItem : f,
      );
      setSurroundingDataFilter(updated);
    } else {
      setSurroundingDataFilter([...surroundingDataFilter, newItem]);
    }

    onHide();
  };

  function renderFiledComponent() {
    const currentOperator = (
      typeof operator === "string" ? operator : operator?.value
    ) as Operator | undefined;
    if (
      currentOperator &&
      currentOperator !== "is null" &&
      currentOperator !== "is not null" &&
      (currentOperator === "between" || currentOperator === "not between")
    ) {
      return (
        <div className={rowStyle}>
          <div className={colStyle}>
            <Field
              label="Min value"
              invalid={!!errors.minValue}
              error={errors.minValue?.message}
            >
              <Input
                {...register("minValue", {
                  required: "Please enter min value",
                })}
              />
            </Field>
          </div>
          <div className={colStyle}>
            <Field
              label="Max value"
              invalid={!!errors.maxValue}
              error={errors.maxValue?.message}
            >
              <Input
                {...register("maxValue", {
                  required: "Please enter max value",
                })}
              />
            </Field>
          </div>
        </div>
      );
    }
    if (
      currentOperator === "=" ||
      currentOperator === "!=" ||
      currentOperator === "like" ||
      currentOperator === "not like" ||
      currentOperator === "match_all" ||
      currentOperator === "match_any" ||
      currentOperator === "match_phrase" ||
      currentOperator === "match_phrase_prefix"
    ) {
      return (
        <>
          <Field
            label="Value"
            invalid={!!errors.value}
            error={(errors.value as any)?.message}
          >
            <Input
              {...register("value", { required: "Please enter a value" })}
              list="field-value-list"
            />
          </Field>
          <datalist id="field-value-list">
            {tableFieldValue.map((item, idx) => (
              <option key={idx} value={item.value} />
            ))}
          </datalist>
        </>
      );
    }
    if (currentOperator === "in" || currentOperator === "not in") {
      return (
        <Field
          label="Value"
          invalid={!!errors.value}
          error={(errors.value as any)?.message}
        >
          <Controller
            name="value"
            control={control}
            rules={{ required: "Please enter a value" }}
            render={({ field }) => (
              <Select
                {...field}
                isMulti={true}
                options={tableFieldValue.map((item) => ({
                  label: item.value,
                  value: item.value,
                }))}
                placeholder="Select values"
                onChange={(selected) =>
                  field.onChange(
                    selected ? selected.map((s: any) => s.value) : [],
                  )
                }
                value={tableFieldValue
                  .filter(
                    (item) =>
                      Array.isArray(field.value) &&
                      field.value.includes(item.value),
                  )
                  .map((item) => ({
                    label: item.value,
                    value: item.value,
                  }))}
              />
            )}
          />
        </Field>
      );
    }
    return <></>;
  }

  return (
    <form onSubmit={handleSubmit(onSubmit)} className={containerStyle}>
      <div className={rowStyle}>
        <div className={colStyle}>
          <Field
            label="Column"
            invalid={!!errors.field}
            error={(errors.field as any)?.message}
          >
            <Controller
              name="field"
              control={control}
              rules={{ required: "Please select a field" }}
              render={({ field }) => (
                <Select
                  {...field}
                  options={tableFields.map((f) => ({
                    label: f.Field,
                    value: f.Field,
                  }))}
                />
              )}
            />
          </Field>
        </div>
        <div className={colStyle}>
          <Field
            label="Condition"
            invalid={!!errors.operator}
            error={(errors.operator as any)?.message}
          >
            <Controller
              name="operator"
              control={control}
              rules={{ required: "Please select an operator" }}
              render={({ field }) => (
                <Select
                  {...field}
                  options={OPERATORS.map((op) => ({
                    label: op,
                    value: op,
                  }))}
                />
              )}
            />
          </Field>
        </div>
      </div>

      {renderFiledComponent()}

      <InlineFieldRow>
        <InlineField label="Custom Label">
          <Controller
            name="showLabel"
            control={control}
            render={({ field }) => <InlineSwitch {...field} />}
          ></Controller>
        </InlineField>
      </InlineFieldRow>

      {showLabel && (
        <Field
          label="Label"
          invalid={!!errors.label}
          error={errors.label?.message}
        >
          <Input {...register("label", { required: "Please enter label" })} />
        </Field>
      )}

      <div className={footerStyle}>
        <Button
          variant="secondary"
          onClick={(e) => {
            e.preventDefault();
            onHide();
          }}
        >
          Cancel
        </Button>
        <Button type="submit">Apply</Button>
      </div>
    </form>
  );
}
