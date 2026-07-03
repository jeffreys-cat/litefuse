import { z } from "zod/v4";
import {
  singleFilter,
  type langfuseObjects,
  TimeScopeSchema,
  wipVariableMapping,
} from "@langfuse/shared";

// Legacy eval targets (TRACE, DATASET) use full variable mapping UI with object selector
// Modern eval targets (EVENT, EXPERIMENT) use simplified UI with just column selection
export const isLegacyEvalTarget = (target: string): boolean =>
  target === "trace" || target === "dataset";

export const evalConfigFormSchema = z.object({
  scoreName: z.string(),
  target: z.string(),
  filter: z.array(singleFilter).nullable(), // reusing the filter type from the tables
  mapping: z.array(wipVariableMapping),
  sampling: z.coerce.number().gt(0).lte(1),
  delay: z.coerce.number().min(0).optional().default(10),
  timeScope: TimeScopeSchema,
  runOnLive: z.boolean().optional().default(true),
});

export type EvalFormType = z.infer<typeof evalConfigFormSchema>;

export type LangfuseObject = (typeof langfuseObjects)[number];

export type VariableMapping = z.infer<typeof wipVariableMapping>;

export const inferDefaultMapping = (
  _variable: string,
): Pick<VariableMapping, "selectedColumnId"> => {
  return {
    selectedColumnId: undefined,
  };
};

export const createDefaultFormMappings = (
  variables: string[],
  target: string,
): z.infer<typeof wipVariableMapping>[] =>
  variables.map((templateVariable) => ({
    templateVariable,
    langfuseObject: isLegacyEvalTarget(target) ? "trace" : undefined,
    objectName: isLegacyEvalTarget(target) ? null : undefined,
    jsonSelector: null,
    ...inferDefaultMapping(templateVariable),
  }));

export const fieldHasJsonSelectorOption = (
  selectedColumnId: string | undefined | null,
): boolean =>
  selectedColumnId === "input" ||
  selectedColumnId === "output" ||
  selectedColumnId === "metadata" ||
  selectedColumnId === "expected_output" ||
  selectedColumnId === "experiment_item_expected_output" ||
  selectedColumnId === "expectedOutput" ||
  selectedColumnId === "experimentItemExpectedOutput";

export const getTargetDisplayName = (target: string): string => {
  switch (target) {
    case "trace":
      return "traces";
    case "event":
      return "observations";
    case "dataset":
      return "dataset run items";
    case "experiment":
      return "experiments";
    default:
      return target;
  }
};
