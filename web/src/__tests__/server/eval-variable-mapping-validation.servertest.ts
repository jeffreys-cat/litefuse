/** @jest-environment node */

import { EvalTargetObject } from "@langfuse/shared";
import {
  createDefaultFormMappings,
  inferDefaultMapping,
} from "@/src/features/evals/utils/evaluator-form-utils";
import { validateAndTransformVariableMapping } from "@/src/features/evals/utils/variable-mapping-validation";

describe("evaluator variable mapping defaults", () => {
  it("does not preselect an Object Field for new template variables", () => {
    expect(inferDefaultMapping("query")).toEqual({
      selectedColumnId: undefined,
    });
    expect(inferDefaultMapping("generation")).toEqual({
      selectedColumnId: undefined,
    });
    expect(
      createDefaultFormMappings(["query"], EvalTargetObject.TRACE),
    ).toEqual([
      {
        templateVariable: "query",
        langfuseObject: "trace",
        objectName: null,
        selectedColumnId: undefined,
        jsonSelector: null,
      },
    ]);
  });

  it("returns an explicit error when any Object Field is empty", () => {
    expect(
      validateAndTransformVariableMapping(
        [
          {
            templateVariable: "query",
            langfuseObject: "trace",
            selectedColumnId: "",
          },
        ],
        EvalTargetObject.TRACE,
      ),
    ).toEqual({
      success: false,
      error:
        "Please select an Object Field for every Evaluation Prompt variable before executing the evaluator.",
    });
  });
});
