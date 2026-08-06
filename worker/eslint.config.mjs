import baseConfig from "@repo/eslint-config";
import { tableRoutingRule } from "@repo/eslint-config/base";

export default [
  ...baseConfig,

  // Worker-specific ignores
  {
    name: "langfuse/worker/ignores",
    ignores: ["**/*test*.*", "**/worker-thread.js"],
  },
  tableRoutingRule([
    "src/features/**/*.ts",
    "src/queues/**/*.ts",
    "src/scripts/**/*.ts",
    "src/services/**/*.ts",
  ]),
];
