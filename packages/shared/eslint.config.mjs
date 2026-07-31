import baseConfig from "@repo/eslint-config";
import { tableRoutingRule } from "@repo/eslint-config/base";

export default [
  ...baseConfig,

  // Table-split guard: no bare events_full/traces_scalar SQL literals in the
  // query-building layer (Stage 0.7 — route through tableFor/sharedTableFor).
  tableRoutingRule([
    "src/server/repositories/**/*.ts",
    "src/server/services/**/*.ts",
  ]),
];
