// Registry of page modules. Add a page here once its module exists and it
// becomes runnable via PAGE=<name> (or included in PAGE=all).
import * as tracing from "./tracing.js";

export const PAGES = {
  tracing,
  // sessions,   // TODO
  // users,      // TODO
  // dashboards, // TODO
  // prompts,    // TODO
  // scores,     // TODO
  // datasets,   // TODO
};
