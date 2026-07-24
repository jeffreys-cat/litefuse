// Shared configuration, all overridable via environment variables.
//
//   BASE_URL    target server            (default http://localhost:3000)
//   PROJECT_ID  project to test against  (default demo seed project)
//   EMAIL       login email
//   PASSWORD    login password
//   PAGE        which page module to run (default "tracing", or "all")
//   VUS         concurrent virtual users (default 10)
//   DURATION    test duration            (default 1m)
//   SLEEP       think-time between iterations in seconds (default 0.3)
//   WINDOW_DAYS display time window the page UI requests (default 1)

export const BASE_URL = __ENV.BASE_URL || "http://localhost:3000";
export const PROJECT_ID =
  __ENV.PROJECT_ID || "7a88fb47-b4e2-43b8-a06c-a5ce950dc53a";
export const EMAIL = __ENV.EMAIL || "demo@litefuse.ai";
export const PASSWORD = __ENV.PASSWORD || "password";
export const PAGE = __ENV.PAGE || "tracing";
export const VUS = parseInt(__ENV.VUS || "10", 10);
export const DURATION = __ENV.DURATION || "1m";
export const SLEEP = parseFloat(__ENV.SLEEP || "0.3");

const WINDOW_DAYS = parseInt(__ENV.WINDOW_DAYS || "1", 10);
const DAY_MS = 86400000;

// What the page UI requests by default (e.g. "Past 1 day"). Used by list/metric
// queries so the load mirrors a real page view.
export function displayWindow() {
  const to = new Date();
  return { from: new Date(to.getTime() - WINDOW_DAYS * DAY_MS), to };
}

// A deliberately huge window used only by discover() so it always finds rows to
// drive the detail (byId) queries, regardless of how old the seed data is.
export function wideWindow() {
  const to = new Date();
  return { from: new Date(to.getTime() - 3650 * DAY_MS), to };
}
