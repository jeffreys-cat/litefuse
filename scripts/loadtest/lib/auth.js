// NextAuth credentials login → returns the `next-auth.session-token` value.
// Web pages authenticate with this session cookie (not an API key).
import http from "k6/http";
import { check, fail } from "k6";
import { BASE_URL, EMAIL, PASSWORD } from "./config.js";

export function login() {
  // 1) CSRF token (+ csrf cookie stored in the per-VU jar automatically).
  const csrfRes = http.get(`${BASE_URL}/api/auth/csrf`);
  check(csrfRes, { "csrf 200": (r) => r.status === 200 });
  const csrfToken = csrfRes.json("csrfToken");
  if (!csrfToken) fail(`could not get csrfToken: ${csrfRes.body}`);

  // 2) credentials callback — NextAuth expects x-www-form-urlencoded.
  const loginRes = http.post(
    `${BASE_URL}/api/auth/callback/credentials`,
    {
      email: EMAIL,
      password: PASSWORD,
      csrfToken,
      callbackUrl: BASE_URL,
      json: "true",
    },
    { headers: { "Content-Type": "application/x-www-form-urlencoded" } },
  );

  const jar = http.cookieJar();
  const token = jar.cookiesForURL(BASE_URL)["next-auth.session-token"];
  if (!token || !token[0]) {
    fail(
      `login failed — no next-auth.session-token (status ${loginRes.status}). ` +
        `Check EMAIL/PASSWORD and AUTH_DISABLE_USERNAME_PASSWORD != "true".`,
    );
  }
  return token[0];
}
