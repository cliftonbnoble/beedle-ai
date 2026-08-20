import nodeTest, { before as nodeBefore } from "node:test";

// TEST-03: live suites hard-required a running server — `node --test tests/` could never fully pass and
// an unreachable server surfaced as assertion failures instead of skips. Import { test, before } from
// this module instead of "node:test" and the whole suite skips cleanly (tests report SKIP, setup hooks
// no-op) when the API isn't running; with a server the behavior is byte-identical to node:test.
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

async function serverReachable() {
  try {
    const response = await fetch(`${apiBase}/health`, { signal: AbortSignal.timeout(3000) });
    return response.ok;
  } catch {
    return false;
  }
}

// AUTH-02: the worker now authenticates every non-public route (session cookie + CSRF header on
// unsafe methods), so live suites must log in before exercising admin endpoints. Done once here —
// the module every live suite already imports — by patching global fetch to attach the session
// headers to apiBase requests. Credentials default to the local .dev.vars pair; override with
// AUTH_TEST_USERNAME / AUTH_TEST_PASSWORD to point suites at a differently-provisioned server.
// A server whose auth is unconfigured (503) or rejects the credentials skips the suites cleanly,
// same as an unreachable server; a pre-auth server (404 on /auth/session) runs unpatched.
async function establishAuthSession() {
  try {
    const session = await fetch(`${apiBase}/auth/session`, { signal: AbortSignal.timeout(3000) });
    if (session.status === 404) return { ok: true, patch: null };
    if (session.status === 503) return { ok: false, reason: "auth not configured on live API server" };
  } catch {
    return { ok: false, reason: "auth session probe failed" };
  }
  const username = process.env.AUTH_TEST_USERNAME || "dev-admin";
  const password = process.env.AUTH_TEST_PASSWORD || "local-dev-password-2026";
  try {
    const login = await fetch(`${apiBase}/auth/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username, password }),
      signal: AbortSignal.timeout(5000)
    });
    if (login.status !== 200) return { ok: false, reason: `auth login failed (${login.status}) for live API server` };
    const cookie = (login.headers.get("set-cookie") || "").split(";")[0];
    const body = await login.json();
    if (!cookie || !body?.csrfToken) return { ok: false, reason: "auth login returned no session cookie or CSRF token" };
    return { ok: true, patch: { cookie, csrfToken: body.csrfToken } };
  } catch {
    return { ok: false, reason: "auth login request failed" };
  }
}

let reachable = await serverReachable();
let unreachableReason = `live API server not reachable at ${apiBase}`;
if (reachable) {
  const auth = await establishAuthSession();
  if (!auth.ok) {
    reachable = false;
    unreachableReason = auth.reason;
  } else if (auth.patch) {
    const { cookie, csrfToken } = auth.patch;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (input, init = {}) => {
      const url = typeof input === "string" ? input : input?.url ? String(input.url) : String(input);
      if (!url.startsWith(apiBase)) return originalFetch(input, init);
      const headers = new Headers(init.headers || (typeof input === "object" && input?.headers) || {});
      if (!headers.has("cookie")) headers.set("cookie", cookie);
      if (!headers.has("x-beedle-csrf")) headers.set("x-beedle-csrf", csrfToken);
      return originalFetch(input, { ...init, headers });
    };
  }
}
const skipReason = reachable ? false : unreachableReason;

export function test(name, optionsOrFn, maybeFn) {
  const hasOptions = typeof optionsOrFn === "object" && optionsOrFn !== null;
  const options = hasOptions ? optionsOrFn : {};
  const fn = hasOptions ? maybeFn : optionsOrFn;
  return nodeTest(name, { ...options, skip: options.skip || skipReason }, fn);
}

export function before(fn, options) {
  return nodeBefore(async (...args) => {
    if (!reachable) return;
    return fn(...args);
  }, options);
}
