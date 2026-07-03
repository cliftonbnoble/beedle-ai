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

const reachable = await serverReachable();
const skipReason = reachable ? false : `live API server not reachable at ${apiBase}`;

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
