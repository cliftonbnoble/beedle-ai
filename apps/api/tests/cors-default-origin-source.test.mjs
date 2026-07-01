import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const indexPath = path.resolve(process.cwd(), "src/index.ts");

test("CORS fails closed: the allowlist is config-only with no hardcoded fallback", async () => {
  const src = await fs.readFile(indexPath, "utf8");

  // The allowlist comes ONLY from CORS_ALLOWED_ORIGINS — no fallback to a baked-in origin list.
  assert.match(src, /const allowedOrigins = parseAllowedOrigins\(env\.CORS_ALLOWED_ORIGINS\);/);
  assert.doesNotMatch(src, /defaultAllowedCorsOrigins/, "no hardcoded default origin list (fail closed when unset)");
  // A hardcoded prod origin in source is the footgun this removes.
  assert.doesNotMatch(src, /=\s*\[[^\]]*pages\.dev/, "prod origin must come from config, not source");

  // An origin is echoed back only if it is an explicit member of the allowlist, and never as a wildcard.
  assert.match(src, /if \(normalizedOrigin && allowedOrigins\.includes\(normalizedOrigin\)\)/);
  assert.doesNotMatch(src, /access-control-allow-origin"\] = "\*"/);

  // Origins are still normalized before comparison.
  assert.match(src, /function parseAllowedOrigins\(input: string \| undefined\): string\[\]/);
  assert.match(src, /function normalizeOrigin\(/);
});
