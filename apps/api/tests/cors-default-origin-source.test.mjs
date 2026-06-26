import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const indexPath = path.resolve(process.cwd(), "src/index.ts");

test("CORS defaults to known app origins instead of wildcard", async () => {
  const src = await fs.readFile(indexPath, "utf8");

  assert.match(src, /const defaultAllowedCorsOrigins = \[/);
  assert.match(src, /http:\/\/localhost:5555/);
  assert.match(src, /http:\/\/127\.0\.0\.1:5555/);
  assert.match(src, /https:\/\/beedle-ai\.pages\.dev/);
  assert.match(src, /const allowedOrigins = configuredOrigins\.length > 0 \? configuredOrigins : defaultAllowedCorsOrigins/);
  assert.match(src, /allowedOrigins\.includes\(normalizedOrigin\)/);
  assert.doesNotMatch(src, /access-control-allow-origin"\] = "\*"/);
});
