import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("costly POST routes enforce a Cloudflare rate-limit binding before routing", async () => {
  const root = process.cwd();
  const [worker, config, env] = await Promise.all([
    fs.readFile(path.resolve(root, "src/index.ts"), "utf8"),
    fs.readFile(path.resolve(root, "wrangler.toml"), "utf8"),
    fs.readFile(path.resolve(root, "src/lib/types.ts"), "utf8")
  ]);

  for (const binding of ["SEARCH_RATE_LIMIT", "INGEST_RATE_LIMIT", "LLM_RATE_LIMIT", "ADMIN_WRITE_RATE_LIMIT"]) {
    assert.match(config, new RegExp(`name = "${binding}"`));
    assert.match(env, new RegExp(`${binding}: RateLimit`));
  }
  assert.match(worker, /const limited = await enforceCostControls\(request, env, auth\.user\)/);
  assert.match(worker, /path === "\/api\/assistant\/chat"/);
  assert.match(worker, /path === "\/ingest\/decision-upload"/);
  assert.match(worker, /path === "\/admin\/retrieval\/debug"/);
  assert.match(worker, /status: 429, headers: \{ "retry-after"/);
  assert.match(worker, /status: 503, headers: \{ "retry-after"/);
  assert.match(worker, /control\.limiter\.limit\(\{ key: authActorKey/);
});
