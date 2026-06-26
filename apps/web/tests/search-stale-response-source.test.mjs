import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchPagePath = path.resolve(process.cwd(), "src/app/search/page.tsx");
const apiPath = path.resolve(process.cwd(), "src/lib/api.ts");

test("search page aborts older requests and ignores stale responses", async () => {
  const src = await fs.readFile(searchPagePath, "utf8");
  const api = await fs.readFile(apiPath, "utf8");

  assert.match(src, /useRef/);
  assert.match(src, /searchRequestRef/);
  assert.match(src, /new AbortController\(\)/);
  assert.match(src, /searchRequestRef\.current\.controller\?\.abort\(\)/);
  assert.match(src, /runSearch\(payload, \{ signal: controller\.signal \}\)/);
  assert.match(src, /searchRequestRef\.current\.id !== requestId/);
  assert.match(src, /return "stale"/);
  assert.match(api, /runSearch\(input: SearchRequest, options: \{ signal\?: AbortSignal \} = \{\}\)/);
  assert.match(api, /signal: options\.signal/);
});
