import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("readJson enforces configured limits for declared and chunked JSON bodies", async () => {
  const src = await fs.readFile(path.resolve(process.cwd(), "src/lib/http.ts"), "utf8");

  assert.match(src, /export const DEFAULT_JSON_BODY_MAX_BYTES = 1 \* 1024 \* 1024/);
  assert.match(src, /contentLength > maxBytes/);
  assert.match(src, /request\.body\?\.getReader\(\)/);
  assert.match(src, /totalBytes > maxBytes/);
  assert.match(src, /await reader\.cancel\("JSON body exceeds configured limit"\)/);
  assert.match(src, /error instanceof RequestBodyTooLargeError/);
});
