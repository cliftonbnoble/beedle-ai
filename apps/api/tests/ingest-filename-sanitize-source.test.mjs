import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestPath = path.resolve(process.cwd(), "src/services/ingest.ts");

// API-05: the uploaded filename is user input that becomes an R2 key segment and part of the persisted
// source URL. It must pass through the charset/length sanitizer before key construction — `/`, `..`,
// `?`, `#`, `%`, control chars, or multi-KB names would otherwise escape the `fileType/date/` key layout
// and persist permanently-broken links.
test("ingest sanitizes the user filename before building the R2 source key", async () => {
  const src = await fs.readFile(ingestPath, "utf8");

  assert.match(src, /function sanitizeSourceFilenameSegment\(filename: string\): string/);
  assert.match(src, /\.replace\(\/\[\^a-zA-Z0-9\._-\]\+\/g, "_"\)/);
  assert.match(src, /\.slice\(0, 120\)/);
  // The key template consumes the sanitized segment, never the raw filename.
  assert.match(src, /crypto\.randomUUID\(\)\}-\$\{sanitizeSourceFilenameSegment\(parsedInput\.sourceFile\.filename\)\}/);
  assert.doesNotMatch(src, /crypto\.randomUUID\(\)\}-\$\{parsedInput\.sourceFile\.filename\}/);
});
