import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

// NS-09: /\bheat|heating|hot water\b/ anchors ONLY the first alternative's start and the last's end —
// the middle alternatives match INSIDE other words ("theater" matched `heater`, "bleak" matched
// `leak`), mis-routing queries into the wrong issue scope with vector search disabled. The fix wraps
// the alternation: /\b(?:heat|heating|hot water\b)/. This guard fails if the broken idiom reappears
// in the query-classification/analysis lexicons.
const files = [
  "src/services/search-query-analysis.ts",
  "src/services/search-query-classification.ts"
];

// A regex literal that starts /\b, immediately continues with a bare word (no group), and carries a
// top-level alternation of bare words — the exact idiom the NS-09 codemod eliminated.
const brokenIdiom = /\/\\b[a-z][a-z0-9 .-]*\|[a-z0-9 .|-]*(?:\\b)?\/[a-z]*/g;

test("issue-scope alternation regexes anchor every alternative (NS-09)", async () => {
  for (const file of files) {
    const source = await fs.readFile(path.resolve(process.cwd(), file), "utf8");
    const offenders = source.match(brokenIdiom) || [];
    assert.deepEqual(
      offenders,
      [],
      `${file}: unanchored alternation regex(es) found — wrap as /\\b(?:...)/ so every alternative is word-anchored:\n  ${offenders.join("\n  ")}`
    );
  }
});
