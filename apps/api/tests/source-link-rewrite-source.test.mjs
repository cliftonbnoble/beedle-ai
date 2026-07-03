import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

// SRC-02 guard: effectiveSourceLink(env, documentId, persistedLink) rewrites the persisted
// `example.invalid` sentinel to `<proxy>/source/<documentId>`. Passing anything other than the document
// id as the second argument (the audit found the R2 key passed there) produces a link that can never
// match the single-segment `/source/:documentId` route — every rendered link 404s. Pin every call site
// to a documentId-shaped argument.
test("every effectiveSourceLink call site passes the document id, not the R2 key", async () => {
  const services = path.resolve(process.cwd(), "src/services");
  const files = (await fs.readdir(services)).filter((f) => f.endsWith(".ts"));

  const callSites = [];
  for (const file of files) {
    const src = await fs.readFile(path.join(services, file), "utf8");
    for (const match of src.matchAll(/effectiveSourceLink\(env,\s*([^,]+),/g)) {
      callSites.push({ file, arg: match[1].trim() });
    }
  }

  assert.ok(callSites.length >= 3, `expected at least the 3 known call sites, found ${callSites.length}`);
  for (const { file, arg } of callSites) {
    assert.match(
      arg,
      /(^|\.)(id|documentId)$/,
      `${file}: effectiveSourceLink must receive the document id, got \`${arg}\` (sourceFileRef/R2 keys produce dead /source/ links)`
    );
  }
});
