import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const legalReferencesPath = path.resolve(process.cwd(), "src/services/legal-references.ts");

test("legal reference normalizers avoid unsafe raw prefix stripping", async () => {
  const src = await fs.readFile(legalReferencesPath, "utf8");

  assert.match(src, /import.*citationKey.*legal-citations/);
  assert.match(src, /return citationIdentity\(input, "ordinance"\)/);
  assert.match(src, /return citationIdentity\(input, "rule"\)/);
  assert.match(src, /replace\(\/\^ic\(\?:\[\\s_-\]\+\|\(\?=\\d\)\)/);
  assert.doesNotMatch(src, /\.replace\(\/\^section/);
  assert.doesNotMatch(src, /\.replace\(\/\^sec/);
  assert.doesNotMatch(src, /\.replace\(\/\^rule/);
  assert.doesNotMatch(src, /\.replace\(\/\^\[ivxlcdm\]\+\\-/);
});
