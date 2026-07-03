import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const sharedSrc = path.resolve(process.cwd(), "../../packages/shared/src");
const wrapperPath = path.join(sharedSrc, "index-codes.ts");
const jsonPath = path.join(sharedSrc, "index-codes.json");

// REPO-02: the index-code catalog must stay data (JSON) loaded by a thin typed wrapper, not a
// multi-thousand-line TypeScript literal. This guards the wrapper shape and validates the JSON
// catalog (which the wrapper casts without runtime validation), so a malformed entry is caught.
test("index-code catalog is JSON loaded by a thin typed wrapper", async () => {
  const wrapper = await fs.readFile(wrapperPath, "utf8");

  assert.match(wrapper, /import indexCodeOptions from "\.\/index-codes\.json"/);
  assert.match(wrapper, /export const canonicalIndexCodeOptions: readonly CanonicalIndexCodeOption\[\]/);
  // The wrapper must stay small — no inline catalog literal.
  const lineCount = wrapper.split("\n").length;
  assert.ok(lineCount < 40, `index-codes.ts should be a thin wrapper, found ${lineCount} lines`);
  assert.doesNotMatch(wrapper, /\{\s*code:\s*"/, "catalog data must live in JSON, not inline in the wrapper");
});

test("index-code catalog JSON is a non-empty array of well-formed entries", async () => {
  const raw = await fs.readFile(jsonPath, "utf8");
  const data = JSON.parse(raw);

  assert.ok(Array.isArray(data), "catalog must be a JSON array");
  assert.ok(data.length > 50, `expected a substantial catalog, found ${data.length} entries`);

  for (const entry of data) {
    assert.equal(typeof entry, "object");
    for (const key of ["code", "description", "ordinance", "rules"]) {
      assert.equal(typeof entry[key], "string", `entry ${JSON.stringify(entry)} missing string "${key}"`);
    }
    assert.ok(entry.code.length > 0, "entry code must be non-empty");
  }

  const codes = data.map((entry) => entry.code);
  assert.equal(new Set(codes).size, codes.length, "catalog codes must be unique");
});
