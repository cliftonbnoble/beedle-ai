import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const sharedRoot = process.cwd();

test("index code catalog is stored as JSON behind a small typed wrapper", async () => {
  const wrapper = await fs.readFile(path.resolve(sharedRoot, "src/index-codes.ts"), "utf8");
  const raw = await fs.readFile(path.resolve(sharedRoot, "src/index-codes.json"), "utf8");
  const data = JSON.parse(raw);

  assert.match(wrapper, /import indexCodeOptions from "\.\/index-codes\.json"/);
  assert.match(wrapper, /export type CanonicalIndexCodeOption/);
  assert.match(wrapper, /canonicalIndexCodeOptions: readonly CanonicalIndexCodeOption\[\] = indexCodeOptions/);
  assert.ok(data.length > 600);
  assert.deepEqual(Object.keys(data[0]).sort(), ["code", "description", "ordinance", "rules"]);
  assert.ok(wrapper.split("\n").length < 30);
});
