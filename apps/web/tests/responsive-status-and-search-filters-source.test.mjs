import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const globalsPath = path.resolve(process.cwd(), "src/app/globals.css");
const searchPagePath = path.resolve(process.cwd(), "src/app/search/page.tsx");

test("status pills keep their labels separate and wrap safely with page headers", async () => {
  const css = await fs.readFile(globalsPath, "utf8");

  assert.match(css, /\.page-hero\s*\{[\s\S]*flex-wrap: wrap;/);
  assert.match(css, /\.status-pill,\s*\n\.pill\s*\{[\s\S]*flex: 0 0 auto;/);
  assert.match(css, /\.status-pill,\s*\n\.pill\s*\{[\s\S]*white-space: nowrap;/);
  assert.match(css, /\.status-pill__label\s*\{[\s\S]*white-space: nowrap;/);
});

test("advanced search controls are hidden behind one reversible UI flag", async () => {
  const src = await fs.readFile(searchPagePath, "utf8");

  assert.match(src, /const SHOW_ADVANCED_SEARCH_FILTERS = false;/);
  assert.match(src, /\{SHOW_ADVANCED_SEARCH_FILTERS \? \(/);
  assert.match(src, /rulesSection: rulesSection \|\| undefined/);
  assert.match(src, /ordinanceSection: ordinanceSection \|\| undefined/);
  assert.match(src, /partyName: partyName \|\| undefined/);
  assert.match(src, /fromDate: fromDate \|\| undefined/);
  assert.match(src, /toDate: toDate \|\| undefined/);
  assert.match(src, /Index code filter/);
  assert.match(src, /Judge filter/);
});

test("the remaining search filters stack on compact screens", async () => {
  const src = await fs.readFile(searchPagePath, "utf8");

  assert.match(
    src,
    /gridTemplateColumns: isCompactResultsLayout \? "minmax\(0, 1fr\)" : "minmax\(0, 2fr\) minmax\(320px, 1fr\)"/
  );
});
