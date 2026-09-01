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

test("primary search filters share a symmetric row and stack responsively", async () => {
  const [src, css] = await Promise.all([
    fs.readFile(searchPagePath, "utf8"),
    fs.readFile(globalsPath, "utf8")
  ]);

  assert.match(src, /<div className="search-filter-bar">/);
  assert.match(src, /search-filter-card search-filter-card--limit/);
  assert.match(src, /<div\s+className="search-filter-card"/);
  assert.match(src, /search-filter-card search-series-filter/);
  assert.match(css, /\.search-filter-bar\s*\{[\s\S]*grid-template-columns:[\s\S]*minmax\(155px, 0\.75fr\)[\s\S]*minmax\(205px, 1fr\)/);
  assert.match(css, /\.search-filter-card\s*\{[\s\S]*min-height: 116px;/);
  assert.match(css, /\.search-filter-card__action\s*\{[\s\S]*margin-top: auto;/);
  assert.match(css, /@media \(max-width: 1200px\)[\s\S]*\.search-filter-bar\s*\{[\s\S]*repeat\(2, minmax\(0, 1fr\)\)/);
  assert.match(css, /@media \(max-width: 640px\)[\s\S]*\.search-filter-bar\s*\{[\s\S]*minmax\(0, 1fr\)/);
});

test("index code and judge filters visibly default to all while requests stay unrestricted", async () => {
  const src = await fs.readFile(searchPagePath, "utf8");

  assert.match(src, /const ALL_SEARCH_INDEX_CODES = SEARCH_INDEX_CODE_OPTIONS\.map/);
  assert.match(src, /initialIndexCodes\.length > 0 \? initialIndexCodes : \[\.\.\.ALL_SEARCH_INDEX_CODES\]/);
  assert.match(src, /initialJudgeNames\.length > 0 \? initialJudgeNames : \[\.\.\.canonicalJudgeNames\]/);
  assert.match(src, /`All \$\{dedupedIndexCodeOptions\.length\} selected`/);
  assert.match(src, /`All \$\{canonicalJudgeNames\.length\} selected`/);
  assert.match(src, /const hasSearchableFilterSelection = indexCodes\.length > 0 && judgeNames\.length > 0;/);
  assert.match(src, /Select at least one index code and one judge to search\./);
  assert.match(src, /indexCodes\.length > 0 && indexCodes\.length < dedupedIndexCodeOptions\.length \? indexCodes : \[\]/);
  assert.match(src, /judgeNames\.length > 0 && judgeNames\.length < canonicalJudgeNames\.length \? judgeNames : \[\]/);
});

test("decision series uses an accessible reversible segmented control", async () => {
  const [src, css] = await Promise.all([
    fs.readFile(searchPagePath, "utf8"),
    fs.readFile(globalsPath, "utf8")
  ]);

  assert.match(src, /type DecisionSeriesFilter = "both" \| "T" \| "L";/);
  assert.match(src, /<legend>Decision series<\/legend>/);
  assert.match(src, /type="radio"/);
  assert.match(src, /name="decision-series"/);
  assert.match(src, /\["both", "Both"\]/);
  assert.match(src, /decisionSeries: decisionSeries === "both" \? undefined : decisionSeries/);
  assert.match(src, /if \(filters\.decisionSeries !== "both"\) params\.set\("decisionSeries", filters\.decisionSeries\)/);
  assert.match(src, /search-series-filter__options is-\$\{decisionSeries\.toLowerCase\(\)\}/);
  assert.match(css, /\.search-series-filter__options::before\s*\{[\s\S]*transition: transform/);
  assert.match(css, /\.search-series-filter__options\.is-t::before/);
  assert.match(css, /\.search-series-filter__options\.is-l::before/);
});
