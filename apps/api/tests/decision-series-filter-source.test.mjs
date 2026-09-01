import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const sharedSchemaPath = path.resolve(process.cwd(), "../../packages/shared/src/index.ts");
const searchQueryAnalysisPath = path.resolve(process.cwd(), "src/services/search-query-analysis.ts");

test("decision series is a bounded structured search filter", async () => {
  const [shared, search] = await Promise.all([
    fs.readFile(sharedSchemaPath, "utf8"),
    fs.readFile(searchQueryAnalysisPath, "utf8")
  ]);

  assert.match(shared, /decisionSeries: z\.enum\(\["T", "L"\]\)\.optional\(\)/);
  assert.match(search, /if \(parsed\.filters\.decisionSeries\)/);
  assert.match(search, /coalesce\(d\.citation, ''\)/);
  assert.match(search, /coalesce\(d\.case_number, ''\)/);
  assert.match(search, /coalesce\(d\.title, ''\)/);
  assert.match(search, /params\.push\(parsed\.filters\.decisionSeries\)/);
  assert.match(search, /if \(filters\.decisionSeries\) kinds\.push\("decision_series"\)/);
});
