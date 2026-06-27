import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const servicePath = path.resolve(process.cwd(), "src/services/admin-ingestion.ts");

test("admin ingestion list over-fetches before derived filters and returns requested page size", async () => {
  const src = await fs.readFile(servicePath, "utf8");

  assert.match(src, /function usesDerivedListFilter\(options: ListIngestionDocumentsOptions\)/);
  assert.match(src, /function usesDerivedListSort\(sort: ListIngestionDocumentsOptions\["sort"\]\)/);
  assert.match(src, /function likelyFixtureSqlExclusionClause\(\)/);
  assert.match(src, /if \(options\.realOnly\) \{\s*where\.push\(likelyFixtureSqlExclusionClause\(\)\)/);
  assert.match(src, /const requiresDerivedProcessing = usesDerivedListFilter\(options\) \|\| usesDerivedListSort\(options\.sort\)/);
  assert.match(src, /const sqlLimit = requiresDerivedProcessing/);
  assert.match(src, /\.bind\(\.\.\.binds, sqlLimit\)/);
  assert.match(src, /const candidateRows = rows\.results \?\? \[\]/);
  assert.match(src, /const returnedDocuments = filtered\.slice\(0, limit\)/);
  assert.match(src, /const derivedCandidatePoolExhausted = requiresDerivedProcessing && candidateRows\.length >= sqlLimit/);
  assert.match(src, /const derivedCandidatePoolLimited = derivedCandidatePoolExhausted && filtered\.length >= limit/);
  assert.match(src, /documents: returnedDocuments/);
  assert.match(src, /candidatePoolSize: candidateRows\.length/);
  assert.match(src, /derivedProcessingApplied: requiresDerivedProcessing/);
  assert.match(src, /derivedCandidatePoolLimited/);
  assert.doesNotMatch(src, /documents: filtered/);
});
