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
  assert.match(src, /function runtimeManualCandidateSqlPrefilterClause\(\)/);
  assert.match(src, /BETWEEN 1 AND 2/);
  assert.match(src, /if \(options\.runtimeManualCandidatesOnly\) \{\s*where\.push\(runtimeManualCandidateSqlPrefilterClause\(\)\)/);
  assert.match(src, /function approvalReadySqlPrefilterClause\(\)/);
  assert.match(src, /d\.qc_required_confirmed = 1/);
  assert.match(src, /d\.approved_at IS NULL/);
  assert.match(src, /WHEN \$\{limitedPilotConfirmed\} THEN 5/);
  assert.match(src, /if \(options\.approvalReadyOnly\) \{\s*where\.push\(approvalReadySqlPrefilterClause\(\)\)/);
  assert.match(src, /function approvalBlockerSqlPrefilterClause\(blocker: string \| undefined\)/);
  assert.match(src, /case "metadata_not_confirmed":\s*return "COALESCE\(d\.qc_required_confirmed, 0\) = 0"/);
  assert.match(src, /case "unresolved_references_above_threshold":\s*return `\$\{unresolvedReferenceCount\} > \$\{approvalUnresolvedThresholdSqlExpr\(\)\}`/);
  assert.match(src, /const blockerSqlPrefilter = approvalBlockerSqlPrefilterClause\(options\.blocker\)/);
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
  assert.match(src, /filtered = filtered\.filter\(\(item\) => item\.approvalReadiness\.eligible\)/);
  assert.match(src, /filtered = filtered\.filter\(\(item\) => item\.approvalReadiness\.blockers\.includes\(options\.blocker as string\)\)/);
  assert.match(src, /filtered = filtered\.filter\(\(item\) => item\.runtimeSurfaceForManualReview\)/);
});
