# Beedle AI Companion - Phase 6A Controlled Corpus Expansion

Cloudflare-first monorepo with:

- ingestion + QC admin workflow
- grounded search
- Case Assistant
- pilot corpus import/evaluation
- retrieval diagnostics and ranking tuning loop
- grounded Conclusions of Law drafting

Deployment guide:

- [/Users/cliftonnoble/Documents/Beedle AI App/docs/cloudflare-deployment-runbook.md](/Users/cliftonnoble/Documents/Beedle%20AI%20App/docs/cloudflare-deployment-runbook.md)

## Key additions (through Phase 6A)

- `POST /api/draft/conclusions` endpoint
- `/drafting` UI for Conclusions of Law generation
- retrieval-first drafting response model with paragraph-level support mapping
- drafting smoke + groundedness + confidence regression tests
- local source proxy route (`GET /source/:documentId`) for local source-link review
- web dev server now runs on `localhost:5555`
- draft debug endpoint for support inspection (`POST /admin/draft/debug`)
- drafting evaluation harness (`pnpm eval:drafting`)
- template drafting endpoint (`POST /api/draft/template`)
- case-type scaffold modes (`blank_scaffold`, `guided_scaffold`, `lightly_contextualized`)
- template evaluation harness (`pnpm eval:template`)
- export endpoint (`POST /api/draft/export`) for markdown/text/html payloads
- copy/export review actions in `/drafting` for full draft/template and per-section copy
- printable HTML workflow for conclusions and template outputs
- validated taxonomy/config schema model for case types and canonical sections
- explicit id/alias/fallback case-type resolution behavior
- read-only config inspection endpoint (`GET /admin/config/taxonomy`)
- config validation endpoint (`POST /admin/config/taxonomy/validate`)
- config resolver endpoint (`POST /admin/config/taxonomy/resolve`)
- `/admin/config` UI for read-only taxonomy inspection
- controlled broader-batch import controls (`BATCH_LIMIT`, `BATCH_OFFSET`, `PILOT_RECURSIVE`, `PILOT_DRY_RUN`)
- corpus quality report script (`pnpm eval:corpus-quality`)
- expanded drafting/template safety validation script (`pnpm eval:expanded-safety`)
- admin ingestion list filters/sorting/status summary for larger-batch QC review
- taxonomy suggestion visibility in ingestion list/detail responses and QC UI
- normalized legal-reference foundation for Index Codes, Ordinance sections, and Rules sections
- legal-reference rebuild + backfill scripts and admin inspection endpoint
- ingestion validation against normalized references with unmatched-reference issue tracking

## Local setup

1. Install deps

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App
pnpm install
```

2. Apply D1 migrations

```bash
cd apps/api
pnpm wrangler d1 migrations apply beedle --local
```

3. Start API (terminal A)

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
pnpm dev
```

If you run API on a different port, set source proxy URL explicitly:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
pnpm wrangler dev --local --port 8799 --var SOURCE_PROXY_BASE_URL:http://127.0.0.1:8799
```

4. Start web (terminal B, on port 5555)

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/web
pnpm dev
```

## Pilot import workflow

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
PILOT_DIR="/absolute/path/to/pilot-docx-folder" \
PILOT_JURISDICTION="City of Beedle" \
API_BASE_URL=http://127.0.0.1:8787 \
pnpm import:pilot
```

Controlled broader-batch options:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
PILOT_DIR="/absolute/path/to/pilot-docx-folder" \
PILOT_JURISDICTION="City of Beedle" \
API_BASE_URL=http://127.0.0.1:8787 \
BATCH_LIMIT=120 \
BATCH_OFFSET=0 \
PILOT_RECURSIVE=1 \
pnpm import:pilot
```

Second pilot batch (example with offset + tagged report):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
PILOT_DIR="/absolute/path/to/pilot-docx-folder" \
PILOT_JURISDICTION="City of Beedle" \
API_BASE_URL=http://127.0.0.1:8787 \
BATCH_LIMIT=120 \
BATCH_OFFSET=120 \
PILOT_RECURSIVE=1 \
PILOT_LABEL="pilot_batch_2" \
PILOT_REPORT_NAME="pilot-import-report-2.json" \
pnpm import:pilot
```

Dry-run file selection without ingesting:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
PILOT_DIR="/absolute/path/to/pilot-docx-folder" \
BATCH_LIMIT=120 \
BATCH_OFFSET=0 \
PILOT_RECURSIVE=1 \
PILOT_DRY_RUN=1 \
pnpm import:pilot
```

Import report:

- `apps/api/reports/pilot-import-report.json`
- report now includes:
- `staged`, `searchable`, `approved`
- `warning_count`, `unresolved_reference_count`, `critical_exception_count`
- `filtered_noise_count`, `low_confidence_taxonomy_count`, `avg_extraction_confidence`
- per-document `critical_exception_references`

Pilot reprocessing after extraction cleanup:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
REPROCESS_STATUS=staged \
REPROCESS_DECISION_ONLY=1 \
REPROCESS_LIMIT=200 \
REPROCESS_REPORT_NAME="pilot-reprocess-report-2.json" \
pnpm reprocess:pilot
```

Reprocess report:

- `apps/api/reports/pilot-reprocess-report.json`
- includes `before`/`after`/`delta` for warnings, unresolved references, filtered noise, and low-confidence taxonomy

Compare first vs second pilot quality:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
PILOT_A_REPORT="./reports/pilot-import-report.json" \
PILOT_B_REPORT="./reports/pilot-import-report-2.json" \
PILOT_COMPARISON_REPORT_NAME="pilot-comparison-report.json" \
pnpm compare:pilot-batches
```

## QC admin review

- [http://localhost:5555/admin/ingestion](http://localhost:5555/admin/ingestion)

For each document:

- inspect extracted metadata + warnings
- inspect section detection + paragraph anchors
- inspect chunk boundaries
- correct metadata if needed
- confirm required metadata
- approve/reject

Larger-batch review filters available:

- status: all/staged/searchable/approved/rejected/pending
- file type: decision/law
- warnings only
- missing required QC metadata only
- unresolved references only
- critical-reference exceptions only (`37.2(g)`, `37.15`, `10.10(c)(3)`)
- filtered-noise docs only
- low-confidence taxonomy only
- missing rules detection only
- missing ordinance detection only
- title/citation/case-number search
- sort by created date/title/extraction confidence/warning count/unresolved refs/critical exceptions

API filter examples:

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&unresolvedReferencesOnly=1&sort=unresolvedReferenceDesc&limit=200" | jq '.summary, (.documents[0] // {})'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?criticalExceptionsOnly=1&sort=criticalExceptionDesc&limit=200" | jq '.summary, (.documents[0] // {})'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&sort=warningCountDesc&limit=50" | jq '.summary, (.documents[0:10])'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&filteredNoiseOnly=1&sort=warningCountDesc&limit=100" | jq '.summary, (.documents[0:10] | map({id,title,filteredNoiseCount,warningCount}))'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&lowConfidenceTaxonomyOnly=1&sort=confidenceAsc&limit=100" | jq '.summary, (.documents[0:10] | map({id,title,extractionConfidence,lowConfidenceTaxonomy,taxonomySuggestion}))'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&missingRulesOnly=1&missingOrdinanceOnly=1&sort=unresolvedReferenceDesc&limit=100" | jq '.summary, (.documents[0:10] | map({id,title,missingRulesDetection,missingOrdinanceDetection,unresolvedReferenceCount}))'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&approvalReadyOnly=1&sort=approvalReadinessDesc&limit=100" | jq '.summary, (.documents[0:10] | map({id,title,approvalReadiness,warningCount,unresolvedReferenceCount,criticalExceptionCount}))'
```

Inspect flagged references on one document:

```bash
DOC_ID="doc_xxx"
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents/$DOC_ID" | jq '.criticalExceptionReferences, .unresolvedReferenceCount, .referenceIssues[0:10]'
```

## Selective Approval Rollout

Blocker breakdown for staged docs:

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&sort=approvalReadinessDesc&limit=400" | jq '.summary.blockerBreakdown, .summary.approvalReady'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=400" | jq '.summary.blockerBreakdown, .summary.realApprovalReady'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=50" | jq '(.documents[0:20] | map({id,title,score:.approvalReadiness.score,blockers:.approvalReadiness.blockers,failedQcRequirements,unresolvedReferenceCount,warningCount,extractionConfidence}))'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&blocker=metadata_not_confirmed&sort=approvalReadinessDesc&limit=200" | jq '.summary, (.documents[0:15] | map({id,title,approvalReadiness,failedQcRequirements}))'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&sort=approvalReadinessDesc&limit=400" | jq '.summary, (.documents | map(select(.failedQcRequirements | index("missing_index_codes"))) | .[0:20] | map({id,title,failedQcRequirements,rules: .missingRulesDetection,ordinance: .missingOrdinanceDetection,unresolvedReferenceCount,approvalReadiness}))'
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&approvalReadyOnly=1&sort=approvalReadinessDesc&limit=200" | jq '.summary.realApprovalReady, (.documents | map(select(.isLikelyFixture == false)) | .[0:20] | map({id,title,approvalReadiness,unresolvedReferenceCount,warningCount}))'
```

Conservative batch metadata confirmation (safe candidates only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
CONFIRM_DRY_RUN=1 \
CONFIRM_LIMIT=40 \
CONFIRM_REPROCESS_FIRST=1 \
CONFIRM_REPORT_NAME="pilot-metadata-confirm-report.json" \
pnpm confirm:pilot-metadata
```

Apply metadata confirmation:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
CONFIRM_DRY_RUN=0 \
CONFIRM_LIMIT=10 \
CONFIRM_REPROCESS_FIRST=1 \
CONFIRM_REPORT_NAME="pilot-metadata-confirm-report.json" \
pnpm confirm:pilot-metadata
```

Reviewer-assisted readiness pass (Phase 6A.14, dry-run first):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_DRY_RUN=1 \
REVIEWER_LIST_LIMIT=250 \
REVIEWER_CONFIRM_LIMIT=12 \
REVIEWER_REPORT_NAME="reviewer-readiness-report.json" \
pnpm reviewer-readiness:pilot
```

Apply bounded metadata confirmation for confirmation-only real docs:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_DRY_RUN=0 \
REVIEWER_LIST_LIMIT=250 \
REVIEWER_CONFIRM_LIMIT=12 \
REVIEWER_REPROCESS_FIRST=1 \
REVIEWER_REPORT_NAME="reviewer-readiness-report-apply.json" \
pnpm reviewer-readiness:pilot
```

Inspect Phase 6A.14 report buckets:

```bash
jq '.summary, .confirmation_only_candidates[0:20], .confirmation_plus_one_manual_fix_candidates[0:20], .structurally_blocked_docs[0:20]' \
  /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/reviewer-readiness-report.json
```

Reviewer-ready list filter (real staged docs only):

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&reviewerReadyOnly=1&sort=reviewerReadinessDesc&limit=200" \
  | jq '.summary, (.documents[0:25] | map({id,title,reviewerReady,reviewerRiskLevel,metadataConfirmationWouldUnlock,reviewerReadyReasons,unresolvedBlockersAfterConfirmation}))'
```

Phase 6A.15 unresolved reviewer triage (dry-run only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
TRIAGE_LIST_LIMIT=300 \
TRIAGE_TOP_LIMIT=120 \
TRIAGE_REPORT_NAME="staged-real-unresolved-triage-report.json" \
pnpm triage:unresolved-staged-real
```

Inspect triage buckets + recurring citation families:

```bash
jq '.summary, .recurring_citation_families[0:20], .staged_real_docs[0:20]' \
  /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/staged-real-unresolved-triage-report.json
```

Admin triage filters for staged real docs:

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&unresolvedTriageBucket=unsafe_37x_structural_block&sort=unresolvedLeverageDesc&limit=200" \
  | jq '.summary, (.documents[0:20] | map({id,title,unresolvedBuckets,estimatedReviewerEffort,recurringCitationFamilies,canBatchReviewWith,topRecommendedReviewerAction}))'

curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&recurringCitationFamily=37.2&sort=batchabilityDesc&limit=200" \
  | jq '.summary, (.documents[0:20] | map({id,title,unresolvedBuckets,estimatedReviewerEffort,recurringCitationFamilies,canBatchReviewWith}))'
```

Phase 6A.16 blocked 37.x reviewer workbench (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
BLOCKED_37X_LIST_LIMIT=300 \
BLOCKED_37X_TOP_LIMIT=150 \
BLOCKED_37X_REPORT_NAME="blocked-37x-review-workbench-report.json" \
pnpm workbench:blocked-37x
```

Inspect blocked 37.x report:

```bash
jq '.summary, .grouped_by_family, .grouped_by_batch_key[0:20], .docs[0:20]' \
  /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/blocked-37x-review-workbench-report.json
```

Admin blocked 37.x filters:

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&blocked37xOnly=1&sort=blocked37xBatchKeyAsc&limit=200" \
  | jq '.summary, (.documents[0:25] | map({id,title,blocked37xReferences,blocked37xReason,blocked37xReviewerHint,blocked37xSafeToBatchReview,blocked37xBatchKey}))'

curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&blocked37xOnly=1&blocked37xFamily=37.3&sort=blocked37xBatchKeyAsc&limit=200" \
  | jq '.summary, (.documents[0:25] | map({id,title,blocked37xReferences,blocked37xBatchKey}))'
```

Reviewer queue API filters (real staged docs by default in UI):

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&reviewerRiskLevel=high&estimatedReviewerEffort=low&sort=unresolvedLeverageDesc&limit=200" \
  | jq '.summary, (.documents[0:25] | map({id,title,approvalBlockers:.approvalReadiness.blockers,unresolvedReferenceCount,unresolvedBuckets,blocked37xSummary:.blocked37xReferences,estimatedReviewerEffort,reviewerRiskLevel,blocked37xBatchKey,topRecommendedReviewerAction}))'

curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&safeToBatchReviewOnly=1&blocked37xOnly=1&sort=batchabilityDesc&limit=200" \
  | jq '.summary, (.documents[0:25] | map({id,title,blocked37xSafeToBatchReview,blocked37xBatchKey,canBatchReviewWith,topRecommendedReviewerAction}))'
```

Phase 6A.18 reviewer batch export + adjudication prep (read-only):

```bash
# JSON export (current filtered reviewer queue pattern)
curl -sS "http://127.0.0.1:8787/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=1200" \
  -o /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/reviewer-batch-export.json

# CSV export
curl -sS "http://127.0.0.1:8787/admin/ingestion/reviewer-export?realOnly=1&format=csv&limit=1200" \
  -o /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/reviewer-batch-export.csv

# Markdown summary export
curl -sS "http://127.0.0.1:8787/admin/ingestion/reviewer-export?realOnly=1&format=markdown&limit=1200" \
  -o /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/reviewer-batch-summary.md

# Adjudication template (CSV/JSON)
curl -sS "http://127.0.0.1:8787/admin/ingestion/reviewer-adjudication-template?realOnly=1&format=csv&limit=1200" \
  -o /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/reviewer-adjudication-template.csv
curl -sS "http://127.0.0.1:8787/admin/ingestion/reviewer-adjudication-template?realOnly=1&format=json&limit=1200" \
  -o /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/reviewer-adjudication-template.json
```

Phase 6A.27 reviewer batch prioritization report (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_PRIORITY_LIMIT=1200 \
REVIEWER_PRIORITY_REPORT_NAME="reviewer-priority-report.json" \
REVIEWER_PRIORITY_MARKDOWN_NAME="reviewer-priority-report.md" \
pnpm reviewer-priority-report

cat "./reports/reviewer-priority-report.json" | jq '.summary, .top10Batches, .largestLowEffortBatches, .blocked37xBatches, .legalContextBatches'
cat "./reports/reviewer-priority-report.md"
```

Phase 6A.28 blocked legal reviewer packets (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_LEGAL_PACKET_LIMIT=1200 \
REVIEWER_LEGAL_PACKET_REPORT_NAME="reviewer-legal-packets.json" \
REVIEWER_LEGAL_PACKET_MARKDOWN_NAME="reviewer-legal-packets.md" \
pnpm reviewer-legal-packets

cat "./reports/reviewer-legal-packets.json" | jq '.summary, .packets[0:5]'
cat "./reports/reviewer-legal-packets.md"
```

Phase 6A.29 blocked legal evidence packets (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_LEGAL_EVIDENCE_LIMIT=1200 \
REVIEWER_LEGAL_EVIDENCE_REPORT_NAME="reviewer-legal-evidence.json" \
REVIEWER_LEGAL_EVIDENCE_MARKDOWN_NAME="reviewer-legal-evidence.md" \
pnpm reviewer-legal-evidence

cat "./reports/reviewer-legal-evidence.json" | jq '.summary, .packets[0:5]'
cat "./reports/reviewer-legal-evidence.md"
```

Phase 6A.30 reviewer decision simulation (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# Option A: consume generated legal evidence report
REVIEWER_DECISION_SIM_INPUT="./reports/reviewer-legal-evidence.json" \
REVIEWER_DECISION_SIM_REPORT_NAME="reviewer-decision-sim.json" \
REVIEWER_DECISION_SIM_MARKDOWN_NAME="reviewer-decision-sim.md" \
pnpm reviewer-decision-sim

# Option B: build from live reviewer export + detail context
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_DECISION_SIM_REPORT_NAME="reviewer-decision-sim-live.json" \
REVIEWER_DECISION_SIM_MARKDOWN_NAME="reviewer-decision-sim-live.md" \
pnpm reviewer-decision-sim

cat "./reports/reviewer-decision-sim.json" | jq '.summary, .tomorrowMorningReviewPlan, .avoidForNow, .large3737SplitRecommendation'
cat "./reports/reviewer-decision-sim.md"
```

Phase 6A.32 split-ready reviewer packets (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# Default: read from existing decision sim + evidence reports
pnpm reviewer-split-packets

# Explicit inputs
REVIEWER_SPLIT_SIM_INPUT="./reports/reviewer-decision-sim.json" \
REVIEWER_SPLIT_EVIDENCE_INPUT="./reports/reviewer-legal-evidence.json" \
REVIEWER_SPLIT_QUEUE_INPUT="./reports/reviewer-batch-export.json" \
REVIEWER_SPLIT_REPORT_NAME="reviewer-split-packets.json" \
REVIEWER_SPLIT_MARKDOWN_NAME="reviewer-split-packets.md" \
pnpm reviewer-split-packets

cat "./reports/reviewer-split-packets.json" | jq '.summary, (.splitBatches[] | {batchKey,docCount,coveredDocCount,uncoveredDocCount,duplicateDocCount,coverageStatus,coverageWarnings,subBucketCounts}), .excludedNonSplitBatches'
cat "./reports/reviewer-split-packets.md"
```

Phase 6A.34 reviewer action queue export (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

REVIEWER_ACTION_QUEUE_SPLIT_INPUT="./reports/reviewer-split-packets.json" \
REVIEWER_ACTION_QUEUE_SIM_INPUT="./reports/reviewer-decision-sim.json" \
REVIEWER_ACTION_QUEUE_EVIDENCE_INPUT="./reports/reviewer-legal-evidence.json" \
REVIEWER_ACTION_QUEUE_REPORT_NAME="reviewer-action-queue.json" \
REVIEWER_ACTION_QUEUE_MARKDOWN_NAME="reviewer-action-queue.md" \
pnpm reviewer-action-queue

cat "./reports/reviewer-action-queue.json" | jq '.summary, .summary.top20DocsToReviewFirst, .reviewFirstQueue[0:20], .reviewAfterQueue[0:20], .holdBlockedQueue[0:20]'
cat "./reports/reviewer-action-queue.md"
```

Phase 6A.35 reviewer worksheet export (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

REVIEWER_WORKSHEET_ACTION_QUEUE_INPUT="./reports/reviewer-action-queue.json" \
REVIEWER_WORKSHEET_EVIDENCE_INPUT="./reports/reviewer-legal-evidence.json" \
REVIEWER_WORKSHEET_SIM_INPUT="./reports/reviewer-decision-sim.json" \
REVIEWER_WORKSHEET_JSON_NAME="reviewer-worksheet.json" \
REVIEWER_WORKSHEET_CSV_NAME="reviewer-worksheet.csv" \
REVIEWER_WORKSHEET_MARKDOWN_NAME="reviewer-worksheet.md" \
pnpm reviewer-worksheet-export

cat "./reports/reviewer-worksheet.json" | jq '.summary, .summary.top20WorksheetRows, .reviewFirstRows[0:20], .reviewAfterRows[0:20], .holdBlockedRows[0:20]'
head -n 1 "./reports/reviewer-worksheet.csv"
cat "./reports/reviewer-worksheet.md"
```

Phase 6A.36 reviewer worksheet validation / import-precheck (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

REVIEWER_WORKSHEET_INPUT="./reports/reviewer-worksheet.csv" \
REVIEWER_WORKSHEET_VALIDATE_REPORT_NAME="reviewer-worksheet-validate.json" \
REVIEWER_WORKSHEET_VALIDATE_MARKDOWN_NAME="reviewer-worksheet-validate.md" \
REVIEWER_WORKSHEET_STRICT=1 \
pnpm reviewer-worksheet-validate

cat "./reports/reviewer-worksheet-validate.json" | jq '.summary, .guidance, (.rows[0:20] | map({rowNumber,documentId,validationState,validationReasons,readyForDryRunComparison}))'
cat "./reports/reviewer-worksheet-validate.md"
```

Phase 6A.37 validated reviewer decision comparison pack (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

REVIEWER_DECISION_COMPARE_VALIDATE_INPUT="./reports/reviewer-worksheet-validate.json" \
REVIEWER_DECISION_COMPARE_QUEUE_INPUT="./reports/reviewer-action-queue.json" \
REVIEWER_DECISION_COMPARE_SIM_INPUT="./reports/reviewer-decision-sim.json" \
REVIEWER_DECISION_COMPARE_REPORT_NAME="reviewer-decision-compare.json" \
REVIEWER_DECISION_COMPARE_MARKDOWN_NAME="reviewer-decision-compare.md" \
pnpm reviewer-decision-compare

cat "./reports/reviewer-decision-compare.json" | jq '.summary, .countsByComparisonOutcome, .guidance, .matches[0:20], .divergences[0:20]'
cat "./reports/reviewer-decision-compare.md"
```

Phase 6A.38 conservative reviewer decision autofill + exceptions worksheet (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

pnpm reviewer-decision-autofill

cat "./reports/reviewer-decision-autofill.json" | jq '.summary, .policyCounts, .countsByAutofillDecision, .countsByConfidence, .exceptionRows[0:40]'
cat "./reports/reviewer-decision-autofill.md"

# Validate prefilled worksheet compatibility
REVIEWER_WORKSHEET_INPUT="./reports/reviewer-worksheet-prefilled.csv" \
REVIEWER_WORKSHEET_VALIDATE_REPORT_NAME="reviewer-worksheet-prefilled-validate.json" \
REVIEWER_WORKSHEET_VALIDATE_MARKDOWN_NAME="reviewer-worksheet-prefilled-validate.md" \
REVIEWER_WORKSHEET_STRICT=1 \
pnpm reviewer-worksheet-validate

cat "./reports/reviewer-worksheet-prefilled-validate.json" | jq '.summary, .countsByValidationState, .guidance'
```

Phase 6A.39 reviewer import simulation (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

REVIEWER_IMPORT_SIM_INPUT="./reports/reviewer-worksheet-prefilled-validate.json" \
REVIEWER_IMPORT_SIM_REPORT_NAME="reviewer-import-sim.json" \
REVIEWER_IMPORT_SIM_MARKDOWN_NAME="reviewer-import-sim.md" \
pnpm reviewer-import-sim

cat "./reports/reviewer-import-sim.json" | jq '.summary, .countsByReviewerDecision, .countsByBlocked37xFamily, .countsByBatchKey, .countsByPriorityLane, .specialAttentionRows[0:40]'
cat "./reports/reviewer-import-sim.md"

## Runtime Manual Candidate Verification (Real vs Fixture)

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"

API_BASE_URL=http://127.0.0.1:8787 \
pnpm reviewer-runtime-manual-report

cat "./reports/reviewer-runtime-manual-report.json" \
  | jq '.summary, .countAlignment, .countsByRecurringCitationFamily, .countsByUnresolvedBucket, (.topCandidateDocs[0:20] | map({id,title,runtimeDisposition,runtimeDoNotAutoApply,runtimeManualReasonCode,runtimeManualReasonSummary,runtimeSuggestedOperatorAction,runtimeOperatorReviewSummary,runtimeReviewDiagnostic,unresolvedBuckets,recurringCitationFamilies,isLikelyFixture}))'
cat "./reports/reviewer-runtime-manual-report.md"

# Optional mixed mode (include fixtures/test docs in operational top list)
API_BASE_URL=http://127.0.0.1:8787 \
REVIEWER_RUNTIME_MANUAL_INCLUDE_FIXTURES=1 \
pnpm reviewer-runtime-manual-report
```

Quick live verification paths:

```bash
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&limit=1200" \
  | jq '.summary, (.documents[0:20] | map({id,title,isLikelyFixture,runtimeDisposition,runtimeSurfaceForManualReview,runtimeDoNotAutoApply,runtimeManualReasonCode,runtimeManualReasonSummary,runtimeSuggestedOperatorAction,runtimeOperatorReviewSummary,runtimeReviewDiagnostic,unresolvedBuckets,recurringCitationFamilies}))'

curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&realOnly=1&limit=1200" \
  | jq '.summary, (.documents[0:20] | map({id,title,isLikelyFixture,runtimeDisposition,runtimeSurfaceForManualReview,runtimeDoNotAutoApply,runtimeManualReasonCode,runtimeManualReasonSummary,runtimeSuggestedOperatorAction,runtimeOperatorReviewSummary,runtimeReviewDiagnostic,unresolvedBuckets,recurringCitationFamilies}))'
```

## Fixture Pruning Report (Read-only)

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"

API_BASE_URL=http://127.0.0.1:8787 \
pnpm reviewer-fixture-pruning-report

cat "./reports/reviewer-fixture-pruning-report.json" \
  | jq '.summary, .countsByBucket, .likelyRemovableFixture[0:20], .ambiguousFixtureLike[0:20]'
cat "./reports/reviewer-fixture-pruning-report.md"
```
```

Phase 6A.19 adjudication importer dry-run (read-only simulation):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ADJUDICATION_INPUT="./reports/reviewer-adjudication-template.csv" \
ADJUDICATION_REPORT_NAME="adjudication-dry-run-report.json" \
ADJUDICATION_STRICT=1 \
pnpm adjudication-import:dry-run

cat "./reports/adjudication-dry-run-report.json" | jq '.summary, .counts_by_row_state, .reviewer_completion_checklist, .invalid_rows[0:20], .still_blocked_docs[0:20]'
# Optional contract diagnostics
cat "./reports/adjudication-dry-run-report.json" | jq '.exportContractWarnings, .debug.rowsMissingBatchKeyCount, .debug.rowsMissingBatchKey[0:20]'
# Title mismatch diagnostics
cat "./reports/adjudication-dry-run-report.json" | jq '.reviewer_completion_checklist.rowsWithTitleMismatchDetails'
```

JSON input form:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ADJUDICATION_INPUT="./reports/reviewer-adjudication-template.json" \
ADJUDICATION_REPORT_NAME="adjudication-dry-run-report-json.json" \
ADJUDICATION_STRICT=1 \
pnpm adjudication-import:dry-run
```

Markdown operator summary output (same dry-run, still read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ADJUDICATION_INPUT="./reports/reviewer-adjudication-template.csv" \
ADJUDICATION_REPORT_NAME="adjudication-dry-run-summary.md" \
ADJUDICATION_REPORT_FORMAT=markdown \
ADJUDICATION_STRICT=1 \
pnpm adjudication-import:dry-run
```

Optional canonical title hint fields in JSON/Markdown report:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ADJUDICATION_INPUT="./reports/reviewer-adjudication-template.csv" \
ADJUDICATION_REPORT_NAME="adjudication-dry-run-report.json" \
ADJUDICATION_INCLUDE_CANONICAL_TITLE_HINTS=1 \
ADJUDICATION_STRICT=1 \
pnpm adjudication-import:dry-run
```

Row-state interpretation for dry-run:
- `blank_template_row`: valid template row, not reviewed yet
- `reviewed_*`: reviewer completed row; actionable/support/block status shown in summary
- `invalid_*`: reviewer row needs correction before any future apply workflow

List conservative approval candidates (dry run, no promotion):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=1 \
PROMOTE_LIMIT=10 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-report.json" \
pnpm rollout:pilot-approval
```

Promote top approval-ready staged docs (conservative, bounded):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=0 \
PROMOTE_LIMIT=25 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-report.json" \
pnpm rollout:pilot-approval
```

If no fully-ready candidates are found, allow metadata-confirmation-only promotion for near-ready docs:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=0 \
AUTO_CONFIRM_REQUIRED=0 \
PROMOTE_LIMIT=10 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-report.json" \
pnpm rollout:pilot-approval
```

Rollout report:

- `apps/api/reports/pilot-approval-rollout-report.json`
- includes candidate docs, promoted docs, reasons docs remain staged, approved real-vs-fixture totals, approved-only retrieval checks, promoted real-doc retrieval checks (self-title + content query), and before/after real staged blocker breakdown

Real promoted doc snapshot:

```bash
cat /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/pilot-approval-rollout-report-apply.json | jq '.summary, .promoted_real_docs'
```

Validate search on promoted real docs:

```bash
cat /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api/reports/pilot-approval-rollout-report.json | jq '.summary, .promoted_real_docs, .promoted_real_doc_search_checks'
curl -sS -X POST http://127.0.0.1:8787/search -H 'content-type: application/json' -d '{"query":"variance","limit":10,"filters":{"approvedOnly":true}}' | jq '.total, .results[0:5] | map({documentId,title,citationAnchor,sourceLink})'
```

Phase 6A.7 second conservative recovery pass (bounded):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# 1) Top real candidates and blocker baseline
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=120" \
  | jq '.summary, (.documents[0:25] | map({id: .id, title: .title, score: .approvalReadiness.score, blockers: .approvalReadiness.blockers, failedQcRequirements: .failedQcRequirements, unresolvedReferenceCount: .unresolvedReferenceCount, warningCount: .warningCount, extractionConfidence: .extractionConfidence}))'

# 2) Conservative confirm pass (reprocess-first, top real docs only)
API_BASE_URL=http://127.0.0.1:8787 \
CONFIRM_DRY_RUN=0 \
CONFIRM_LIMIT=12 \
CONFIRM_REPROCESS_FIRST=1 \
CONFIRM_REPORT_NAME="pilot-metadata-confirm-report-pass2.json" \
pnpm confirm:pilot-metadata

# 3) Bounded real-doc rollout pass
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=0 \
PROMOTE_LIMIT=8 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-report-pass2.json" \
pnpm rollout:pilot-approval

# 4) Real-doc-only validation and blocker delta
cat ./reports/pilot-approval-rollout-report-pass2.json \
  | jq '.summary, .before_real_blocker_breakdown, .after_real_blocker_breakdown, .promoted_real_docs, .promoted_real_doc_search_checks'
```

Phase 6A.8 content-query retrieval validation on approved real docs:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# 1) Inspect approved real decision docs
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=approved&fileType=decision_docx&realOnly=1&sort=createdAtDesc&limit=40" \
  | jq '.summary, (.documents[0:20] | map({id,title,warningCount,unresolvedReferenceCount,extractionConfidence}))'

# 2) Inspect chunks + anchors for one approved real doc
DOC_ID="doc_replace_me"
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents/$DOC_ID" \
  | jq '{id,title,sections:(.sections|length),chunks:(.chunks|length),sample_chunks:(.chunks[0:5] | map({sectionLabel,paragraphAnchor,citationAnchor,chunkText:(.chunkText[0:160])}))}'

# 3) Run retrieval-quality validation report (real-doc focused)
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=1 \
PROMOTE_LIMIT=0 \
APPROVED_REAL_CHECK_LIMIT=25 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-report-content-check.json" \
pnpm rollout:pilot-approval

# Optional: reprocess approved real docs after parser/chunk cleanup before rerunning checks
API_BASE_URL=http://127.0.0.1:8787 \
REPROCESS_STATUS=approved \
REPROCESS_DECISION_ONLY=1 \
REPROCESS_LIMIT=60 \
REPROCESS_REPORT_NAME="pilot-reprocess-approved-content-pass.json" \
pnpm reprocess:pilot

# 4) Compare self-find vs content-find vs legal-reference-find
cat ./reports/pilot-approval-rollout-report-content-check.json \
  | jq '.summary, .approved_real_doc_search_checks[0:20], .promoted_real_doc_search_checks'

# 5) Probe-generation regression tests
API_BASE_URL=http://127.0.0.1:8787 pnpm test:rollout-probes
```

Phase 6A.10 conservative staged-real remediation (dry-run then bounded apply):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# 1) Baseline staged real blockers
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=200" \
  | jq '.summary.blockerBreakdown, .summary.realApprovalReady'

# 2) Dry-run conservative remediation candidate classification
API_BASE_URL=http://127.0.0.1:8787 \
REMEDIATE_DRY_RUN=1 \
REMEDIATE_LIMIT=12 \
REMEDIATE_LIST_LIMIT=200 \
REMEDIATE_REPORT_NAME="staged-real-remediation-dryrun.json" \
pnpm remediate:staged-real

# 3) Apply bounded conservative remediation pass
API_BASE_URL=http://127.0.0.1:8787 \
REMEDIATE_DRY_RUN=0 \
REMEDIATE_REPROCESS_FIRST=1 \
REMEDIATE_LIMIT=8 \
REMEDIATE_LIST_LIMIT=200 \
REMEDIATE_REPORT_NAME="staged-real-remediation-apply.json" \
pnpm remediate:staged-real

# 4) Inspect before/after blockers, candidates, and promoted real docs
jq '.summary, .before_blocker_breakdown, .after_blocker_breakdown, .candidates[0:20], .promoted' ./reports/staged-real-remediation-apply.json

# 5) Re-run approved real retrieval quality checks after promotion
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=1 \
PROMOTE_LIMIT=0 \
APPROVED_REAL_CHECK_LIMIT=25 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-report-content-check-post-remediation.json" \
pnpm rollout:pilot-approval

jq '.summary, .approved_real_doc_search_checks[0:20]' ./reports/pilot-approval-rollout-report-content-check-post-remediation.json

# 6) Remediation rule regression tests
pnpm test:staged-remediation
```

Phase 6A.11 staged real blocker forensics (read-only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# 1) Run blocker forensics for top staged real docs
API_BASE_URL=http://127.0.0.1:8787 \
FORENSICS_TOP_LIMIT=25 \
FORENSICS_LIST_LIMIT=200 \
FORENSICS_REPORT_NAME="staged-real-blocker-forensics-report.json" \
pnpm forensics:staged-real

# 2) Inspect root-cause aggregates + reviewer unlockability
jq '.summary, .aggregate' ./reports/staged-real-blocker-forensics-report.json

# 3) Inspect top docs with next actions
jq '.docs[0:25] | map({id,title,score,blockerCategory,blockers,unresolvedReferenceCount,indexCodeSource,validatedReferencesPresent,reviewer_unlockable,safe_after_manual_confirmation,recommended_next_action,unresolvedDetail: (.unresolvedDetail[0:8])})' \
  ./reports/staged-real-blocker-forensics-report.json

# 4) Forensics regression tests
pnpm test:staged-forensics
```

Phase 6A.12 recurring 37.x citation diagnostics (read-only + experimental alias simulation):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# 1) Baseline recurring 37.x diagnostics (read-only)
API_BASE_URL=http://127.0.0.1:8787 \
DIAGNOSE_37X_TOP_LIMIT=25 \
DIAGNOSE_37X_LIST_LIMIT=250 \
DIAGNOSE_37X_REPORT_NAME="staged-real-37x-diagnostics-report.json" \
pnpm diagnose:37x

# 2) Inspect aggregate counts and recurring citation classification
jq '.summary, .aggregate, .recurring_37x | map({citation,occurrences,ordinance_referenceType_count,rules_referenceType_count,classification,rationale,verify:.verify.diagnostic})' \
  ./reports/staged-real-37x-diagnostics-report.json

# 3) Inspect detailed samples for explicit 37.x family
jq '.recurring_37x | map(select(.citation=="37.2" or .citation=="37.3" or .citation=="37.7" or .citation=="37.8" or .citation=="37.9"))' \
  ./reports/staged-real-37x-diagnostics-report.json

# 4) Experimental alias simulation (still read-only; no approval changes)
API_BASE_URL=http://127.0.0.1:8787 \
DIAGNOSE_37X_TOP_LIMIT=25 \
EXPERIMENTAL_37X_ALIAS_MODE=1 \
DIAGNOSE_37X_REPORT_NAME="staged-real-37x-diagnostics-alias-sim.json" \
pnpm diagnose:37x

jq '.aggregate, (.recurring_37x | map({citation,classification,experimental_would_resolve}))' \
  ./reports/staged-real-37x-diagnostics-alias-sim.json

# 5) 37.x diagnostics regression tests
pnpm test:diagnose-37x
```

Phase 6A.13 safe production fix for ordinance-prefixed 37.x normalization (narrow set only):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api

# 1) Reprocess staged real docs to recompute reference validation with fixed normalization
API_BASE_URL=http://127.0.0.1:8787 \
REPROCESS_STATUS=staged \
REPROCESS_DECISION_ONLY=1 \
REPROCESS_LIMIT=250 \
REPROCESS_REPORT_NAME="pilot-reprocess-staged-37x-fix.json" \
pnpm reprocess:pilot

# 2) Recheck staged real blocker breakdown
curl -sS "http://127.0.0.1:8787/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=250" \
  | jq '.summary.blockerBreakdown, .summary.realApprovalReady'

# 3) Conservative staged real remediation (dry-run then apply)
API_BASE_URL=http://127.0.0.1:8787 \
REMEDIATE_DRY_RUN=1 \
REMEDIATE_LIMIT=12 \
REMEDIATE_LIST_LIMIT=250 \
REMEDIATE_REPORT_NAME="staged-real-remediation-post-37x-dryrun.json" \
pnpm remediate:staged-real

API_BASE_URL=http://127.0.0.1:8787 \
REMEDIATE_DRY_RUN=0 \
REMEDIATE_REPROCESS_FIRST=1 \
REMEDIATE_LIMIT=8 \
REMEDIATE_LIST_LIMIT=250 \
REMEDIATE_REPORT_NAME="staged-real-remediation-post-37x-apply.json" \
pnpm remediate:staged-real

jq '.summary, .before_blocker_breakdown, .after_blocker_breakdown, .promoted' \
  ./reports/staged-real-remediation-post-37x-apply.json

# 4) Rerun rollout candidate check (no promotion) + approved real retrieval checks
API_BASE_URL=http://127.0.0.1:8787 \
ROLLOUT_DRY_RUN=1 \
PROMOTE_LIMIT=10 \
APPROVED_REAL_CHECK_LIMIT=25 \
ROLLOUT_REPORT_NAME="pilot-approval-rollout-post-37x.json" \
pnpm rollout:pilot-approval

jq '.summary, .candidate_docs[0:20], .approved_real_doc_search_checks[0:20]' \
  ./reports/pilot-approval-rollout-post-37x.json

# 5) Regression tests for legal-reference normalization behavior
API_BASE_URL=http://127.0.0.1:8787 pnpm test:legal-references
pnpm test:diagnose-37x
```

Missing-index remediation validation test:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm test:missing-index-remediation
```

Note: this test rebuilds legal references with a minimal deterministic test set. Re-run your normal reference normalization afterward before continued pilot work.

## Retrieval diagnostics

Interactive view:

- [http://localhost:5555/admin/retrieval](http://localhost:5555/admin/retrieval)

Drafting UI:

- [http://localhost:5555/drafting](http://localhost:5555/drafting)
- [http://localhost:5555/admin/config](http://localhost:5555/admin/config)

Raw endpoint:

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/retrieval/debug \
  -H 'content-type: application/json' \
  -d '{
    "query":"L182214",
    "queryType":"citation_lookup",
    "limit":10,
    "filters":{"approvedOnly":false}
  }' | jq
```

## Retrieval evaluation harness

Gold set fixture (editable):

- `apps/api/eval/gold-set.json`

Run evaluation:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:retrieval
```

Report output:

- `apps/api/reports/retrieval-eval-report.json`

The harness supports:

- keyword/topic
- exact phrase
- citation lookup
- party name lookup
- Index Code filter
- Rules/Ordinance filter
- Case Assistant retrieval input checks

## Drafting evaluation harness

Drafting gold set fixture (editable):

- `apps/api/eval/draft-gold-set.json`

Run evaluation:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:drafting
```

Report output:

- `apps/api/reports/drafting-eval-report.json`

## Template evaluation harness

Template gold set fixture (editable):

- `apps/api/eval/template-gold-set.json`

Run evaluation:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:template
```

Report output:

- `apps/api/reports/template-eval-report.json`

## Taxonomy/config inspection

Inspect active config:

```bash
curl -sS http://127.0.0.1:8787/admin/config/taxonomy | jq
```

Resolve case type (id/alias/fallback):

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/config/taxonomy/resolve \
  -H 'content-type: application/json' \
  -d '{"case_type":"variance"}' | jq
```

Validate config payload shape:

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/config/taxonomy/validate \
  -H 'content-type: application/json' \
  -d '{"version":"broken","case_types":[]}' | jq
```

## Legal-reference normalization (Phase 6C+)

Rebuild normalized reference layers with true-text preferred (layout-text fallback, PDF last resort):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
INDEX_CODES_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Index codes.pdf" \
ORDINANCE_TRUE_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rent-ordinance-2-9-26_true-text.txt" \
ORDINANCE_LAYOUT_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rent-ordinance-2-9-26_layout-text.txt" \
ORDINANCE_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Rent Ordinance - 2-9-26.pdf" \
RULES_TRUE_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rules-and-regulations-1-13-26_true-text.txt" \
RULES_LAYOUT_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rules-and-regulations-1-13-26_layout-text.txt" \
RULES_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Rules and Regulations - 1-13-26.pdf" \
API_BASE_URL=http://127.0.0.1:8787 \
pnpm normalize:references
```

Dry-run parsing without writing to D1:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
INDEX_CODES_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Index codes.pdf" \
ORDINANCE_TRUE_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rent-ordinance-2-9-26_true-text.txt" \
ORDINANCE_LAYOUT_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rent-ordinance-2-9-26_layout-text.txt" \
ORDINANCE_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Rent Ordinance - 2-9-26.pdf" \
RULES_TRUE_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rules-and-regulations-1-13-26_true-text.txt" \
RULES_LAYOUT_TEXT="/Users/cliftonnoble/Downloads/Bee’s Files 2/rules-and-regulations-1-13-26_layout-text.txt" \
RULES_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Rules and Regulations - 1-13-26.pdf" \
REFERENCES_DRY_RUN=1 \
pnpm normalize:references
```

Generate text exports locally from the source PDFs (if you do not already have exported text files):

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
ORDINANCE_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Rent Ordinance - 2-9-26.pdf" \
RULES_PDF="/Users/cliftonnoble/Downloads/Bee’s Files 2/Rules and Regulations - 1-13-26.pdf" \
pnpm generate:reference-text-exports
```

Then use the generated paths shown in output as:

- `ORDINANCE_TRUE_TEXT=.../rent-ordinance.export.txt`
- `RULES_TRUE_TEXT=.../rules-and-regs.export.txt`
- Optional backward-compat vars still accepted: `ORDINANCE_TEXT_EXPORT`, `RULES_TEXT_EXPORT` (treated as layout-text fallback)

Backfill normalized reference links/issues for already-ingested documents:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 BACKFILL_LIMIT=1500 pnpm backfill:references
```

Inspect normalized references + unmatched decision references:

```bash
curl -sS http://127.0.0.1:8787/admin/references | jq
```

Coverage and crosswalk completeness checks:

```bash
curl -sS http://127.0.0.1:8787/admin/references | jq '.coverage_report'
curl -sS http://127.0.0.1:8787/admin/references | jq '.summary.crosswalk_count'
curl -sS http://127.0.0.1:8787/admin/references | jq '.coverage_report.crosswalk'
curl -sS http://127.0.0.1:8787/admin/references | jq '.coverage_report.ordinance.parser_used, .coverage_report.rules.parser_used'
curl -sS http://127.0.0.1:8787/admin/references | jq '.unresolved_crosswalks | length'
curl -sS http://127.0.0.1:8787/admin/references | jq '.unresolved_crosswalks[0:10]'
curl -sS http://127.0.0.1:8787/admin/references | jq '.readiness_status'
curl -sS http://127.0.0.1:8787/admin/references | jq '.critical_citation_exceptions'
```

Collision diagnostics from committed coverage report:

```bash
curl -sS http://127.0.0.1:8787/admin/references | jq '.coverage_report.ordinance | {parsed_section_count, committed_section_count, duplicate_normalized_citations_encountered, duplicates_merged, duplicates_dropped}'
curl -sS http://127.0.0.1:8787/admin/references | jq '.coverage_report.rules | {parsed_section_count, committed_section_count, duplicate_normalized_citations_encountered, duplicates_merged, duplicates_dropped}'
```

Critical citation verification:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm verify:critical-citations
```

Rules citation inventory inspection:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm inspect:rules-citations
API_BASE_URL=http://127.0.0.1:8787 RULE_CITATION="6.14" pnpm inspect:rules-citations
API_BASE_URL=http://127.0.0.1:8787 RULE_BARE="6.14" pnpm inspect:rules-citations
API_BASE_URL=http://127.0.0.1:8787 RULE_PREFIX="10.10" RULE_LIMIT=50 pnpm inspect:rules-citations
```

Or directly via endpoint:

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/references/verify-citations \
  -H 'content-type: application/json' \
  -d '{"citations":["37.2(g)","37.3(a)(1)","37.15","1.11","6.13","10.10(c)(3)","13.14"]}' | jq
```

Specific final critical checks:

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/references/verify-citations \
  -H 'content-type: application/json' \
  -d '{"citations":["10.10(c)(3)","37.3(a)(1)"]}' | jq
```

Rules-focused critical verification (includes crosswalk-targeted rules):

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/references/verify-citations \
  -H 'content-type: application/json' \
  -d '{"citations":["1.11","6.13","6.14","8.12","10.10(c)(3)","13.14"]}' | jq
```

Crosswalk resolution breakdown:

```bash
curl -sS http://127.0.0.1:8787/admin/references | jq '.summary.crosswalk_count, .coverage_report.crosswalk.resolved_links, .coverage_report.crosswalk.unresolved_links'
```

One-command readiness check:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 MIN_ORDINANCE_SECTIONS=15 MIN_RULES_SECTIONS=10 pnpm eval:reference-coverage
```

UI inspection:

- [http://localhost:5555/admin/references](http://localhost:5555/admin/references)

## Pilot quality evaluation summary script

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:pilot
```

Report output:

- `apps/api/reports/pilot-eval-report.json`

## Corpus quality report for expanded batch

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 CORPUS_LIMIT=800 pnpm eval:corpus-quality
```

Report output:

- `apps/api/reports/corpus-quality-report.json`

Includes:

- ingested/approved/searchable/staged/rejected counts
- missing required metadata counts
- extraction warning distribution
- case-type suggestion distribution
- anchor/chunk anomaly lists
- low-confidence extraction list

## Regression tests

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm test:pilot-hardening
API_BASE_URL=http://127.0.0.1:8787 pnpm test:local-harness
API_BASE_URL=http://127.0.0.1:8787 pnpm test:case-assistant
API_BASE_URL=http://127.0.0.1:8787 pnpm test:draft-conclusions
API_BASE_URL=http://127.0.0.1:8787 pnpm test:draft-template
API_BASE_URL=http://127.0.0.1:8787 pnpm test:draft-export
API_BASE_URL=http://127.0.0.1:8787 pnpm test:taxonomy-config
API_BASE_URL=http://127.0.0.1:8787 pnpm test:corpus-onboarding
API_BASE_URL=http://127.0.0.1:8787 pnpm test:approval-rollout
API_BASE_URL=http://127.0.0.1:8787 pnpm test:metadata-cleanup
API_BASE_URL=http://127.0.0.1:8787 pnpm test:missing-index-remediation
API_BASE_URL=http://127.0.0.1:8787 pnpm test:legal-references
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:drafting
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:template
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:corpus-quality
API_BASE_URL=http://127.0.0.1:8787 pnpm eval:expanded-safety
```

## Drafting endpoint quick test

```bash
curl -sS -X POST http://127.0.0.1:8787/api/draft/conclusions \
  -H 'content-type: application/json' \
  -d '{
    "findings_text":"Applicant seeks zoning variance relief for lot coverage with notice and mitigation findings.",
    "law_text":"Rule 3.1 notice requirements and Ordinance 77-19 limits apply.",
    "index_codes":["IC-104"],
    "rules_sections":["Rule 3.1"],
    "ordinance_sections":["Ordinance 77-19"],
    "issue_tags":["variance","lot coverage"]
  }' | jq
```

## Draft debug quick test

```bash
curl -sS -X POST http://127.0.0.1:8787/admin/draft/debug \
  -H 'content-type: application/json' \
  -d '{
    "findings_text":"Applicant seeks zoning variance relief for lot coverage with notice and mitigation findings.",
    "law_text":"Rule 3.1 notice requirements and Ordinance 77-19 limits apply.",
    "index_codes":["IC-104"],
    "rules_sections":["Rule 3.1"],
    "ordinance_sections":["Ordinance 77-19"],
    "issue_tags":["variance","lot coverage"]
  }' | jq
```

## Template endpoint quick tests

Blank scaffold:

```bash
curl -sS -X POST http://127.0.0.1:8787/api/draft/template \
  -H 'content-type: application/json' \
  -d '{"case_type":"zoning_variance","template_mode":"blank_scaffold"}' | jq
```

Guided scaffold:

```bash
curl -sS -X POST http://127.0.0.1:8787/api/draft/template \
  -H 'content-type: application/json' \
  -d '{"case_type":"licensing_enforcement","template_mode":"guided_scaffold"}' | jq
```

Lightly contextualized:

```bash
curl -sS -X POST http://127.0.0.1:8787/api/draft/template \
  -H 'content-type: application/json' \
  -d '{
    "case_type":"zoning_variance",
    "template_mode":"lightly_contextualized",
    "findings_text":"Applicant seeks lot coverage variance with documented neighborhood notice and mitigation commitments.",
    "law_text":"Rule 3.1 notice requirements and Ordinance 77-19 lot coverage standards apply.",
    "index_codes":["IC-104"],
    "rules_sections":["Rule 3.1"],
    "ordinance_sections":["Ordinance 77-19"]
  }' | jq
```

## Export endpoint quick tests

Markdown export (conclusions):

```bash
curl -sS -X POST http://127.0.0.1:8787/api/draft/export \
  -H 'content-type: application/json' \
  -d '{
    "kind":"conclusions",
    "format":"markdown",
    "document_title":"Conclusions of Law Draft",
    "conclusions":{
      "query_summary":"demo",
      "draft_text":"1. Legal Framework\n[demo]",
      "draft_sections":[{"id":"s1","heading":"Legal Framework","text":"[demo]","citation_ids":[]}],
      "paragraph_support":[{"paragraph_id":"p1","section_id":"s1","text":"[demo]","support_level":"unsupported","citation_ids":[],"support_notes":["No supporting authorities mapped"]}],
      "supporting_authorities":[],
      "reasoning_notes":[],
      "confidence":"low",
      "confidence_signals":{"retrieval_strength":0,"authority_count":0,"direct_conclusions_count":0,"explicit_support_ratio":0,"findings_coverage":0,"law_coverage":0,"conflict_index":0,"paragraph_support_ratio":0,"confidence_score":0},
      "limitations":["demo"],
      "citations":[]
    }
  }' | jq
```

## API endpoints

- `POST /search`
- `POST /admin/retrieval/debug`
- `POST /api/case-assistant`
- `POST /api/draft/conclusions`
- `POST /api/draft/template`
- `POST /api/draft/export`
- `POST /admin/draft/debug`
- `GET /admin/config/taxonomy`
- `POST /admin/config/taxonomy/resolve`
- `POST /admin/config/taxonomy/validate`
- `GET /source/:documentId`
- `GET /admin/ingestion/documents`
- `GET /admin/ingestion/documents/:documentId`
- `POST /admin/ingestion/documents/:documentId/metadata`
- `POST /admin/ingestion/documents/:documentId/approve`
- `POST /admin/ingestion/documents/:documentId/reject`
- `POST /admin/ingestion/documents/:documentId/reprocess`

## Guardrails retained

- citation integrity preserved (no fabricated citation objects)
- reranking remains traceable via diagnostics
- QC/search-ready gating not weakened
- rejected docs remain excluded from search

## Remaining risks before Phase 6A

- pilot gold set still small; needs broader judge/ALJ scenarios
- ranking weights are heuristic and should be calibrated on larger labeled set
- party-name matching is string-based and may miss aliases/entity normalization
- OCR/scanned legacy docs still need dedicated path for robust extraction
- diagnostics are dev-facing; production auth/audit controls still needed
- drafting prose quality is still heuristic/template-driven and should be upgraded with statement-level proposition validation
- taxonomy is validated and inspectable but still code-hosted; admin-managed persistent config storage is still a TODO
- source proxy is local convenience and should be protected or disabled outside trusted development environments
- DOCX export is still TODO; current export formats are markdown/text/html only

## Phase R1: Retrieval-First Ingestion Foundation

This phase adds a retrieval-focused document/chunk model for decision documents and a deterministic chunk preview path.

### Retrieval-first architecture (R1)

- Ingestion remains the source of truth (`documents`, `document_sections`, `section_paragraphs`, `document_reference_links`).
- Retrieval document model is read-only and normalized from persisted ingestion state.
- Retrieval chunks are generated deterministically from persisted section/paragraph structure.
- Chunk IDs are deterministic (`drchk_<stable-hash>`), not random.
- Each chunk carries provenance for source traceability:
  - source file/link
  - section + paragraph anchors
  - citation anchors
  - char offsets in reconstructed plain text when available
- Chunk metadata includes citation families + rules/ordinance mentions detected in each chunk.
- This phase is embedding-ready (chunk shape stable) but does not change embedding storage/writes.

### Retrieval chunk inspection endpoint

```bash
curl -sS "http://127.0.0.1:8787/admin/retrieval/documents/<DOCUMENT_ID>/chunks?includeText=1" | jq '.document, .stats, (.chunks[0:5])'
```

Use `includeText=0` to keep metadata only:

```bash
curl -sS "http://127.0.0.1:8787/admin/retrieval/documents/<DOCUMENT_ID>/chunks?includeText=0" | jq '.document, .stats, (.chunks[0:5])'
```

### Retrieval chunk report (JSON + Markdown)

Auto-select recent real decision docs:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
RETRIEVAL_DOC_LIMIT=5 \
RETRIEVAL_REAL_ONLY=1 \
RETRIEVAL_INCLUDE_TEXT=1 \
pnpm report:retrieval-chunks
```

Use explicit document IDs:

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 \
RETRIEVAL_DOC_IDS="doc_a,doc_b" \
RETRIEVAL_INCLUDE_TEXT=1 \
RETRIEVAL_CHUNK_REPORT_NAME="retrieval-chunk-report-sample.json" \
RETRIEVAL_CHUNK_MARKDOWN_NAME="retrieval-chunk-report-sample.md" \
pnpm report:retrieval-chunks
```

Output files:

- `apps/api/reports/retrieval-chunk-report.json`
- `apps/api/reports/retrieval-chunk-report.md`

### Deterministic chunking test

```bash
cd /Users/cliftonnoble/Documents/Beedle\ AI\ App/apps/api
API_BASE_URL=http://127.0.0.1:8787 pnpm test:retrieval-foundation
```

Read-only note:

- R1 is retrieval-inspection only.
- No approval mutation, no metadata writeback, no citation writeback, no QC threshold changes.
