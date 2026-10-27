# Beedle AI engineering audit and repair backlog

Reviewed **2026-09-29**, against commit **`a95c508ddddbec98de811cb17a2422a05a2487dc`**. The original review did not change application code. Remediation began on **2026-10-26** on `codex/audit-remediation`; each item records its implementation and validation below. Historical evidence describes the reviewed commit, not necessarily the current code.

## Current remediation scope

Updated by the user on **2026-10-26**: implement **M01–M26**, sequentially, one tested local commit per finding. **H01 authentication and all Low findings remain out of scope.** The completed/reviewed 17 High commits are preserved. This Medium goal supersedes the earlier High-only restriction; High implementation/review records below are historical.

## High-only implementation completed

Completed locally on **2026-10-26**: **H02–H18**, exactly **17 commits**, one per finding, on `codex/audit-remediation`. H01 remains deferred; all 26 Medium and 7 Low findings remain open. Each High repair record below describes the change, verification and rollout limits.

Final verification passed: 84 API behavioral tests, 84 API source guards, 38 utility tests, 10 search/queryability checks, 32 web tests, all workspace typechecks and the framework security-floor check. Local/production Worker dry-run bundles and the Pages build passed. Local runtime rehearsals and the nine-scenario H18 browser regression are recorded per finding. Earlier validation guards were updated within their owning H10/H13/H17 commits; no extra repair commits were added.

**Release gates remain:** these are local implementation commits, not a production rollout. Back up and rehearse migrations on an isolated representative corpus, review legacy/unverified holds and required reapproval, provision and verify separate preview resources/build variables, and test actual providers before deployment. H01 authentication, M12 cross-reviewer concurrency and M20 adapter support remain explicitly outside this completed scope. The dependency registry audit remains blocked as described under limitations.

## Follow-up regression review

Reviewed all 17 High fixes again on **2026-10-26**. Corrections in H05, H06, H07, H08, H12, H13, H16, H17 and H18 are folded into their existing commits. The [detailed review ledger](high-fix-regression-review-2026-10-26.md) records the evidence, rejected hypotheses, verification and compatibility effects for every High fix. The branch retains exactly 17 finding commits; H01 and Medium/Low scope remain deferred.

Follow-up checks passed: **94 API behavioral tests, 84 source guards, 38 utility tests, 10 search/queryability checks, 33 web tests**, workspace typechecks, framework guard, Worker dry run, actual Pages build, 10 browser race scenarios and a disposable workerd/D1/R2 HTTP rehearsal. No production deployment or migration was performed. Catalogs whose older normalizer merged distinct Parts require rebuilding from original import artifacts; very short/header-only PDF pages may now require manual extraction review.

## Assessment

The most important work is to make the corpus and generated answers trustworthy under failure, retries, and concurrent use. The application already has useful foundations: strict TypeScript, shared Zod contracts, parameterized SQL, FTS, bounded search candidates, upload compensation, a vector queue, error sanitization, and regression fixtures. Several earlier fixes are real improvements. However, comments describing those fixes sometimes promise stronger behavior than the current implementation provides.

The highest risks are missing application authorization, unsafe source-file serving, extraction and citation errors, misleading grounding signals, inconsistent publication/approval rules, and incomplete transactional boundaries across ingestion, activation, and background jobs. Optimizing these systems without first making their contracts explicit would make incorrect results faster and harder to diagnose.

There are also substantial performance opportunities: replace remaining corpus-wide substring scans, make FTS updates use stable row IDs, remove request-time schema writes, stabilize pagination, bound maintenance work, and stop repeating extraction and reference queries unnecessarily. Do not start by adding broad caching or increasing concurrency everywhere; both can conceal correctness defects, and a single D1 database processes queries serially.

## How to use this document

- **High:** security exposure, plausible data loss/corruption, wrong legal evidence, or broken trust/publication boundaries. Resolve before broadening use.
- **Medium:** material reliability, correctness, scaling, latency, or maintainability problems with a narrower immediate impact.
- **Low:** localized UX, operational clarity, or engineering improvements with lower immediate risk.
- **Difficulty:** Small is a focused change; Medium spans a few components; Large requires a migration, new state model, or substantial evaluation. These are relative estimates, not delivery commitments.
- **Change risk:** the risk of implementing the repair incorrectly, distinct from the severity of the existing problem. High-risk changes need a compatibility rollout, fixtures, and a tested rollback.
- Each unchecked finding is open. Record the fixing PR, verification results, and any migration/rollback evidence beneath that item before checking it off.

Evidence labels: **Reproduced** means a local isolated execution demonstrated the behavior; **Code-confirmed** means the path is directly present in source; **Deployment-dependent** means production exposure needs infrastructure verification. Code line references describe this commit and will move as repairs land.

## Finding index

**51 findings: 18 High, 26 Medium, 7 Low.** Each finding below includes evidence, cause, recommended solution, acceptance checks, difficulty, and change risk.

### High

| ID | Finding | Difficulty / change risk |
| --- | --- | --- |
| H01 | [Enforce authentication, authorization, and document access in the API](#h01--enforce-authentication-authorization-and-document-access-in-the-api) | Large / High. |
| H02 | [Replace the vulnerable Next.js release and establish security update ownership](#h02--replace-the-vulnerable-nextjs-release-and-establish-security-update-ownership) | Medium–Large / High. |
| H03 | [Do not serve uploader-controlled active content from the API origin](#h03--do-not-serve-uploader-controlled-active-content-from-the-api-origin) | Medium / Medium. |
| H04 | [Enforce upload and archive limits before allocating the full payload](#h04--enforce-upload-and-archive-limits-before-allocating-the-full-payload) | Large / High. |
| H05 | [Replace permissive PDF/DOCX fallbacks with a real extraction quality gate](#h05--replace-permissive-pdfdocx-fallbacks-with-a-real-extraction-quality-gate) | Large / High. |
| H06 | [Preserve full legal subsection citations and unify critical-reference normalization](#h06--preserve-full-legal-subsection-citations-and-unify-critical-reference-normalization) | Medium–Large / High. |
| H07 | [Stop reporting unrelated draft paragraphs as strongly supported](#h07--stop-reporting-unrelated-draft-paragraphs-as-strongly-supported) | Large / High. |
| H08 | [Retrieve actual governing law and distinguish it from similar-case facts](#h08--retrieve-actual-governing-law-and-distinguish-it-from-similar-case-facts) | Large / High. |
| H09 | [Surface drafting limitations and evidence in the review interface](#h09--surface-drafting-limitations-and-evidence-in-the-review-interface) | Medium / Medium. |
| H10 | [Define and enforce one publication and trust policy across retrieval paths](#h10--define-and-enforce-one-publication-and-trust-policy-across-retrieval-paths) | Large / High. |
| H11 | [Make approval use the actual reviewer policy and an atomic revision check](#h11--make-approval-use-the-actual-reviewer-policy-and-an-atomic-revision-check) | Medium–Large / High. |
| H12 | [Reprocessing must preserve reviewer edits and publish a coherent new document revision](#h12--reprocessing-must-preserve-reviewer-edits-and-publish-a-coherent-new-document-revision) | Large / High. |
| H13 | [Validate activation before writing and make retries non-destructive](#h13--validate-activation-before-writing-and-make-retries-non-destructive) | Large / High. |
| H14 | [Give vector jobs exclusive leases, durable attempts, and recovery for abandoned work](#h14--give-vector-jobs-exclusive-leases-durable-attempts-and-recovery-for-abandoned-work) | Large / High. |
| H15 | [Publish vector jobs only after ingestion artifacts are committed and complete](#h15--publish-vector-jobs-only-after-ingestion-artifacts-are-committed-and-complete) | Medium / High. |
| H16 | [Replace global legal-reference data through versioned publication, not delete-and-restore](#h16--replace-global-legal-reference-data-through-versioned-publication-not-delete-and-restore) | Large / High. |
| H17 | [Isolate local development and previews from the production vector index](#h17--isolate-local-development-and-previews-from-the-production-vector-index) | Medium–Large / High. |
| H18 | [Bind reviewer forms and mutation completions to the selected document](#h18--bind-reviewer-forms-and-mutation-completions-to-the-selected-document) | Medium / Medium. |

### Medium

| ID | Finding | Difficulty / change risk |
| --- | --- | --- |
| M01 | [Close gaps in endpoint cost controls](#m01--close-gaps-in-endpoint-cost-controls) | Small–Medium / Medium. |
| M02 | [Bound the full LLM operation, including response consumption](#m02--bound-the-full-llm-operation-including-response-consumption) | Medium / Medium. |
| M03 | [Move remaining maintenance work to resumable bounded jobs](#m03--move-remaining-maintenance-work-to-resumable-bounded-jobs) | Large / High. |
| M04 | [Track vector generations and clean up obsolete embeddings](#m04--track-vector-generations-and-clean-up-obsolete-embeddings) | Large / High. |
| M05 | [Apply semantic-search filters before the global top-K cutoff](#m05--apply-semantic-search-filters-before-the-global-top-k-cutoff) | Large / High. |
| M06 | [Finish the indexed search path and put a budget on expensive fallbacks](#m06--finish-the-indexed-search-path-and-put-a-budget-on-expensive-fallbacks) | Large / High. |
| M07 | [Give FTS rows stable integer identities for efficient updates and deletes](#m07--give-fts-rows-stable-integer-identities-for-efficient-updates-and-deletes) | Large / High. |
| M08 | [Make pagination stable and distinguish result counts from bounded candidates](#m08--make-pagination-stable-and-distinguish-result-counts-from-bounded-candidates) | Large / High. |
| M09 | [Remove schema creation and sticky failure flags from request handling](#m09--remove-schema-creation-and-sticky-failure-flags-from-request-handling) | Medium–Large / High. |
| M10 | [Backfill FTS when upgrading a database that already contains documents](#m10--backfill-fts-when-upgrading-a-database-that-already-contains-documents) | Medium–Large / High. |
| M11 | [Parenthesize the low-confidence reviewer filter](#m11--parenthesize-the-low-confidence-reviewer-filter) | Small / Low. |
| M12 | [Fix metadata PATCH semantics and protect concurrent reviewer edits](#m12--fix-metadata-patch-semantics-and-protect-concurrent-reviewer-edits) | Medium / Medium–High. |
| M13 | [Paginate reviewer work and remove quadratic peer lists](#m13--paginate-reviewer-work-and-remove-quadratic-peer-lists) | Large / High. |
| M14 | [Validate assistant citations and keep retrieved content out of privileged prompt roles](#m14--validate-assistant-citations-and-keep-retrieved-content-out-of-privileged-prompt-roles) | Large / High. |
| M15 | [Align chat history and message-size contracts](#m15--align-chat-history-and-message-size-contracts) | Small–Medium / Medium. |
| M16 | [Retrieve for the conversation's actual question and honor every accepted filter](#m16--retrieve-for-the-conversations-actual-question-and-honor-every-accepted-filter) | Medium–Large / Medium–High. |
| M17 | [Separate live chat state from persistence and make storage failures recoverable](#m17--separate-live-chat-state-from-persistence-and-make-storage-failures-recoverable) | Medium / Medium. |
| M18 | [Version governing references by jurisdiction and effective date](#m18--version-governing-references-by-jurisdiction-and-effective-date) | Large / High. |
| M19 | [Add behavioral integration tests and an independently judged relevance holdout](#m19--add-behavioral-integration-tests-and-an-independently-judged-relevance-holdout) | Large / Medium. |
| M20 | [Gate deployments on schema readiness and the actual hosting adapter](#m20--gate-deployments-on-schema-readiness-and-the-actual-hosting-adapter) | Large / High. |
| M21 | [Measure end-to-end latency, fallback rates, and durable-job health](#m21--measure-end-to-end-latency-fallback-rates-and-durable-job-health) | Medium / Low–Medium. |
| M22 | [Make ingestion idempotent and distinguish duplicate files from new revisions](#m22--make-ingestion-idempotent-and-distinguish-duplicate-files-from-new-revisions) | Medium–Large / High. |
| M23 | [Use a truthful fallback when taxonomy inference has no signal](#m23--use-a-truthful-fallback-when-taxonomy-inference-has-no-signal) | Small–Medium / Medium. |
| M24 | [Neutralize spreadsheet formulas in reviewer CSV exports](#m24--neutralize-spreadsheet-formulas-in-reviewer-csv-exports) | Small / Low–Medium. |
| M25 | [Reject malformed mutations and bound validation work](#m25--reject-malformed-mutations-and-bound-validation-work) | Medium / Medium. |
| M26 | [Make migration and recovery scripts target-aware and verifiable](#m26--make-migration-and-recovery-scripts-target-aware-and-verifiable) | Medium–Large / High. |

### Low

| ID | Finding | Difficulty / change risk |
| --- | --- | --- |
| L01 | [Split policy, retrieval stages, and persistence behind tested contracts](#l01--split-policy-retrieval-stages-and-persistence-behind-tested-contracts) | Large / High. |
| L02 | [Generate binding types and validate external payloads at runtime](#l02--generate-binding-types-and-validate-external-payloads-at-runtime) | Small–Medium / Low. |
| L03 | [Remove the external CSS font dependency from the critical render path](#l03--remove-the-external-css-font-dependency-from-the-critical-render-path) | Small / Low. |
| L04 | [Complete keyboard and focus behavior in dialogs and reviewer controls](#l04--complete-keyboard-and-focus-behavior-in-dialogs-and-reviewer-controls) | Small–Medium / Low. |
| L05 | [Make the unfinished intake page unambiguously noninteractive](#l05--make-the-unfinished-intake-page-unambiguously-noninteractive) | Small / Low. |
| L06 | [Consolidate current operations guidance and remove machine-specific assumptions](#l06--consolidate-current-operations-guidance-and-remove-machine-specific-assumptions) | Small–Medium / Low. |
| L07 | [Load decision detail independently of search context](#l07--load-decision-detail-independently-of-search-context) | Small–Medium / Low–Medium. |


## High severity

### H01 — Enforce authentication, authorization, and document access in the API

- [ ] Deferred by user on 2026-10-26 — authentication is explicitly outside this remediation goal.
- **Evidence:** Code-confirmed; external exposure is deployment-dependent. [apps/api/src/index.ts:148](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:148), [apps/api/src/index.ts:76](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:76), [apps/api/src/routes/source.ts:86](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/routes/source.ts:86), [apps/api/src/services/retrieval-foundation.ts:926](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-foundation.ts:926).
- **Problem and cause:** The Worker dispatches requests after cost controls but never verifies an identity or permission. Ingestion, approval, rejection, metadata edits, reference rebuilds, activation, debug endpoints, and original source downloads share this boundary. Knowing a document ID is sufficient for the source/detail handlers. CORS controls browser response access; it does not authorize requests. The runbook describes Cloudflare Access, but the checkout cannot establish whether every hostname/path is protected. Even a blanket Access allowlist would not distinguish a reader from a corpus administrator.
- **Best solution:** Define reader, reviewer, and administrator capabilities. Verify the chosen identity mechanism at the Worker boundary and enforce route and document permissions centrally. If using Access, validate its JWT issuer/audience/signature and audit all alternate hostnames. Give operator scripts scoped service credentials. Add actor identity and before/after revisions to mutation audit records. Add CSRF/origin protection where cookie-based identity is used.
- **Verification:** Anonymous and reader identities cannot invoke writes; rejected/staged sources follow an explicit read policy; authorized review still works; hostname and service-token tests run against the deployment configuration.
- **Difficulty / change risk:** **Large / High.** Authentication can break the frontend, source links, scripts, CI smoke tests, and cross-origin sessions. Roll out a complete client/server integration, not an isolated middleware patch.

### H02 — Replace the vulnerable Next.js release and establish security update ownership

- [x] Implemented and verified locally on 2026-10-26; deployment verification remains a release gate.
- **Evidence:** [apps/web/package.json:19](/Users/cliftonnoble/Code/beedle-ai/apps/web/package.json:19) pins Next.js **15.2.0**; the successful build also reports 15.2.0. The official [December 2025 Next.js security update](https://nextjs.org/blog/security-update-2025-12-11) identifies affected App Router releases and lists 15.2.8 as a patched release for the issues covered by that bulletin. This application uses the App Router and has an edge-rendered dynamic route.
- **Problem and cause:** The framework is behind published security fixes. A successful build is not evidence that these protocol-level vulnerabilities are mitigated. Specific exploitability depends on the deployed adapter/runtime and enabled features; this review did not attempt exploitation. The bulletin is evidence that 15.2.0 is unsafe, not a recommendation to treat 15.2.8 as the latest safe version in September 2026.
- **Best solution:** Resolve a currently maintained, patched Next/React combination compatible with the chosen Cloudflare deployment path, update the lockfile, and add an owned dependency-security update process. Evaluate the hosting adapter at the same time as M20, but keep the emergency security upgrade as small as practical.
- **Verification:** Dependency advisory checks, full build, adapter build, actual deployed-runtime smoke, search/detail navigation, and assistant/drafting flows. Record the resolved advisories and any accepted exceptions.
- **Difficulty / change risk:** **Medium–Large / High.** React, Next, and the Pages adapter must be tested together. Full dependency advisory scanning remains subject to the limitation recorded at the end of this document.

**Repair record — H02**

- Commit subject: `fix(H02): patch the web framework and guard security floors`.
- Pinned Next 15.5.28 and React/React DOM 19.1.10; forced Next’s resolved Sharp to 0.35.5. Added a resolved-version guard, regression tests, CI/predeploy checks, weekly Dependabot proposals, and maintainer ownership in `docs/dependency-security.md`. Route types now generate before typechecking on a clean checkout.
- Validation: frozen offline installation, all workspace typechecks, 26 web tests, the actual Pages adapter build (including Next build), and local workerd HTTP/HTML smoke checks for search, assistant, drafting, and dynamic decision detail. No production requests or deployment.
- The existing adapter still declares Next <=15.5.2; its patched-framework build and local runtime pass, but the support-range warning is retained as a tracked M20 platform risk. Do not downgrade to an insecure version to silence it. The installed local workerd supported compatibility date 2026-03-01; production/runtime-version and full interactive/API preview verification are required before release.
- No schema migration. Roll back through a patched forward fix if runtime compatibility fails; do not restore Next 15.2.0.

### H03 — Do not serve uploader-controlled active content from the API origin

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** Code-confirmed. [packages/shared/src/index.ts:24](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:24), [apps/api/src/services/ingest.ts:467](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:467), [apps/api/src/services/storage.ts:3](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/storage.ts:3), [apps/api/src/routes/source.ts:110](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/routes/source.ts:110).
- **Problem and cause:** `mimeType` is accepted as an arbitrary string, persisted as R2 metadata, and reused as the response `Content-Type`. Sources are served `inline`. The parser's text fallbacks allow a nominal DOCX input to be arbitrary text. A source uploaded as `text/html` can therefore execute as a document on the API origin when opened. Filename sanitization does not address content execution.
- **Best solution:** Sniff and allowlist actual formats; derive the stored MIME type from validated content. Serve untrusted originals as attachments with `X-Content-Type-Options: nosniff`, or through an isolated download origin with a restrictive policy. Only permit inline previews for a deliberately supported format and preview mechanism. Add response security headers appropriate to the web and source surfaces.
- **Verification:** Upload benign HTML/SVG masquerading as a decision and confirm it is rejected or downloaded without execution. Verify valid DOCX/PDF/Markdown downloads, filenames, and source authorization.
- **Difficulty / change risk:** **Medium / Medium.** Changing disposition affects the current source-opening UX; preserve a usable download/preview flow.

**Repair record — H03**

- Commit subject: `fix(H03): validate source formats and force safe downloads`.
- Intake validates byte signatures/container identity and explicit Markdown text before persistence. Client MIME hints no longer select a parser or stored content type. Stored objects receive a derived MIME type and attachment disposition.
- Original and reconstructed-source responses use attachment disposition, nosniff, a sandbox CSP, no-store, and no-referrer. Legacy objects ignore their untrusted MIME and download as octet-stream, so old objects are protected without a backfill. Source viewing now downloads the original; the application’s text preview remains available.
- Validation: four behavioral tests executing the actual format/storage/source handlers, 84 API source tests, the source fallback regression, and API typecheck. Cases include HTML/SVG masquerading as documents, binary Markdown, non-DOCX ZIP, legacy malicious MIME, derived PDF MIME, and header-safe filenames.
- No migration or cloud writes. Authentication is intentionally deferred under H01. Full extraction/size validation is handled by H04/H05; MIME sniffing alone is not an extraction-quality assertion.

### H04 — Enforce upload and archive limits before allocating the full payload

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** Code-confirmed. [apps/api/src/routes/ingest.ts:48](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/routes/ingest.ts:48), [apps/api/src/routes/ingest.ts:53](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/routes/ingest.ts:53), [apps/api/src/services/parser.ts:194](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/parser.ts:194).
- **Problem and cause:** Multipart requests rely on `Content-Length` before calling `request.formData()`. A request without that header is fully parsed before the file-size check. DOCX extraction calls `unzipSync(bytes)` on the entire archive, then checks the aggregate decompressed size. A highly compressed archive can exhaust memory/CPU before the nominal 40 MiB limit is evaluated. The catch then converts extraction failures into an empty result and text fallback. Multipart also converts bytes to base64 only to decode them again, multiplying peak memory.
- **Best solution:** Apply a streaming envelope byte limit regardless of headers. Parse only required DOCX entries and enforce per-entry and aggregate output limits while decompressing, along with entry-count/ratio safeguards. Propagate format/size failures explicitly. Pass binary bytes internally; reserve base64 for legacy JSON clients. Move substantial extraction out of the request when needed.
- **Verification:** Chunked multipart above the limit, oversized metadata parts, many-entry archives, highly compressed required XML, corrupt ZIPs, and valid boundary-size inputs. Confirm controlled 4xx responses and bounded memory, not a Worker crash.
- **Difficulty / change risk:** **Large / High.** Parser and ingestion contract changes can reject previously accepted files; retain raw originals and test a representative corpus before rollout.

**Repair record — H04**

- Commit subject: `fix(H04): bound multipart intake and DOCX expansion`.
- Multipart is streamed through envelope-byte and part-count checks before native parsing. Metadata is bounded, duplicate files are rejected, and binary bytes reach ingestion directly. JSON/base64 intake remains supported with predecode and decoded-size limits.
- DOCX uses a bounded central-directory preflight and expands only document XML in 256-byte compressed input pieces. Entry count, ratio, per-entry/aggregate declared sizes, actual output size, CRC, paths, local/central consistency, and overlap are checked. Malformed/unsupported archives propagate errors instead of falling back after a caught extraction failure.
- Validation: 10 behavioral tests (including H03), 84 source tests, API/shared typechecks, and a successful Worker dry-run bundle. Fixtures include chunked/false-size uploads, cancellation, one-byte boundary splits, metadata/part limits, stored/deflated/streaming ZIPs, forged size, CRC corruption, oversized expansion, and unused corrupt media that is never inflated.
- Limits and compatibility are documented in `docs/source-intake.md`. No migration or production changes; previously accepted malformed/encrypted/ZIP64 documents must be converted through a reviewed workflow rather than bypassing limits. Extraction quality remains the separate H05 gate.

### H05 — Replace permissive PDF/DOCX fallbacks with a real extraction quality gate

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** **Reproduced.** [apps/api/src/services/parser.ts:222](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/parser.ts:222), [apps/api/src/services/parser.ts:331](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/parser.ts:331), [apps/api/src/services/ingest.ts:270](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:270). A synthetic PDF containing only catalog/object syntax produced a nonempty `Body` section made of PDF source syntax.
- **Problem and cause:** The PDF implementation recognizes a narrow `(...) Tj` pattern, otherwise treating printable binary/file syntax as text. It does not provide a general PDF text extractor or OCR. Failed DOCX parsing similarly falls back to decoding the ZIP bytes as text. Ingestion has no minimum meaningful-text/quality gate; law inputs are made searchable automatically and marked metadata-confirmed. Unsupported or unreadable law can enter retrieval as apparent authority.
- **Best solution:** Use a maintained extractor in a bounded background environment; preserve page coordinates and extraction diagnostics. Require meaningful text, expected structure, and an explicit extraction status before publication. Route scanned/unsupported documents to OCR or manual review. Do not silently reinterpret a failed binary format as plain text; plain-text/Markdown intake should be an explicit format.
- **Verification:** Compressed PDFs, fonts/encodings, tables, scanned pages, corrupt DOCX, empty documents, and known good originals. Compare extracted passages to source pages and ensure failed extraction remains unpublished.
- **Difficulty / change risk:** **Large / High.** Re-extraction changes chunk identities, anchors, ranking, and metadata. Use versioned derived data and a reversible publication switch.

**Repair record (H05)**

- Commit subject: `fix(H05): gate publication on real bounded text extraction`.
- Replaced PDF operator/printable-byte fallback with pinned unpdf 1.9.1 / PDF.js 6.4.299; replaced regex DOCX extraction with strict namespace-aware sax 1.6.1. No binary-as-text fallback remains. Diagnostics retain parser version and PDF page coordinates.
- Added bounded extraction and meaningful-text checks. Failed extraction retains an unpublished original with `needs_review`, no text artifacts, no vector job, and no metadata confirmation. Law extraction no longer fabricates human metadata confirmation. Reprocessing dispatches explicit Markdown correctly and records diagnostics.
- Validation: 14 behavioral tests, 84 source tests, fallback-retirement test, API typecheck, Worker dry-run (684 KiB gzip), and actual isolated workerd extraction (compressed/font-mapped PDF 200; blank-page PDF 422). No cloud resources or production documents used.
- Deliberate operating limit: inline PDFs <=3 MiB/50 pages; unsupported or larger documents require reviewed offline OCR/extraction. A general background extractor and automatic OCR are not built. This resolves unsafe publication while keeping resource use bounded; see `docs/source-intake.md` for limits, recovery, and required real-corpus comparison before release.
- No schema migration or corpus rewrite. Existing records are not retroactively certified. H12 supplies safe versioned reprocessing; do not restore permissive fallbacks as rollback.

### H06 — Preserve full legal subsection citations and unify critical-reference normalization

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** **Reproduced.** [apps/api/src/services/parser.ts:515](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/parser.ts:515), [apps/api/src/services/parser.ts:538](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/parser.ts:538), [apps/api/src/services/qc-shared.ts:17](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/qc-shared.ts:17). Input `Rule 10.10(c)(3). Apply Ordinance 37.2(g).` yielded `Rule 10.10` and `Ordinance 37.2`. Passing `Ordinance 37.2(g)` directly to the critical-reference detector yielded no exception.
- **Problem and cause:** A trailing word boundary after optional closing parentheses allows the citation regex to backtrack and drop the subsection. The QC normalizer also does not strip the ordinance prefix. Distinct legal provisions collapse to a parent and critical citations can miss review warnings. This is not merely display cleanup; references drive QC and retrieval.
- **Best solution:** Build one typed citation parser/normalizer with explicit rule/ordinance context and a boundary condition that permits a closing parenthesis. Preserve raw text, exact canonical citation, parent relationship, and resolution status separately. Repair affected metadata through a versioned, audited re-extraction pass; do not automatically promote newly resolved records.
- **Verification:** Nested subsections, punctuation/newlines, Roman/part prefixes, rule/ordinance ambiguity, exact-vs-parent matching, and the three documented critical exceptions. Recheck a corpus sample against original documents.
- **Difficulty / change risk:** **Medium–Large / High.** Correcting identifiers changes filter coverage, approval blockers, crosswalks, and ranking. Requires a data migration and relevance comparison.

**Repair record (H06)**

- Commit subject: `fix(H06): preserve exact citations and explicit legal context`.
- One typed parser now serves extraction, critical-reference QC, and normalized catalog lookup. Full nested subsections survive punctuation/newlines. Raw text, context, canonical identifier, immediate parent, part, parse version, and unresolved/ambiguous status remain separate in `metadata_json.citationEvidence`.
- Removed proximity-based double assignment and the special-case ordinance prefix allowlist. Explicit context constrains lookup; ambiguous bare sections are retained as evidence with a review warning. Exact-subsection validation cannot resolve against a parent. Critical exceptions include prefixed ordinance references.
- Validation: 18 behavioral tests, 84 source tests, API typecheck; fixtures cover the original reproduction, nested subsections, Roman/part prefixes, malformed identifiers, ambiguity, parent-only resolution, and persisted raw/canonical values.
- Migration/release: existing truncated metadata cannot be repaired by adding guessed suffixes. Retain current revisions, stage re-extraction with H12, compare `citationEvidence` and original pages, then review changed references before publication; never auto-approve a newly resolved citation. Rebuild normalized catalog generations through H16 if stored identifiers used old prefix conventions. No production data rewrite was run. Real-corpus comparison remains a release requirement.
- Rollback preserves prior published document/catalog generations; do not replace exact identifiers with parent approximations.

### H07 — Stop reporting unrelated draft paragraphs as strongly supported

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** **Reproduced.** [apps/api/src/services/draft-conclusions.ts:387](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:387), [apps/api/src/services/draft-conclusions.ts:566](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:566), [apps/api/src/services/draft-conclusions.ts:582](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:582). The paragraph **“Zebras orbit Jupiter.”** received `support_level: "strong"` from two unrelated rent/roof excerpts labeled explicit.
- **Problem and cause:** A Conclusions/Analysis section bonus is 0.14 and the admission threshold is only `> 0.08`; support can therefore qualify with zero token overlap. Two authorities marked explicit produce “strong.” “Explicit” itself can be assigned from a single shared token. The confidence score then incorporates this false support as evidence. These are similarity heuristics, not verification that a proposition follows from authority.
- **Best solution:** Immediately cap/rename the current signals as retrieval similarity and require review. Build claim-level support with exact evidence spans and source IDs, distinguish facts from legal propositions, and check contradictions/negation. Any learned verifier should be evaluated against adjudicator-labeled supported, contradicted, and insufficient-evidence examples and default to uncertainty. Do not let section bonuses establish support.
- **Verification:** The reproduced unrelated paragraph must be unsupported. Include negated holdings, same-topic but different rules, invented facts/citations, conflicting decisions, and overlapping sources from one document. Measure false-supported rate, not only retrieval overlap.
- **Difficulty / change risk:** **Large / High.** The output contract and confidence labels will change. Preserve historical outputs with their old scoring version rather than silently relabeling them.

**Repair record (H07)**

- Commit subject: `fix(H07): replace inferred support with reviewable claim evidence`.
- New drafts use `assessment_version: retrieval-only-2` and `review_required: true`. Paragraphs are only `unverified` or `unsupported`; section labels, descriptive match text, and an `explicit` label cannot establish support. Overall confidence remains low and semantic support/confidence scores remain zero until a separately validated verifier exists.
- Claims carry exact source-excerpt offsets, source IDs, exact-quotation versus retrieval-match labels, and conservative review flags for possible negation conflict, unmatched provisions, and unknown citation IDs. Source excerpts are deduplicated by document. Provided-fact text is distinguished from legal references and unclassified claims; other cases do not establish current-record facts.
- Existing payloads default to `legacy-1` and keep their historical scores instead of silently relabeling them. New draft generation requests citation IDs, retains paragraph boundaries, and maps only real retrieved IDs into evidence.
- Validation: 22 behavioral tests, 84 source tests, API/shared/web typechecks. Adversarial checks cover the original unrelated paragraph, negation, different provisions, invented citations, mixed supported-looking/unrelated claims, repeated chunks from one document, real excerpt offsets, fallback assembly, and legacy schema compatibility. All adversarial cases emit zero verified/strong support by construction.
- This is a conservative review aid, not automated legal entailment. Sentence splitting and conflict flags are heuristics; same-topic relevance is never certification. A human-labeled verifier evaluation remains a prerequisite to adding stronger labels. H09 exposes the review evidence consistently in the UI and every export format.
- No database migration. Deploy shared/API/web contracts together. Preserve assessment versions in stored or exported drafts during rollback.

### H08 — Retrieve actual governing law and distinguish it from similar-case facts

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** Code-confirmed. [apps/api/src/services/draft-conclusions.ts:152](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:152), [apps/api/src/services/draft-conclusions.ts:201](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:201), [apps/api/src/services/draft-conclusions.ts:421](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:421), [packages/shared/src/index.ts:295](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:295).
- **Problem and cause:** Draft research searches only `decision_docx`, then fills `relevant_law` with those decisions' Findings passages. `rules_sections`, `ordinance_sections`, and `uploaded_doc_ids` exist in the draft request contract but are not applied by this retrieval path. The draft can thus claim legal support from factual passages and silently ignore supplied context. Outcome guidance counts words such as “grant”/“denied,” without establishing whose request, which claim, negation, or the final disposition; it can bias the generated conclusion.
- **Best solution:** Separate current-record facts, similar holdings, and governing rules/ordinances in the research model. Retrieve requested provisions with jurisdiction/effective-version controls, honor every supported filter/input, and remove unsupported inputs from the contract until implemented. Obtain disposition from reviewed structured findings or a validated holding extractor; otherwise return unclear. Do not feed keyword vote counts to the model as authoritative outcome guidance.
- **Verification:** A rules-only request retrieves that rule; uploaded context is used only with proper access/trust labeling; Findings never populate a governing-law list; “request to grant was denied” does not become a grant recommendation.
- **Difficulty / change risk:** **Large / High.** This changes draft substance and ranking, requiring legal-domain review and holdout evaluation, not only snapshots.

**Repair record (H08)**

- Commit subject: `fix(H08): separate actual provisions from similar-case evidence`.
- Draft law research now resolves exact rules/ordinance requests (including explicit citations in supplied law text) to active catalog body text. Similar decision findings never populate the law list. Missing/ambiguous provisions are visible, and parent substitution is forbidden. Six-provision limits reject oversized requests explicitly instead of silently omitting law.
- Added source kinds, captured provision revisions, revision-checked provision links, and distinct supplied-findings/law context. Similar-decision retrieval requests trusted-only scope. Nonempty uploaded IDs are explicitly unsupported until the deferred document-access work is available. Rules-only requests are supported; empty requests are rejected.
- Removed keyword disposition/conflict votes and outcome guidance in the model context. Direction remains unclear without a reviewed structured outcome; the fallback explains what must be checked rather than asserting that retrieved facts are governing law.
- Validation: 25 behavioral tests, 84 source tests, API/shared/web typechecks, Worker dry-run. Real in-memory SQLite tests exercise exact provision lookup, missing subsections, revision conflicts, unavailable uploads, request bounds, and the “request to grant was denied” regression.
- Limitation: catalog jurisdiction/effective editions cannot be established from existing data. These are explicitly unverified provision candidates requiring review, not certified governing law. The separate Medium M18 redesign remains out of scope. See `docs/drafting-evidence.md`.
- No data migration or production requests. Deploy API/shared/web together; keep captured source revisions and rerun research when a linked provision changes. Legal-domain evaluation against actual case materials remains required before release.

### H09 — Surface drafting limitations and evidence in the review interface

- [x] Implemented and verified locally on 2026-10-26.
- **Evidence:** Code-confirmed. [apps/web/src/app/drafting/page.tsx:186](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/drafting/page.tsx:186), [apps/api/src/services/draft-conclusions.ts:775](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:775), [apps/api/src/services/draft-export.ts:60](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-export.ts:60).
- **Problem and cause:** The API returns paragraph support, citations, and limitations, but the main draft UI shows confidence, fallback status, raw draft text, and copy/export actions. A reviewer cannot inspect the supplied grounding in the normal review flow. Copy-full-draft discards it; the HTML export omits confidence, limitations, and the citation bibliography, while text export omits the bibliography.
- **Best solution:** Render limitations prominently, expose per-paragraph evidence and source navigation, and distinguish reviewed from unreviewed output. Make export modes explicit: review package with provenance, or draft-only text with a clear review status. Preserve model, corpus/extraction version, generation time, and source IDs in a durable review artifact. Do not rely on a confidence badge as the review workflow.
- **Verification:** Unsupported/conflicted paragraphs are visible before copying; sources open to the right passage; Markdown/text/HTML review packages retain essential caveats and provenance consistently.
- **Difficulty / change risk:** **Medium / Medium.** A UI/API/export contract change; coordinate with H07/H08 so the displayed evidence has valid semantics.

**Repair record (H09)**

- Commit subject: `fix(H09): preserve evidence and caveats through draft review and export`.
- Added a review component with prominent unreviewed status, limitation/fallback messages, unsupported/conflict counts, claim-level captured passages, source anchors/links, and available version provenance. Copy now produces a review package. Markdown, text, and HTML exports share one content model; JSON retains the full structured review response.
- Captured generation time, model/fallback state, assessment version, source/extraction metadata where available, and excerpt SHA-256. Revisions observed after retrieval are labeled honestly; unknown corpus/extraction versions remain unknown. Historical draft labels retain their legacy warning.
- Exported source/model text is escaped or fenced, with safe HTTP(S) HTML navigation and a restrictive HTML CSP. All formats include limitations, source bibliography, captured excerpts, and provenance.
- Validation: 28 behavioral tests, 84 source tests, 26 web tests, API/shared/web typechecks, and a successful actual Pages adapter build. Playwright with a local fixture API verified submit/render, visible unsupported/negation flags, captured-passage navigation, original provision revision navigation, clipboard caveats, all four downloads, and desktop/mobile layouts without horizontal overflow. Screenshots/downloads are local ignored QA artifacts under `output/playwright/h09/`. No live model or production API was called.
- No schema/data migration. Deploy API/shared/web together. Retain complete review packages; do not treat a hash, confidence label, or export as human approval. Existing whole-corpus versioning gaps are explicitly represented rather than retroactively invented.

### H10 — Define and enforce one publication and trust policy across retrieval paths

- [x] Implemented locally; migration rehearsal required before release
- **Evidence:** Code-confirmed. [apps/api/src/services/search-query-analysis.ts:1642](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-query-analysis.ts:1642), [apps/api/src/services/search.ts:669](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:669), [apps/api/src/services/search.ts:1982](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:1982), [apps/api/src/services/case-assistant.ts:110](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/case-assistant.ts:110), [apps/web/src/app/search/page.tsx:426](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/search/page.tsx:426).
- **Problem and cause:** Search scope never requires `searchable_at`. The UI requests provisional mode with `approvedOnly: false`, so any non-rejected chunked decision qualifies, including staged or partially persisted content. Some keyword paths widen `trusted_only` to provisional mode when results are sparse (notably when `approvedOnly` is false), while the response retains the requested mode. Conversely, an approved document without activated retrieval chunks can be returned by trusted-only scope but labeled provisional. Uploaded-document research bypasses rejection/approval predicates and labels the rows trusted.
- **Best solution:** Specify separate states for ingestion completeness, reviewer approval, publication, and retrieval readiness. Centralize eligibility predicates and trust labels across search, assistants, previews, and sources. Require a completed published revision for user-facing retrieval; allow explicit authorized draft inspection separately. Never widen trust silently. If a wider search is offered, report the effective scope and require deliberate user intent where it changes evidence eligibility.
- **Verification:** Matrix-test every lifecycle state across all retrieval paths, including partial imports, rejected uploads, approved-but-unactivated documents, and sparse trusted-only queries. Check that labels match actual policy.
- **Difficulty / change risk:** **Large / High.** Existing searches may lose documents that are currently admitted accidentally or by legacy policy. Inventory the corpus and backfill explicit states before enforcing the new rule.

**Repair record (H10)**

- Commit subject: `fix(H10): enforce coherent publication and trust across retrieval`.
- Added explicit ingestion readiness, document/publication/approval revisions, and shared SQL eligibility predicates. Only completed published revisions enter public search, source downloads, or decision previews. Trust requires current human approval and confirmation; vector activation cannot confer it. Search fallbacks retain scope and final candidates revalidate eligibility and labels.
- Search defaults to trusted records, with a visible opt-in for published provisional evidence. Assistant/uploaded-document research requires trust. Staged/legacy inspection uses explicit admin routes; H01 authentication remains deferred.
- Verified **30 behavioral tests, 84 source guards, 26 web tests**, API/shared/web typechecks, and a Worker dry-run bundle (688.77 KiB gzip). The lifecycle matrix runs actual SQL, search, download, preview, and assistant implementations, including activated-unapproved and approved-unactivated records. Actual ingestion tests verify completion after artifact/source persistence and failure compensation.
- **Release risk:** migration 0013 holds existing rows as legacy. [Publication policy and rollout](publication-policy.md) includes read-only inventory SQL, isolated-copy rehearsal, verification/review requirements, and coordinated backup restoration. No production inventory or migration was performed; H12 provides verified reprocessing. Existing public bucket URLs require separate perimeter/storage work under H01.

### H11 — Make approval use the actual reviewer policy and an atomic revision check

- [x] Implemented locally; revision-aware client rollout required
- **Evidence:** Code-confirmed and isolated approval probe. [apps/api/src/services/admin-ingestion.ts:186](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:186), [apps/api/src/services/ingest.ts:518](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:518), [apps/api/src/services/admin-ingestion.ts:2341](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:2341).
- **Problem and cause:** Reviewer readiness blocks critical references, unresolved references, excessive warnings, low extraction confidence, and missing confirmation. The approval service checks only three QC booleans, confirmation, and rejection. A document marked ineligible by the UI can still be approved through either approval endpoint. The read-check-update sequence also permits an intervening rejection or metadata change; the final UPDATE only checks the ID.
- **Best solution:** Implement one versioned approval policy used by both display and mutation. Enforce it against the exact revision being approved using a conditional write and affected-row check. If exceptions are allowed, make them a separate authorized override with a reason and audit trail. Reset approval/confirmation when evidence changes materially.
- **Verification:** Every displayed blocker also blocks mutation, both aliases behave identically, and concurrent reject/edit/approve tests cannot produce a rejected-yet-approved or stale-approved revision.
- **Difficulty / change risk:** **Medium–Large / High.** This can block previously accepted bulk workflows. Migrate operator tooling and document the override policy first.

**Repair record (H11)**

- Commit subject: `fix(H11): bind approval to reviewer policy and document revision`.
- Extracted `reviewer-policy-1`, shared by list/detail readiness and approval. All blockers (including raw critical citations and ingestion readiness) apply to both approval aliases. Approval requires the inspected revision, performs a conditional write, and records its policy; trusted retrieval also requires that policy version.
- Database triggers advance the revision and revoke approval/confirmation on material evidence or reference changes. Confirmation changes revoke stale approval. Metadata/reference replacement and explicit confirmation now share a bounded, revision-guarded D1 transaction, preventing approval between partial validation writes.
- Updated the reviewer client and all three approval operator scripts to send the inspected/confirmed revision. Missing revision returns 400, stale revision 409, and policy blockers 422. There is no exception bypass; actor authorization remains H01 work.
- Verified **34 behavioral tests, 84 source guards, 26 web tests**, API/shared/web typechecks, and an isolated actual Worker/D1 smoke: approval, metadata/reference commit, automatic revocation, stale 409, reapproval, and rejection all passed. Adversarial behavioral cases cover every list/detail blocker through both endpoints, interleaved reject/edit/confirmation/reference changes, stale transaction guards, and rollback after an injected batch failure.
- **Release/compatibility risk:** clients posting `{}` must migrate; legacy policy approvals do not confer current trust. Reference backfills can revoke approval and require review. Atomic review updates above 90 mutation statements fail before writes. [Approval contract and rollout notes](approval-policy.md) record the policy, limits, and rehearsal requirements. No production migration was executed. General nullable PATCH behavior remains M12, outside this goal.

### H12 — Reprocessing must preserve reviewer edits and publish a coherent new document revision

- [x] Implemented locally; staged corpus rehearsal required before release
- **Evidence:** Code-confirmed. [apps/api/src/services/admin-ingestion.ts:2120](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:2120), [apps/api/src/services/admin-ingestion.ts:2230](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:2230), [apps/api/src/services/admin-ingestion.ts:2270](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:2270), [apps/api/src/services/ingest.ts:147](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:147), [apps/api/src/services/ingest.ts:214](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:214).
- **Problem and cause:** Reprocessing overwrites author, case number, references, outcome, and extraction metadata from the source without distinguishing reviewed values. It usually does not rebuild existing paragraphs/chunks at all; artifacts rebuild only if a count is zero. If rebuilding is triggered, the statement list deletes old artifacts first and executes multiple committed batches. A later failure can leave partial text. Active retrieval chunks/vector jobs are not regenerated, and approval/confirmation remain in place. The comment about insert-before-delete safety in `ingest.ts` does not describe this actual path.
- **Best solution:** Introduce source and extraction revisions plus explicit reviewer overrides/provenance. Build replacement metadata/text/reference/chunk records in an unpublished generation, validate it, then atomically switch the active generation. Enqueue vector refresh by revision, retain the previous generation for rollback, and require renewed review when material data changes.
- **Verification:** Corrected author/reference values survive reprocessing; source/parser changes update the reader and both search indexes consistently; injected failures at every batch boundary preserve the previous visible document; concurrent reprocesses cannot mix generations.
- **Difficulty / change risk:** **Large / High.** This is a schema and lifecycle migration. Avoid a one-shot corpus rewrite; start with one document and compare output before gradual rollout.

**Repair record (H12)**

- Commit subject: `fix(H12): publish reprocessed documents through retained generations`.
- Added generation staging for metadata, source copies, sections, paragraphs, chunks, and reference validation, with source/artifact SHA-256 digests and counts. One revision-guarded D1 transaction retains the prior projection, swaps the replacement, updates FTS, deactivates old retrieval excerpts, and persists a generation-specific vector refresh job. Existing text is always rebuilt.
- Reviewer values now carry explicit provenance and survive reprocessing. Ambiguous historical edits are preserved as `legacy_preserved` and shown for review. Source/extractor metadata remains distinct. Every replacement/restore clears confirmation/publication/approval; rejection is never reversed.
- Added generation inspection and guarded rollback routes. Old queue messages/completions cannot alter a replacement generation's job. Search drops results captured across a revision change, and multi-query readers return a conflict instead of mixing metadata and text.
- Verified **43 behavioral tests, 84 source guards, 26 web tests**, API/shared/web typechecks, and the Worker bundle. Fault injection covers every staging/publication batch, concurrent reprocess and reviewer edits, corrupt source/text digests, extraction/storage failure, lost commit acknowledgment, rollback, reader consistency, and vector generation fencing. An isolated actual Worker/D1/R2 HTTP rehearsal passed for a **24-chunk** document, including stale rollback rejection, restored source download, and preservation of reviewed metadata.
- **Release risk/limits:** migration 0015 and these services must ship together with the other High lifecycle fixes. [Generation operations and rehearsal](document-generations.md) documents bounded inline processing, retained failed stages, legacy-value review, and restore semantics. No real-corpus rewrite or production migration was run. H14/H15 still complete job leasing and initial-ingestion outbox guarantees; large maintenance jobs and provider-vector garbage collection remain out-of-scope M03/M04.

### H13 — Validate activation before writing and make retries non-destructive

- [x] Implemented and locally verified (2026-10-26)
- **Evidence:** Code-confirmed; SQLite cascade reproduced. [apps/api/src/services/retrieval-activation.ts:114](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-activation.ts:114), [apps/api/src/services/retrieval-activation.ts:552](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-activation.ts:552), [apps/api/src/services/retrieval-activation.ts:685](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-activation.ts:685), [apps/api/migrations/0006_retrieval_activation.sql:12](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0006_retrieval_activation.sql:12).
- **Problem and cause:** Manifest compatibility is calculated but does not block writes. Checksums are stored without verifying payload content; trust is largely asserted by caller-supplied baseline/promoted IDs. “Provenance complete” checks field presence, not correspondence to persisted source text. Embedding/search rows can disagree about their document. Reusing a batch ID executes `INSERT OR REPLACE` on a parent with cascading foreign keys, deleting the previous child rows before the replacement work is complete. Activation then performs vector and database writes incrementally with no published-generation boundary.
- **Best solution:** Strictly validate a complete activation schema and server-side eligibility before any mutation. Bind a cryptographic digest to source revision, text, chunk/document IDs, and rollback plan; reject inconsistent/mismatched rows. Treat a batch ID as immutable/idempotent: identical replay resumes/returns the same job, different content conflicts. Stage a generation, verify it, then publish. Keep rollback targets server-owned and version-specific.
- **Verification:** Wrong checksums/rollback sets, cross-document rows, fabricated source text, duplicate IDs, same-ID different payloads, and interruptions after each storage step. A replay must preserve existing data and converge. In the current schema, the local reproduction reduced activation child rows from one to zero after replacing their batch parent.
- **Difficulty / change risk:** **Large / High.** Requires changes to activation scripts and persisted manifests. Preserve old manifests and support deliberate compatibility conversion.

**Repair record (H13)**

- Commit subject: `fix(H13): validate and atomically publish immutable activation plans`.
- Added strict v2 preparation/write/rollback contracts. The server derives eligibility, exact text/anchors, source hashes and revisions, chunk identities, and rollback targets. Caller trust labels and unverified checksums no longer authorize publication. Initial extraction captures its source SHA-256; unknown legacy provenance requires reprocessing/review.
- Immutable plans and batch-specific chunks stage behind an exclusive lease. Identical retries resume or report historical completion; different content conflicts. Vector-backed publication requires stored vector identity verification, then one guarded D1 transaction switches the complete selection. Search fences concurrent retrieval revisions. Parent/child rows are retained rather than replaced.
- Rollback uses only stored, version-specific targets and verifies retained evidence before the atomic switch. Missing/corrupt history, concurrent edits, stale owners, and newer activations fail closed. Added a review-first operator CLI, explicit legacy evidence conversion, and immediate retirement guards on old writer scripts; existing reports are preserved.
- Verified **54 behavioral tests, 84 source guards**, API typecheck, and Worker bundle (**694.47 KiB gzip**). Fault injection covers forged/different/cross-document plans, interrupted staging/vector/publication/rollback, lost acknowledgments, stale ownership, concurrent revisions, changed sources, delayed vector visibility and incomplete retained projections. Actual isolated Worker/D1/R2 HTTP and CLI prepare/publish/replay/rollback rehearsals passed.
- **Release risk/limits:** migration 0016 and v2 clients must ship together. [Activation operations and compatibility conversion](activation-generations.md) documents bounded batches, legacy reconciliation, retries and retained rollback. No production resources or historical reports were rewritten. Real-corpus sizing and real-provider visibility still require an isolated release rehearsal; authentication remains deferred H01.

### H14 — Give vector jobs exclusive leases, durable attempts, and recovery for abandoned work

- [x] Implemented and locally verified (2026-10-26)
- **Evidence:** **Reproduced** duplicate claims; remaining behavior code-confirmed. [apps/api/src/services/vector-jobs.ts:73](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/vector-jobs.ts:73), [apps/api/src/services/vector-jobs.ts:113](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/vector-jobs.ts:113), [apps/api/src/index.ts:157](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:157).
- **Problem and cause:** Claiming uses `state != 'completed'`, so queued, processing, and failed jobs can all be claimed again. Two concurrent calls both claimed the same synthetic job. There is no lease token on completion/retry/failure writes. The sweep only considers queued rows, leaving abandoned processing rows without application-level recovery. The queue handler uses delivery attempts rather than the durable attempt count, while sweeps can create new messages and reset the delivery history.
- **Best solution:** Atomically claim only eligible queued work or expired leases; record owner token, revision, attempt count, lease expiry, and next-attempt time. Guard all terminal writes with the owner token. Recover expired processing jobs, apply a durable retry budget with backoff/jitter, and expose failed/dead-letter work to operators. Duplicate deliveries should acknowledge without doing duplicate computation.
- **Verification:** Concurrent duplicates, lost acknowledgments, process termination, lease expiration, slow embeddings, retries after partial upserts, exhausted durable attempts, and stale-worker completion.
- **Difficulty / change risk:** **Large / High.** Queue/job schema rollout must account for already queued messages and existing stuck jobs.

**Repair record (H14)**

- Commit subject: `fix(H14): lease vector jobs and persist bounded retry recovery`.
- Added exclusive owner tokens, renewable expirations, claimed document revisions, next-attempt timestamps, durable failure counts and chunk checkpoints. Every progress/completion/failure write is fenced; abandoned owners are recovered with backoff, and exhausted jobs remain terminal.
- Queue delivery attempts no longer control the retry budget. Duplicate messages acknowledge without duplicate computation; infrastructure failures retry delivery, while persisted failures/continuations are recovered by the scheduled sweep. Four concurrent embeddings, 16-vector upserts, 15-second provider bounds and 64-chunk deliveries keep work bounded. Acknowledged partial batches resume without re-embedding.
- Added paginated `/admin/vector-jobs` inspection for failed and pending work. Reprocessed generations reset their own lease/progress fields without allowing stale workers to mutate them. Migration preserves the consumed legacy retry budget conservatively.
- Verified **64 behavioral tests, 84 source guards**, API typecheck, and isolated workerd/D1/R2 execution with instrumented AI/vector providers: two concurrent invocations produced one claim and completed **24 chunks**, replay skipped, and operator listing/input validation passed. Behavioral cases cover durable exhaustion across fresh deliveries, abandoned/slow owners, stale terminal writes, partial upserts, productive continuation, lost acknowledgments, backoff, malformed messages, and a provider upsert that never resolves.
- **Release risk/limits:** migrate and replace the old consumer together; [job operations and recovery](vector-job-recovery.md) describes lease/attempt semantics and rollout. No production queue/provider configuration changed. H15 still completes the initial-ingestion outbox/manifest contract and must ship with the lifecycle changes.

### H15 — Publish vector jobs only after ingestion artifacts are committed and complete

- [x] Implemented and locally verified (2026-10-26)
- **Evidence:** Code-confirmed interleaving. [apps/api/src/services/ingest.ts:452](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:452), [apps/api/src/services/ingest.ts:461](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:461), [apps/api/src/services/vector-jobs.ts:73](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/vector-jobs.ts:73), [apps/api/src/services/vector-jobs.ts:123](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/vector-jobs.ts:123).
- **Problem and cause:** The vector job is inserted as queued in the first database batch, before all text batches and the R2 write finish. The scheduled sweep considers any queued row with null `enqueued_at` immediately eligible. It can dispatch an import still in progress. The consumer can see zero or partial chunks, mark the job completed, and later skip the post-ingest message because completed jobs are terminal. This is an outbox-publication race even if queue deduplication is fixed.
- **Best solution:** Persist preparation state separately, then commit an explicit ready-to-publish transition only after expected artifacts and source storage are verified. Include document revision and expected chunk count/hash in the job. The consumer must refuse an incomplete or superseded generation. Ensure cleanup/sweeps cannot publish a compensated import.
- **Verification:** Pause ingestion after its first batch, run the sweeper/consumer, finish ingestion, and verify only the completed generation is embedded. Repeat with ingestion failure and compensation.
- **Difficulty / change risk:** **Medium / High.** Coordinate with H10/H12/H14; an incomplete migration could strand existing jobs.

**Repair record (H15)**

- Commit subject: `fix(H15): commit verified vector outboxes with complete ingestion`.
- Removed queued jobs from initial text preparation. Source read-back hash, persisted chunk count and ordered SHA-256 chunk manifest are verified before a final transaction publishes both document readiness and the outbox. Reprocess/restore share the same generation-specific outbox writer.
- Enqueue, sweep and claim require verified outbox readiness. Consumers verify current generation/source identity and exact source/chunk manifests before embedding and before finishing a delivery. The job records its preparation revision; H14 separately fences the current document revision during processing. Missing, altered or superseded evidence cannot complete a job.
- Compensation now deletes only untouched preparing imports. A lost commit acknowledgment is reconciled against the ready document/outbox receipt, and an unknown database outcome preserves evidence rather than risking deletion of committed data. Failed queue sends remain recoverable through the durable sweep.
- Verified **71 behavioral tests, 84 source guards**, API typecheck and isolated workerd/D1/R2 publication/consumption of **24 chunks** with instrumented providers. Tests interleave sweep/consume after every text batch, inject storage/publication failures, simulate lost/unknown acknowledgments, verify manifest corruption rejection and exercise legacy migration holds.
- **Release risk/limits:** migration 0018 deliberately holds unfinished legacy jobs without verified manifests; it does not invent their provenance. [Outbox rollout and recovery](vector-ingestion-outbox.md) documents old-consumer shutdown, targeted reprocessing and ambiguous-outcome inspection. No production data migration or cloud provider writes were run.

### H16 — Replace global legal-reference data through versioned publication, not delete-and-restore

- [x] Implemented and locally verified (2026-10-26)
- **Evidence:** Code-confirmed. [apps/api/src/services/legal-references.ts:145](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/legal-references.ts:145), [apps/api/src/services/legal-references.ts:179](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/legal-references.ts:179), [apps/api/src/services/legal-references.ts:556](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/legal-references.ts:556).
- **Problem and cause:** Rebuild snapshots the old tables into Worker memory, clears live reference tables, and inserts replacements across batches. Readers and ingestion can observe an empty or partial catalog. The recovery snapshot is not durable; termination can prevent restoration. Concurrent rebuilds can restore over each other's newer state. Existing document reference links/issues are not tied to a catalog version and remain stale until a separate backfill.
- **Best solution:** Write a new reference-set version to staging tables, validate counts/uniqueness/coverage, then switch the active version in one transaction. Keep the previous version and a durable job log. Build document validation against a specific reference version and publish its refreshed results deliberately. Serialize catalog publication.
- **Verification:** Kill the rebuild between batches, read concurrently, run conflicting rebuilds, and validate documents during rebuild. Readers should see one complete version throughout; rollback should not require an in-memory snapshot.
- **Difficulty / change risk:** **Large / High.** Changes reference keys, validation, and operational import procedures. Preserve exact critical-citation semantics during migration.

**Repair record (H16)**

- Commit subject: `fix(H16): publish retained legal catalogs and version reference validation`.
- Replaced memory-snapshot/delete-and-restore rebuilding with immutable staged catalogs, verified count/hash manifests, durable publication history and an epoch-guarded atomic projection switch. Prior versions survive failures and explicit rollback; identical active payload replay is a no-op. Direct legacy writes to live legal projections fail outside publication.
- Document links/issues and generation snapshots now identify their validation catalog. Stale/unverified validation blocks approval and trusted retrieval, including concurrent approval/activation writes. Explicit reference refresh is atomic per document, preserves its prior validation history and revokes obsolete approval/confirmation. No bulk corpus promotion occurs on catalog publication.
- Multi-query readers detect catalog switches, lookup maps pin one immutable set, and prior draft provision links resolve retained published evidence. Added version inspection/guarded restore endpoints. Pre-H16 document generation manifests remain restorable with explicitly unverified catalog provenance.
- Verified **80 behavioral tests, 84 source guards** and API typecheck. Fault cases cover every staging/publication boundary, competing rebuilds, lost acknowledgments, exact replay, pinned validation, reader/approval races, transactional refresh failure and old generation formats. Actual isolated workerd/D1/R2 HTTP publication, stale validation, backfill, stale rollback rejection and restoration passed.
- **Release risk/limits:** quiesce old writers before migration 0019; existing validation is intentionally held as unverified until targeted refresh/review. [Catalog rollout, history and rollback](legal-catalog-versions.md) documents compatibility, bounds and legacy-tool changes. No production migration or corpus rewrite was performed. Effective legal editions and general maintenance remain separate Medium findings.

### H17 — Isolate local development and previews from the production vector index

- [x] Implemented and locally verified (2026-10-26)
- **Evidence:** Code-confirmed configuration; actual remote effects depend on the command/credentials used. [apps/api/wrangler.toml:80](/Users/cliftonnoble/Code/beedle-ai/apps/api/wrangler.toml:80), [apps/api/wrangler.toml:86](/Users/cliftonnoble/Code/beedle-ai/apps/api/wrangler.toml:86), [apps/api/src/services/vector-jobs.ts:138](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/vector-jobs.ts:138), [apps/web/src/lib/api.ts:35](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/lib/api.ts:35).
- **Problem and cause:** The default configuration uses a remote Vectorize binding named `beedle-docs` and the same namespace while local D1/R2 state can differ. Code paths for ingestion, activation, and vector backfill write through that binding. Development is therefore not reliably isolated: local IDs/text can pollute a production vector namespace or incur remote AI costs. Production-mode preview builds also default to the production API when their URL variable is missing.
- **Best solution:** Require explicit environment configuration; assign independent D1/R2/queue/vector resources or namespaces and identity policies to test/preview/production. Make local writes incapable of addressing production by default. Fail the build/start on missing required environment selection rather than falling back to production. If deliberately retaining one production environment, provide an entirely isolated disposable test environment for state-changing checks.
- **Verification:** Run local ingestion/backfill with instrumented bindings and prove all writes target test resources; verify preview API configuration and service credentials. Inventory existing vectors before cleaning up suspected contamination.
- **Difficulty / change risk:** **Medium–Large / High.** Incorrect rebinding can make production appear empty. Export configuration and verify resource IDs/namespace coverage before any switch.

**Repair record (H17)**

- Commit subject: `fix(H17): isolate local and preview deployments from production`.
- Default Worker config now contains only disposable local bindings and omits AI/Vectorize. Every entrypoint validates environment readiness/namespace; local service environments remove remote capabilities and leftover LLM secrets. Production resource identities and Worker name are retained under explicit `env.production`; preview has independent names, namespace and rate limits and stays disabled until its real resources/URLs are configured.
- Explicit deployment validation rejects placeholders and preview resources shared with production. CI deploy/migration commands select production deliberately. Web builds require an explicit environment; preview cannot use the production API, local mode requires loopback, and Pages branches must match the declared environment. Production no longer permits local/feature-preview CORS origins.
- Verified 84 API behavioral tests, 84 source guards, 28 web tests, API/web typechecks, generated binding types, local/production Worker dry-run bundles and an explicit preview Next build. A missing preview API fails the real build. Instrumented ingestion/queue/backfill made zero remote calls; actual local workerd/D1/R2 ingestion and backfill confirmed absent AI/vector bindings.
- **Release risk/limits:** update Pages build variables and review exact production bindings before rollout. No cloud resources, production inventory/cleanup, migration or deployment were performed. [Environment setup and rollout](environment-isolation.md) covers preview provisioning/secrets, namespace coverage, local state changes and rollback. Real-provider preview verification requires those separately provisioned resources; H01 remains deferred.

### H18 — Bind reviewer forms and mutation completions to the selected document

- [x] Implemented and locally verified (2026-10-26)
- **Evidence:** Code-confirmed. [apps/web/src/app/admin/ingestion/page.tsx:350](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/admin/ingestion/page.tsx:350), [apps/web/src/app/admin/ingestion/page.tsx:494](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/admin/ingestion/page.tsx:494), [apps/web/src/app/admin/ingestion/page.tsx:833](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/admin/ingestion/page.tsx:833).
- **Problem and cause:** Selecting B leaves A's detail/form visible and actionable while B loads; actions use `selectedId`, not the displayed detail's identity. There is also a second race: a mutation started on A can finish after selection changes to B and call `loadDetail(A)`. That becomes the latest detail request and defeats the existing request-sequence guard, filling A's fields while `selectedId` remains B. Saving at that point can write A's metadata onto B. Loading and mutation state do not disable these actions.
- **Best solution:** Model the form as `{documentId, revision, values}` and require that identity to match the current selection before enabling or executing a mutation. Clear or make stale detail read-only while loading. Apply a mutation's result only if its document/revision still matches; otherwise refresh its list entry without replacing the selected pane. Combine this with the server revision checks in M12.
- **Verification:** Browser tests with delayed responses: select A then B; start save/approve on A then select B; fail B's load; submit repeatedly. No action may apply displayed A data to B, and a stale completion cannot replace B's form.
- **Difficulty / change risk:** **Medium / Medium.** Preserve unsaved-change handling and clear success/error messages for the document actually changed.

**Repair record (H18)**

- Commit subject: `fix(H18): bind reviewer forms and completions to document sessions`.
- Replaced independent form values with a document/revision snapshot and synchronous selection/request/mutation fences. Changing selection clears the old pane immediately; failed or mismatched loads cannot expose actionable stale data. All actions capture the displayed identity and values, and repeat submissions are blocked before React rerenders.
- Late success refreshes the list using current filters but reloads detail only in the original review session. A → B → A requires explicit reload after the earlier mutation, including invalidating an in-flight stale detail read. Mutation notices identify the affected document. Unsaved-change cancellation, save-error value retention, explicit retry/reload and disabled pending controls are preserved or added.
- Verified **32 web tests**, workspace typechecks and nine actual-browser scenarios with controlled delayed/failing API responses. Save, approve and reject all retained A's identity after switching to B; rapid repeated clicks sent one write; B failure left no A form; failed saves preserved edits. Pages production-mode build uses an explicit isolated preview API.
- **Release risk/limits:** no migration required. [Reviewer session behavior and reproducible browser checks](reviewer-session-safety.md) records the model and rollback. Separate-reviewer metadata/rejection concurrency and idempotency remain M12, deliberately outside this High-only scope; approval retains H11's server revision check.

## Medium severity

### M01 — Close gaps in endpoint cost controls

- [x] Implemented and locally verified (2026-10-26); verified-identity quotas remain deferred with H01.
- **Evidence:** Code-confirmed. [apps/api/src/index.ts:181](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:181), [apps/api/src/index.ts:76](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:76), [apps/api/src/index.ts:97](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:97), [apps/api/src/services/draft-template.ts:31](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-template.ts:31).
- **Problem and cause:** `/admin/draft/debug` performs research plus an LLM call without the LLM limiter. `/api/draft/template` can execute the multi-search case-assistant pipeline without a limiter, even for a blank scaffold that will discard the authorities. The legacy `/decisions/:id/approve` alias bypasses the admin-write bucket. Expensive GET reviewer exports/previews are outside the POST-only mapping. IP-only controls also conflate users behind a shared network and provide no account-wide spend budget.
- **Best solution:** Declare permissions and cost class alongside each route; test that every expensive route and alias is classified. Skip retrieval for scaffold modes that do not need it. Add identity-aware quotas after H01, bounded work per request, and an overall AI/job spend control. Keep IP controls as an additional layer.
- **Verification:** Route inventory tests plus actual 429 behavior for every expensive alias; blank scaffolds make no search/AI calls; normal judge-office traffic is not starved by one shared IP.
- **Difficulty / change risk:** **Small–Medium / Medium.** Coordinate limits with imports, service accounts, and frontend retry behavior.

**Repair record (M01)**

- Commit subject: `fix(M01): declare route costs and enforce shared provider budgets`.
- Each route declares its cost beside its handler, and enforcement wraps the actual router match. Covered missing debug/template/approval aliases and expensive GET reads/exports, with separate review/export buckets. Blank scaffolds now perform zero retrieval/provider work even when contextual text is supplied.
- Added atomic D1 application/environment minute/day budgets for LLM, embedding and vector-write attempts, enforced before outbound work across HTTP and background services. Invalid policy/missing schema fails closed. Quota exhaustion defers vector jobs without consuming provider-failure retries; counter inspection is read-only.
- Verified API behavioral/source suites, web/util suites, API typecheck and production Worker dry run. Actual isolated workerd/D1 rate-limit tests returned 429 for all eight omitted route categories; a synthetic loopback provider received only one completion across two client IPs under a one-call shared cap. No paid/production providers were contacted.
- **Rollout/limits:** apply migration 0020 and configure the new bindings/budget JSON before deployment. [Cost-control policy, units, tests and rollback](compute-cost-controls.md) explains conservative charging and model/traffic calibration. Trusted-user/office identity and authorization remain H01; IP counters are only an additional burst control. Work-unit ceilings are not a dollar-accurate account-wide billing cap.

### M02 — Bound the full LLM operation, including response consumption

- [x] Implemented and locally verified (2026-10-26).
- **Evidence:** Code-confirmed. [apps/api/src/services/assistant-chat.ts:240](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:240), [apps/api/src/services/draft-conclusions.ts:326](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:326), [apps/api/src/services/embeddings.ts:26](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/embeddings.ts:26).
- **Problem and cause:** Both LLM clients clear their abort timer as soon as fetch returns response headers; `response.json()` and error-body reads happen afterward. A provider can send headers promptly and stall the body indefinitely. Error bodies are fully buffered before truncating the logged string. Assistant chat specifies no output-token cap. The embedding timeout returns null without recording timeout as a failed/degraded vector attempt, making diagnostics ambiguous.
- **Best solution:** Keep the deadline active through bounded body consumption and parsing, enforce response-byte and output-token limits, and validate the provider response shape. Return structured timeout/degradation reasons. Propagate request cancellation and a total request deadline where supported; preserve the current grounded fallback.
- **Verification:** Mock headers followed by a stalled stream, oversized response/error bodies, malformed responses, timeouts, and provider failures. Assert bounded completion time and explicit fallback reasons.
- **Difficulty / change risk:** **Medium / Medium.** Strict limits can truncate legitimate long answers; choose measured budgets and handle truncation explicitly.

**Repair record (M02)**

- Commit subject: `fix(M02): bound provider response consumption and report degradation`.
- Both completion clients now use one reader with an 18-second deadline spanning headers, streaming reads and validation. Request cancellation is propagated; the provider receives only the time remaining in the 40-second service deadline after research. Success/error bodies are limited to 128/8 KiB, answer text to 40,000 characters, and output to 1,200 assistant / 900 draft tokens. Oversized, malformed, empty, filtered or token-truncated completions return the existing review/extractive fallback with a safe reason; upstream bodies are never logged or returned.
- Embedding outcomes now distinguish unavailable bindings, timeout, cancellation and invalid vectors. Search records failed/degraded embedding attempts instead of reporting a successful empty semantic result. Binding work cannot be cancelled by the platform API; its late settlement is safely observed and conservatively charged.
- Verified fault injection for stalled headers/body, lying/missing lengths, error-body limits, malformed JSON/shape, token truncation, network failures, cancellation and expired budgets; draft assembly retains its review scaffold. API behavioral/source suites, web tests and workspace typechecks pass; production Worker bundle dry run passes. An isolated workerd rehearsal completed a synthetic success in 8 ms, cancelled a stalled body at 201 ms under a 200 ms test budget, and rejected an oversized body in 3 ms. No paid or production provider calls.
- **Rollout/limits:** no migration required. Review fallback rates and tune limits only with measured answer lengths. The service deadline prevents a late provider start after research consumes its budget; D1/retrieval deadlines and query-work limits are addressed separately in M06. [Cloudflare response streaming](https://developers.cloudflare.com/workers/runtime-apis/response/) and [provider output parameters](https://openrouter.ai/docs/api_reference/parameters) informed the implementation. Rollback restores the prior unbounded body-read behavior.

### M03 — Move remaining maintenance work to resumable bounded jobs

- [x] Implemented and locally verified (2026-10-26).
- **Evidence:** Code-confirmed. [apps/api/src/services/retrieval-vector-backfill.ts:6](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-vector-backfill.ts:6), [apps/api/src/services/retrieval-vector-backfill.ts:101](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-vector-backfill.ts:101), [apps/api/src/services/retrieval-activation.ts:723](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-activation.ts:723), [apps/api/src/services/legal-references.ts:1332](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/legal-references.ts:1332).
- **Problem and cause:** Vector backfill permits 5,000 rows from each of two sources and embeds them sequentially inside one HTTP invocation. Activation similarly embeds/upserts per chunk in the request. A failure after some batches commits partial progress without a durable per-item checkpoint. Backfill `dryRun` still calls the embedding model; it only skips upserts. OFFSET-based loaders lack a complete tie-break ordering and can skip/repeat rows while the corpus changes.
- **Best solution:** Return an accepted job ID, use fixed-size work units with durable cursors/checkpoints, and expose progress/retry/cancel semantics. Use stable keyset cursors bound to a corpus revision and separate eligibility planning from actual inference. Make dry-run perform validation/planning only unless a paid inference probe is explicitly selected. Apply the same approach to large reference backfills.
- **Verification:** Resume after failure without redoing successful work; concurrent ingestion cannot shift pagination; dry-run performs no AI/upsert work; job totals reconcile with source revision counts.
- **Difficulty / change risk:** **Large / High.** Scripts currently expect synchronous result bodies; introduce a compatibility client and job polling before removing that contract.

**Repair record (M03)**

- Commit subject: `fix(M03): checkpoint bounded maintenance jobs and add polling clients`.
- Vector/reference backfills now create finite immutable D1 manifests and return accepted job IDs. Steps read at most four items, with owner leases, source/catalog/model preconditions, successful/submitted checkpoints, durable retry budgets, cancellation and progress diagnostics. Nonzero OFFSET is rejected; source keysets and a frozen per-job selection prevent concurrent-ingestion shifts. Dry-run plans perform zero inference/upserts.
- Activation reuses H14's source-bound immutable plan and staged publication, with four vector candidates per delivery and evidence rechecks between units. API callers receive durable IDs; maintained CLI writers use a shared bounded polling/resume client. The scheduled handler advances a limited recovery batch.
- Verified partial provider failure, asynchronous visibility without re-embedding acknowledged work, competing owners, cancellation, stale source skips, catalog changes, atomic reference checkpoint rollback, exhausted leases, shared-budget deferral, cursor advancement and HTTP activation acceptance. API behavior/source suites and typecheck pass; Worker production bundle dry run passes. Actual isolated workerd/D1/R2 verified 202 acceptance, encoded status URLs, reference checkpoint/replay, cancellation, zero-inference dry-run and malformed-JSON 400 responses.
- **Rollout/limits:** apply migration 0021 before API/CLI rollout. [Job contracts, recovery, snapshot limits and rollback](maintenance-jobs.md) describe coordinated client migration, inspection of held visibility failures, and the unavoidable unknown-acknowledgement window for paid calls. Each job is a finite snapshot; a multi-job sweep is not one transaction across future ingestion. In-flight cancellation does not undo completed writes, and authentication remains H01.

### M04 — Track vector generations and clean up obsolete embeddings

- [x] Implemented and locally verified (2026-10-26); external index provisioning/relevance remain release gates.
- **Evidence:** Code-confirmed. [apps/api/src/services/vector-jobs.ts:123](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/vector-jobs.ts:123), [apps/api/src/services/embeddings.ts:41](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/embeddings.ts:41), [apps/api/src/services/retrieval-activation.ts:988](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-activation.ts:988). No runtime service calls `VECTOR_INDEX.deleteByIds`.
- **Problem and cause:** Job identity is only document ID; completion does not identify the text/chunking/model version. Model changes can mix incompatible embedding spaces under one namespace. Rejection/rollback/reprocessing leaves obsolete vectors in the index. D1 filtering can suppress an inactive vector from the response, but it still occupies a nearest-neighbor slot and storage. A whole document's embeddings are accumulated in memory and a failed attempt re-embeds the full document.
- **Best solution:** Version the embedding model, dimensions, parser/chunker, and source revision. Use generation-specific IDs/namespaces, page chunk work, batch model inputs where supported, and checkpoint successful chunks. Publish only verified generations; asynchronously garbage-collect obsolete vectors with a retention period and reconciliation report. Validate dimensions and finite vector values before writes.
- **Verification:** Model migration, changed chunks, rejection/rollback, partial retry, and long-document tests. Reconcile eligible D1 chunk IDs with vector IDs and demonstrate that old generations do not consume live recall slots.
- **Difficulty / change risk:** **Large / High.** Re-embedding and namespace switches can be expensive and change rankings. Shadow-query and retain the previous index for rollback.

**Repair record (M04)**

- Commit subject: `fix(M04): version vector generations and reconcile their lifecycle`.
- Added model/dimension/pipeline namespaces and immutable source/parser/chunker/generation receipts with separate physical submission incarnations. Document jobs batch model inputs, cache successful embeddings, checkpoint acknowledged/verified work and freeze the embedding profile. Activation plans include the profile digest. All writers validate finite, exact-dimensional vectors and preserve existing source/lease/publication guards.
- Durable per-profile synchronization removes obsolete generations through an indexed live filter before top-K; a dirty profile or corpus change during a query reports semantic degradation. Source identity remains authoritative in D1. Serialized metadata mutations and held unknown acknowledgements prevent promotion/retirement races; uncertain initial writes and restored vectors use fresh physical IDs.
- Added bounded reconciliation/provider-page inspection, shadow comparison tooling, 30-day asynchronous garbage collection with observed deletion, and protection for current entries/published rollback plans. Legacy unversioned namespaces remain excluded and retained for deliberate inventory/rollback. Trigger-inclusive actual D1 checks caught and fixed false approval conflicts/inflated publication counts; the test adapter now reproduces those semantics.
- Verified full API behavior/source suites, workspace typechecks and Worker bundle dry run, including cache reuse, concurrency, lost acknowledgements, delayed visibility, profile changes, rejection/restoration, rollback retention and provider discrepancies. Actual isolated workerd/D1/R2 approved a synthetic decision, verified four profile-scoped vectors, reconciled them, retired them after rejection and preserved them within retention; no paid/production providers were contacted.
- **Rollout/risk:** migration 0022, the provider's `live` metadata index, dimension/binding checks, full coverage/provider reconciliation and independent relevance review precede promotion. Saved deployment readiness remains false until those external prerequisites are met. [Lifecycle operations, shadow tests, retention, held-acknowledgement recovery and rollback](vector-generations.md) detail the high-risk migration. H01/authentication remains deferred.

### M05 — Apply semantic-search filters before the global top-K cutoff

- [x] Implemented and locally verified (2026-10-26); metadata-index provisioning and real-corpus recall/latency remain release gates.
- **Evidence:** Code-confirmed. [apps/api/src/services/search-fts.ts:1276](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-fts.ts:1276), [apps/api/src/services/search.ts:769](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:769).
- **Problem and cause:** Vectorize receives only the query vector, namespace, and top-K. Judge, index-code, document, jurisdiction, and corpus eligibility filters are applied when hydrating those globally selected IDs from D1. A narrow filter can discard every global neighbor even when strong matches exist inside that filter. Increasing an already capped top-K is not a reliable fix. The namespace-error fallback also performs an unscoped query based on matching error text rather than an explicit environment policy.
- **Best solution:** Materialize supported filter metadata/indexes and apply eligible constraints in the vector query; retain D1 as the final authority. For filters that cannot be represented, use a deliberate partition/fallback strategy with recall evaluation. Remove production namespace-dropping fallback and make any local compatibility mode explicit.
- **Verification:** Construct a corpus where the relevant filtered document ranks below 100 globally but first within the filter. Check combined filters, rejected documents, environment isolation, and recall/latency before and after.
- **Difficulty / change risk:** **Large / High.** Requires metadata backfill and relevance evaluation; stale vector metadata must never override D1 access policy.

**Repair record (M05)**

- Commit subject: `fix(M05): scope semantic neighbors before the top-K cutoff`.
- Product search now passes its accepted D1 scope into semantic retrieval. Projected scalar metadata covers judge, jurisdiction, document, file type, trust, date, series and chunk section. Exact D1-derived reference cohorts preserve index-code/rules/ordinance membership without pretending the provider indexes string arrays. Substring/unrepresentable scopes use exact document partitions; oversized scopes report deliberate semantic degradation with zero unscoped provider work.
- Added the ten-field metadata-index manifest, a readiness gate, bounded byte/fan-out/query deadlines, and observed scope verification. Separate v2 filter checkpoints plus document/facet/catalog invalidation prevent stale projections and older Workers from falsely acknowledging them. M04's namespace-dropping removal and authoritative D1 hydration remain enforced.
- Verified a target below 120 global distractors is recalled with its judge filter; combined filters, AND/OR codes, party/date/section scopes, metadata/catalog/facet changes, empty scopes, provider errors, prefix collisions, old checkpoints and partition caps pass. Full API behavior/source suites, workspace typechecks and Worker dry run pass. Actual local workerd/D1/R2 sent scoped filters through `/search` with 120 synthetic distractors and skipped empty scopes without a provider call; no paid/production providers were contacted.
- **Rollout/limits:** migration 0023 and all declared provider metadata indexes precede metadata backfill and `VECTOR_FILTERS_READY=true`; saved environments remain false. [Filter semantics, cohort proof, bounded fallback and rollout/rollback](vector-filtering.md) explain the 112-membership/four-partition limits and required real-corpus holdout/latency checks. Authentication remains H01.

### M06 — Finish the indexed search path and put a budget on expensive fallbacks

- [x] Implemented and locally verified (production relevance/scale gates remain)
- **Evidence:** Code-confirmed. [apps/api/src/services/search-lexical-sql.ts:12](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-lexical-sql.ts:12), [apps/api/src/services/search-fts.ts:875](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-fts.ts:875), [apps/api/src/services/search.ts:429](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:429), [apps/api/src/services/search.ts:654](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:654).
- **Problem and cause:** Remaining fallbacks calculate `instr(lower(...))` across chunk text and metadata, with repeated ranking expressions. FTS indexes chunk text/section labels but not title, citation, or author; comments correctly acknowledge that some metadata matches are unreachable through FTS, forcing scans for parity. Scoped search often issues serial four-document batches. Layered retries/fallbacks can amplify cost, and `search()` can restart the full pipeline on a retryable error. There is no shared end-to-end query-work budget.
- **Best solution:** Design indexed retrieval channels for body text and searchable metadata, then combine bounded candidate sets. Preserve exact metadata lookup separately from broad text ranking. Measure query plans and rows read for representative classes; bound fallback stages by deadline and query count. Reuse per-request candidates and avoid repeated full-pipeline retries. Evaluate bounded parallel work only where it actually reduces wall time; D1 itself is serial per database.
- **Verification:** Warm/cold tests for common, rare, zero-hit, metadata-only, quoted, and combined-filter queries, measuring p50/p95/p99, rows read, and relevance. Remove a scan only after its unique recall cases have indexed coverage.
- **Difficulty / change risk:** **Large / High.** Retrieval refactoring can silently change the result set. Use shadow comparisons and an independently judged holdout corpus.

**Repair record (M06):**

- Added source-backed body and metadata trigram recall with bounded candidates and stable identities. Preserved inside-word and metadata-only matches, removed the broad substring rank/keyword/issue scans and serial four-document phrase loops, and retained structured scope/publication checks.
- Added a guarded, resumable 100-identity build for populated upgrades; fresh installs are ready and incomplete upgrades return 503. Live writes maintain indexes during backfill. The full search retry is gone; one correction and all HTTP research searches share a sticky work/deadline/cancellation budget with safe failures and numeric debug usage.
- Behavioral SQL fixtures cover unique recall cases, query plans, scoped filters, backfill/replay/live edits, lease conflicts and injected transaction failure; request budget tests cover limits, cancellation and no restart. Actual workerd/D1/R2 verified metadata-only product recall and retained filtered vector recall below 120 distractors. Local tiny-corpus debug calls took 2–14 ms with 3–5 statements and 9–47 reported rows read; these are not production SLOs.
- **Rollout/risk:** [Indexed-search limits, build procedure and rollback](indexed-search-budgets.md). Trigram size/write cost, candidate-cap recall, short-input contract and independent real-corpus relevance/latency checks are release gates. D1 cannot cancel an already-running statement. M07–M10 retain their separate index-maintenance, pagination, runtime-bootstrap and restore tasks; authentication remains H01.

### M07 — Give FTS rows stable integer identities for efficient updates and deletes

- [x] Implemented and locally verified (preview scale/parity gates remain)
- **Evidence:** SQLite query-plan reproduction. [apps/api/migrations/0008_search_fts.sql:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0008_search_fts.sql:1), [apps/api/migrations/0008_search_fts.sql:28](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0008_search_fts.sql:28).
- **Problem and cause:** FTS maintenance deletes by `source_kind` and `chunk_id`, both declared UNINDEXED. EXPLAIN shows an unconstrained FTS virtual-table scan for this delete, whereas a rowid delete uses the rowid constraint. Reprocessing or bulk cleanup can scan the complete FTS store for each deleted/updated chunk. The self-contained FTS table also duplicates text already held in several relational representations.
- **Best solution:** Introduce a stable integer rowid mapping or a consolidated search-content table with an external-content FTS index and rowid-based maintenance. Keep primary/retrieval chunk identities unambiguous. Rebuild and validate into a new index before switching readers. Account for metadata changes that affect indexed title/citation/author fields under M06.
- **Verification:** EXPLAIN and measured bulk update/delete cost should scale with changed rows; insert/update/delete/cascade tests must keep FTS and source counts/content synchronized. Validate crash recovery and index rebuild.
- **Difficulty / change risk:** **Large / High.** A search-index migration with backfill and trigger changes. Keep the old index until parity is established.

**Repair record (M07):**

- Reused stable integer identities for a source-backed external-content phrase index. Transactional source triggers maintain exact OLD-token deletions by rowid; active/unindexed fields read current source values, with distinct identities for primary/retrieval chunks and immutable rowid/source kinds.
- Added a leased, bounded shadow build that retains the old live reader until complete coverage, then atomically switches the canonical FTS name, vocabulary and rowid triggers. Failed cutovers roll back; lost acknowledgments and partial backfills replay safely. Legacy maintenance freezes at cutover, avoiding the old per-chunk FTS scans while retaining its snapshot.
- Verified insert/update/move/delete/cascade/generation paths, active toggles, populated upgrade, query plans, integrity, replay and cutover failure injection. Actual populated workerd/D1/R2 upgrade completed its cutover, preserved phrase results, and indexed new reviewed writes. Full API behavioral/source checks pass; workspace typechecks and Worker dry run are recorded with this commit.
- **Rollout/risk:** [Build/cutover, integrity checks, archive and rollback](stable-fts-identities.md). Both substring and phrase readiness must complete. The legacy archive is a frozen snapshot and retains storage overhead until explicitly retired; never blindly swap it over newer data. Real-corpus size/write latency/parity checks remain release gates. M10 separately extends restore/corruption repair; authentication remains H01.

### M08 — Make pagination stable and distinguish result counts from bounded candidates

- [x] Implemented and locally verified (real-corpus rollout gates remain)
- **Evidence:** Code-confirmed. [apps/api/src/services/search.ts:177](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:177), [apps/api/src/services/search-query-analysis.ts:2091](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-query-analysis.ts:2091), [apps/api/src/services/search.ts:1943](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:1943), [apps/api/src/services/search.ts:2018](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:2018).
- **Problem and cause:** Each page recomputes recall, reranking, diversification, and deduplication using a `pageWindow` derived from offset and limit. Later pages can therefore rank a different candidate universe before slicing it. `total` and `hasMore` describe the current bounded presentation set, not all matching corpus documents. Deep paging repeats expensive work and can miss/repeat results; presentation deduplication by citation plus only the first 120 normalized snippet characters can also merge distinct decisions/passages.
- **Best solution:** Use a query-session/cursor bound to query, filters, permissions, corpus revision, and ranking version. Page a stable ordered result set or stable candidate snapshot. Expose exact/lower-bound/estimated count semantics and candidate exhaustion. Deduplicate by explicit document/source identity, preserving related/remand records.
- **Verification:** Pages have no overlap or missing entries relative to a fixed session; changing page size does not reorder prior results; corpus changes invalidate or version the cursor; related decisions sharing prefixes remain accessible.
- **Difficulty / change risk:** **Large / High.** Changes response semantics and frontend pagination. Do not add a shared cache before including authorization and corpus revision in its key.

**Repair record (M08):**

- Fixed candidate generation independently of requested offset/page size, then paged one immutable D1 snapshot using authenticated cursors bound to query/filters, format, existing public-corpus policy, corpus epoch and ranking version. Pages reuse the slate; corpus changes/expiry reject it. Offset consumers now receive explicit migration guidance.
- Replaced snippet/citation deduplication with document identity and scoped diversification anchors by document. An 18-related-decision fixture exposed and now prevents global-anchor collapse. Counts explicitly describe bounded candidates, with snapshot exhaustion separate from corpus exhaustiveness.
- Added atomic 256-session/16 MiB capacity, 512 KiB payload cap, ten-minute access lifetime, bounded expiry cleanup, keyed request identity and private/no-store responses. The UI follows cursors, checks version/position before append and preserves submitted context through unsent edits; invalidated sessions ask for a fresh search.
- Verified variable-size traversal, no overlap/omissions, no repeated candidate queries, query/format/filter/position rejection, stale epochs, capacity races and TTL cleanup. Actual workerd/D1/R2 resumed across Worker restart and rejected a cursor after a synthetic edit; an isolated real Next browser verified paging, submitted-context links/highlighting and invalidation handling. Full local behavioral/source/web checks, workspace typechecks and Worker dry run pass.
- **Rollout/risk:** [API migration, capacity/privacy, consistency fences and rollback](search-pagination-sessions.md). Any corpus mutation conservatively invalidates older sessions; fixed candidate policy needs real-corpus relevance/latency validation. Account/private authorization binding remains a prerequisite of H01, which is explicitly out of scope; no shared cross-user cache was added.

### M09 — Remove schema creation and sticky failure flags from request handling

- [x] Implemented and locally verified (populated release gates remain)
- **Evidence:** Code-confirmed. [apps/api/src/services/search-fts.ts:44](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-fts.ts:44), [apps/api/src/services/search-fts.ts:64](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-fts.ts:64), [apps/api/src/services/search-fts.ts:352](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-fts.ts:352), [apps/api/src/services/search-fts.ts:391](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-fts.ts:391).
- **Problem and cause:** A cold search can run index/table/trigger creation, facet backfills, and an FTS probe. Retryable errors are swallowed before setting “ensured” flags. A transient bootstrap failure can disable FTS or vocabulary for an isolate's lifetime. The facet population check uses OR across three tables; one populated table can suppress repair of another empty one. Cold isolates independently repeat this work, and module flags are not a durable migration/readiness record.
- **Best solution:** Put all schema/index creation in versioned migrations and all backfills in resumable jobs. Verify schema/backfill readiness explicitly at deployment/startup. Distinguish a missing migration from a transient query error; use bounded retry/expiry for capability probes rather than permanently disabling an accelerator after one failure. Avoid sharing request-owned I/O promises as a bootstrap mechanism.
- **Verification:** Concurrent cold starts, transient D1 errors, one missing facet table/backfill, and rolling deploys. A normal search should perform no DDL or corpus-wide bootstrap writes.
- **Difficulty / change risk:** **Medium–Large / High.** Runtime fallback currently masks incomplete deployments. Roll out readiness checks and complete migrations before removing it.

**Repair record (M09):**

- Removed runtime schema/vocabulary creation, facet bootstrap scans, shared I/O promises and isolate-wide readiness/failure flags. Each search uses readonly object/type and durable component-state checks; missing schema or incomplete builds return 503, while transient failures cannot poison later requests.
- Migration 0027 owns schema declarations and a versioned readiness marker. Added a leased 100-document facet reconciler that repairs all three facets independently, reads current values in one atomic batch, and checkpoints only committed work. The admin index workflow now completes substring, phrase and facet states before overall readiness.
- Verified cold/concurrent requests issue no bootstrap writes, missing schema is not silently provisioned, optional vocabulary/transient readiness recover, and a partially populated facet set is repaired with replay/live-write/lease/failure coverage. Actual workerd/D1/R2 rejected schema drift and pending work, then served cold/warm searches with zero DDL/bootstrap writes; one injected concurrent failure left the other and later requests healthy. Full behavioral/source checks, workspace typechecks and Worker dry run pass.
- **Rollout/risk:** [Readiness manifest, bounded facet build and migration/rollback procedure](search-readiness.md). Apply migrations and finish all build states before routing searches to the new reader; pending populated databases intentionally return 503. Direct imports/restores must invalidate/reconcile data readiness; M10/M26 address broader repair/recovery, and H01 remains out of scope.

### M10 — Backfill FTS when upgrading a database that already contains documents

- [x] Implemented and locally verified (restore/scale release gates remain)
- **Evidence:** **Reproduced.** [apps/api/migrations/0008_search_fts.sql:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0008_search_fts.sql:1), [apps/api/migrations/0010_search_fts_backfill.sql:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0010_search_fts_backfill.sql:1). Applying migrations 1–7, inserting a valid document/section/paragraph/chunk, then applying 8–12 left **one source chunk and zero FTS rows**.
- **Problem and cause:** Migration 8 creates triggers that maintain future writes, but does not index existing rows. Migration 10 is `SELECT 1` and assumes the deployed corpus was populated by an earlier runtime bootstrap. That assumption is not encoded or verified. A fresh-empty database works; an upgrade/import/restore with pre-existing data can silently miss search results.
- **Best solution:** Add a new migration/job rather than rewriting applied migration history. Backfill missing rows idempotently in bounded batches, track completion by source revision, and compare source/index coverage before marking the index ready. Do not blindly reinsert the entire corpus into a table without a uniqueness strategy.
- **Verification:** Test empty install, populated upgrade, partially backfilled database, replay, and restored snapshot. FTS coverage must reconcile with both source chunk tables and contain no duplicate identities.
- **Difficulty / change risk:** **Medium–Large / High.** An unbounded backfill can exceed D1 limits or duplicate data. Coordinate with M07 and retain rollback/readiness controls.

**Repair record (M10):**

- Preserved historical migrations and reproduced the populated 0007→0010 FTS hole. Added bounded source/mapping/physical-entry coverage that does not mistake external-content view counts for token postings, with explicit consistency/revision/deep-integrity limits.
- Added explicit fresh-generation repair: guarded atomic canonical-index replacement, finite keyset high-water marks, 100-identity checkpoints, transactional generation/revision stamps, live source maintenance, orphan pruning and bounded verification before readiness. Unknown state is rebuilt without blindly reinserting over old postings; approval/publication/source content stay unchanged.
- Added lease/generation fences, generation ID history with reuse protection, ambiguous-ack reconciliation, explicit restart and archive-rotation controls, and fail-closed restoration of incomplete control state. Dry-run and pure index endpoints skip unrelated provider maintenance.
- Verified populated upgrades, replay/restore holes, ghost/missing entries, missing indexes, concurrent/live writes, integer rowids, start/checkpoint failures, lost acknowledgments and restart/fencing. Actual workerd/D1/R2 detected masked posting loss, held search, rebuilt and passed all three integrity checks without changing business-document metadata; new reviewed writes and mid-job Worker restart passed. An injected post-DDL failure rolled back indexes, archives and state. Full behavioral/source checks, workspace typechecks and Worker dry run pass.
- **Rollout/risk:** [Coverage API, resumable repair, maintenance/storage limits and rollback](search-index-recovery.md). Full uncertain-index recovery intentionally gates search through rebuild plus facets. Large D1 index resets need preview capacity/timing checks; frozen external-content archives are not source backups. Physical database corruption requires consistent backup restore. M26 supplies the broader target-aware recovery workflow; H01 remains out of scope.

### M11 — Parenthesize the low-confidence reviewer filter

- [x] Implemented and locally verified
- **Evidence:** **Reproduced.** [apps/api/src/services/admin-ingestion.ts:1130](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:1130), [apps/api/src/services/admin-ingestion.ts:1190](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:1190).
- **Problem and cause:** The low-confidence condition is `fallback = 1 OR confidence < 0.45`, appended to other predicates with AND without parentheses. SQL precedence lets the second branch bypass earlier filters. An in-memory reproduction of decision-only plus low-confidence returned both a decision and a law document. Other status/scope constraints can similarly be bypassed.
- **Best solution:** Wrap compound conditions when composing them, or use a typed predicate builder that preserves grouping. Validate all list filter combinations rather than relying on source-pattern assertions.
- **Verification:** A truth-table fixture covering file type, status, real-only, taxonomy fallback, missing confidence, and high/low confidence. Both OR branches must obey all surrounding filters.
- **Difficulty / change risk:** **Small / Low.** Result counts will change because incorrectly included rows disappear.

**Repair record (M11):**

- The reviewer list now parenthesizes each predicate before joining filters with AND, so both taxonomy OR branches obey file type, status, real-only, query and other surrounding constraints. The same composed predicate serves candidate counts and row selection.
- Removed absent-score coercion to zero. Only numeric confidence below 0.45 or an explicit fallback matches; genuine zero stays low, while missing/null/non-numeric confidence agrees with the reviewer display's unknown state. The score predicate safely handles malformed metadata JSON without modifying stored metadata.
- Executed the actual listing service against a 240-row truth-table fixture across 38 scope/query/sort combinations, including fallback-high, low/zero/boundary/high scores, missing/null and non-numeric scores. Both result identities and derived-prefilter counts match independent expected sets. All 170 API behavioral tests, 84 source checks and API typecheck pass.
- **Rollout/rollback:** no migration or data rewrite. Previously leaked rows and unknown-score rows can disappear from the low-confidence queue; that count change is intentional. Preserve grouped conjunctions in a forward fix rather than restoring the precedence bug. Queue pagination/count scaling remains M13; classifier confidence semantics remain M23, and authentication remains H01.

### M12 — Fix metadata PATCH semantics and protect concurrent reviewer edits

- [x] Implemented and locally verified (authenticated actor attribution remains H01)
- **Evidence:** Code-confirmed and SQL reproduction. [packages/shared/src/index.ts:832](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:832), [apps/api/src/services/admin-ingestion.ts:2030](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:2030), [apps/api/src/services/admin-ingestion.ts:2060](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:2060).
- **Problem and cause:** Nullable metadata fields are written with COALESCE, so an explicit null cannot clear a wrong author/date/case number. `confirm_required_metadata: false` maps to undefined and cannot revoke confirmation. Confirmation with omitted arrays considers their lengths zero instead of using persisted values. Multiple reviewers overwrite each other without a version check, and metadata updates/reference-link replacements can span independently committed batches.
- **Best solution:** Distinguish omitted, null, and explicit values; compute confirmation against the resulting full record. Add an expected revision/ETag and reject stale edits with a conflict response. Update metadata and its validation generation coherently; invalidate approval when required. Record who changed what.
- **Verification:** Clear each nullable field, unconfirm, confirm without resending unchanged arrays, edit one field, and race two reviewers. Invalid reference changes must not retain valid approval.
- **Difficulty / change risk:** **Medium / Medium–High.** Existing scripts may depend on null meaning “unchanged”; version the request contract or migrate them together.

**Repair record (M12):**

- Required the client's reviewed revision and a mutation UUID for metadata saves and rejections. A transaction guard rejects stale forms and races; the UI and maintained scripts/fixtures use the new contract. Offline corrections carry their captured snapshot revision and refuse older unversioned reports.
- Distinguished omission from explicit null/false/empty arrays, preserved reviewer clears across reprocessing, and computed confirmation against the entire resulting record. Required-group confirmation fails before writes when incomplete; legal-reference validity still gates approval. Metadata, reference validation/catalog fences, confirmation and change history commit together.
- Added atomic retry/change receipts with before/after metadata, timestamps and applied revisions. Duplicate/lost-response delivery converges, UUID reuse with a different request conflicts, and replay cannot overwrite later edits. Rejection now follows the same review fence and clears confirmation/approval/searchability. Actor is explicitly unknown pending H01.
- Passed 177 API behavioral tests, 84 source checks, 33 web tests and workspace typechecks. Actual local workerd/D1/R2 verified null/false, retained-array confirmation, duplicate/stale requests, concurrent 200/409, rejection and rollback after an injected batch failure. Worker production dry-run bundle passed; no deployment or paid provider calls. The utility suite's sole failure is the independently reproduced pre-existing baseline inventory classification for `vector-generation-shadow.mjs` (37 other checks pass).
- **Rollout/risk:** [Request/retry semantics, migration 0029, coordinated clients, audit identity limits and rollback](reviewer-metadata-edits.md). This deliberately breaks unversioned metadata/rejection callers; release clients/API together and regenerate old offline candidate reports. Preserve receipts with their matching database snapshot; do not roll back to an unguarded writer. H01 actor attribution, M13 queue scaling, M25 general mutation bounds and M26 recovery remain separate findings.

### M13 — Paginate reviewer work and remove quadratic peer lists

- [ ] Open
- **Evidence:** Code-confirmed. [apps/api/src/services/admin-ingestion.ts:1191](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:1191), [apps/api/src/services/admin-ingestion.ts:1500](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:1500), [apps/api/src/services/admin-ingestion.ts:1624](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:1624), [apps/web/src/app/admin/ingestion/page.tsx:317](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/admin/ingestion/page.tsx:317).
- **Problem and cause:** The UI asks for 600 rows; the API can load up to 4,000 candidates for derived filtering and performs many correlated counts. Every item embeds all other IDs in its triage group as `canBatchReviewWith`, creating O(n²) allocation/serialization for large groups. Large derived sets are capped before final ranking/filtering, so some matches are unreachable. Summary totals are counts of returned rows, not the complete queue, and there is no cursor for traversing the full list.
- **Best solution:** Materialize/index reviewer state or aggregate it in SQL with well-defined revision semantics. Return a group key and peer count, load peer IDs on demand, and add keyset pagination. Separate global queue totals from page totals and expose incomplete/estimated results clearly. Virtualize long UI lists only after reducing the payload/work.
- **Verification:** Large same-group fixtures, more than 4,000 candidates, derived sorts, and complete queue traversal. Measure response size, heap, rows read, and queue-count accuracy.
- **Difficulty / change risk:** **Large / High.** Derived reviewer policy is duplicated in SQL and JavaScript; establish parity fixtures before changing execution.

### M14 — Validate assistant citations and keep retrieved content out of privileged prompt roles

- [ ] Open
- **Evidence:** Code-confirmed. [apps/api/src/services/assistant-chat.ts:193](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:193), [apps/api/src/services/assistant-chat.ts:256](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:256), [apps/api/src/services/assistant-chat.ts:326](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:326), [apps/api/src/services/draft-conclusions.ts:299](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/draft-conclusions.ts:299).
- **Problem and cause:** The assistant accepts any nonempty provider answer and attaches every retrieved decision as a citation, regardless of whether the answer used or correctly represented it. A citations panel therefore establishes retrieval, not claim support. Prompt instructions to stay grounded are useful but are not verified. Raw user/source content is interpolated into delimiter strings and then sent in a system-role message; delimiter text in a source can blur boundaries. This is an answer-integrity exposure, not evidence of model tool execution or credential theft.
- **Best solution:** Keep fixed behavioral instructions separate from user and retrieved data using appropriate message roles and structured serialization. Require structured claim-to-source IDs, validate those IDs against the authorized evidence set, and distinguish “retrieved sources” from “sources supporting this answer.” Reject or visibly qualify unsupported claims and preserve an extractive fallback. Test adversarial source text; do not promise that escaping alone eliminates prompt injection. Coordinate paragraph-support semantics with H07–H09.
- **Verification:** Fabricated citation IDs, irrelevant attached sources, no-source answers, contradictory passages, and source text containing fake role/delimiter instructions. The UI must not imply that a claim is supported merely because retrieval returned documents.
- **Difficulty / change risk:** **Large / High.** Changes output contracts and may increase abstention. Evaluate factual support with human-reviewed examples before adjusting thresholds.

### M15 — Align chat history and message-size contracts

- [ ] Open
- **Evidence:** **Reproduced** schema failure; code-confirmed client mismatch. [packages/shared/src/index.ts:434](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:434), [packages/shared/src/index.ts:438](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:438), [packages/shared/src/index.ts:76](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:76), [apps/web/src/app/case-assistant/page.tsx:36](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/case-assistant/page.tsx:36), [apps/api/src/services/assistant-chat.ts:80](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:80).
- **Problem and cause:** The browser sends the complete conversation, but the API permits only 20 messages. After ten normal user/assistant exchanges, the eleventh user message sends 21 entries and fails validation. A chat message may contain 12,000 characters, while its derived search query is limited to 2,000; long accepted chat input can fail inside retrieval. The model already uses only the last eight messages, so rejecting the whole stored conversation provides no useful protection.
- **Best solution:** Separate persisted history from a bounded model/retrieval context. Use a shared history/token budget and deliberate summarization or a validated recent-message window. Derive a bounded search query with explicit behavior for long/very short inputs; show any user-actionable limit before submission. Do not fix this by removing limits entirely.
- **Verification:** More than 30 exchanges, long user questions, long pasted facts, empty/whitespace messages, and context-window boundaries. Existing history should remain visible and usable after the request window is trimmed.
- **Difficulty / change risk:** **Small–Medium / Medium.** Naive truncation can remove facts needed for a follow-up; coordinate with M16.

### M16 — Retrieve for the conversation's actual question and honor every accepted filter

- [ ] Open
- **Evidence:** Code-confirmed. [apps/api/src/services/assistant-chat.ts:36](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:36), [apps/api/src/services/assistant-chat.ts:74](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:74), [apps/api/src/services/assistant-chat.ts:311](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:311), [packages/shared/src/index.ts:437](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:437).
- **Problem and cause:** Retrieval sees only the latest user message. The recent conversation is sent to the model after retrieval, so “What about the second case?” searches those words without the antecedent issue or case identity. The shared request accepts `judgeNames`, but `runAssistantChat` never passes them to search. Users/API clients can believe their scope is applied when it is not. Only one top result per document is retained, which can also omit the passage needed to resolve a follow-up.
- **Best solution:** Resolve follow-up references into a bounded standalone retrieval request using explicit conversation/case state. Carry selected document IDs and all supported filters through a single validated scope object. Surface the effective scope and reject unsupported filters instead of silently ignoring them. Retrieve complementary passages only within that authorized scope.
- **Verification:** Pronoun/ordinal follow-ups, changing issue mid-chat, selected-case questions, judge filters, and conflicting scope changes. Evaluate retrieval separately from answer generation so a fluent answer cannot hide wrong evidence.
- **Difficulty / change risk:** **Medium–Large / Medium–High.** Query rewriting can change intent; retain the original question and make the effective scope inspectable.

### M17 — Separate live chat state from persistence and make storage failures recoverable

- [ ] Open
- **Evidence:** Code-confirmed. [apps/web/src/components/assistant-thread-store.ts:61](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/components/assistant-thread-store.ts:61), [apps/web/src/components/assistant-thread-store.ts:105](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/components/assistant-thread-store.ts:105), [apps/web/src/app/case-assistant/page.tsx:124](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/case-assistant/page.tsx:124).
- **Problem and cause:** Saving normalizes away pending messages and streaming flags, dispatches a same-window update event, and the active page reloads that normalized copy. This can erase its own live pending/typing state despite the comment saying live state is separate. Every change serializes the complete thread collection; quota/private-storage failures are uncaught. Cross-tab writers replace the entire collection and can lose each other's changes. Stored message shapes are only partially validated. A single origin-wide key has no account boundary or sign-out retention policy.
- **Best solution:** Use one explicit live-state store, with persistence as an output adapter. Ignore self-originated sync notifications or merge durable records without overwriting in-flight state. Validate a versioned storage schema, handle unavailable/full storage visibly, and cap/debounce writes or use IndexedDB for larger histories. Merge per-thread revisions across tabs. When identity is introduced, namespace and clear/retain data according to an explicit account policy.
- **Verification:** Pending response plus same-window save, streaming completion, quota errors, malformed stored data, two-tab concurrent edits, reload mid-request, and account switching. A storage failure must not lose the current answer or crash the interaction.
- **Difficulty / change risk:** **Medium / Medium.** Migrate existing saved chats without silently discarding them. Browser integration tests are needed; this review established the event path from code, not a rendered-browser reproduction.

### M18 — Version governing references by jurisdiction and effective date

- [ ] Open
- **Evidence:** Code-confirmed schema limitation. [apps/api/migrations/0004_reference_normalization.sql:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0004_reference_normalization.sql:1), [apps/api/migrations/0004_reference_normalization.sql:62](/Users/cliftonnoble/Code/beedle-ai/apps/api/migrations/0004_reference_normalization.sql:62), [apps/api/src/services/legal-references.ts:752](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/legal-references.ts:752).
- **Problem and cause:** Reference tables use globally unique citation identifiers plus an active flag. They have no jurisdiction/effective-date/version dimension; document-reference links store canonical strings without the catalog revision used to validate them. Replacing the catalog can change the meaning of a historical validation. A historical decision's citation being present in today's catalog does not establish that the same text governed the decision. This matters even in one jurisdiction when rules change; multi-jurisdiction intake makes collisions worse.
- **Best solution:** Introduce immutable reference editions with jurisdiction, effective interval, source hash/provenance, and explicit applicability policy. Bind validation and generated evidence to an edition. Where the applicable edition is unknown, label it unknown and require review rather than silently treating current text as historical authority. Retain old editions and links when publishing a new catalog under H16.
- **Verification:** Same citation with different text across two dates/jurisdictions, repealed provisions, unknown decision dates, and replay of an older draft. The exact referenced text and validation edition must remain recoverable.
- **Difficulty / change risk:** **Large / High.** Existing data lacks some of these facts. Backfill what is evidenced and preserve unknowns; domain review is needed to define applicability, not to invent missing dates.

### M19 — Add behavioral integration tests and an independently judged relevance holdout

- [ ] Open
- **Evidence:** [.github/workflows/ci.yml:37](/Users/cliftonnoble/Code/beedle-ai/.github/workflows/ci.yml:37), [apps/api/tests/search-golden-ranking.test.mjs:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/tests/search-golden-ranking.test.mjs:1), [apps/api/tests/live-test-helpers.mjs:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/tests/live-test-helpers.mjs:1), [apps/api/tests/fixtures/search-relevance-judgments.json:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/tests/fixtures/search-relevance-judgments.json:1), [apps/web/tests/assistant-thread-persistence-source.test.mjs:1](/Users/cliftonnoble/Code/beedle-ai/apps/web/tests/assistant-thread-persistence-source.test.mjs:1).
- **Problem and cause:** CI has useful deterministic utility tests, but many guards inspect source strings rather than execute behavior. They can pass with the approval, timing, and persistence defects above. Live golden tests skip without a server; that happened in this audit. The 27-query golden fixture is pinned expected behavior, while three of the 19 relevance queries borrow golden-derived labels and one is measure-only. These are useful regression fixtures but cannot independently establish quality on unfamiliar queries. Current CI does not exercise actual Worker/D1/Queue failure boundaries or browser interactions.
- **Best solution:** Add a small hermetic Worker/D1 integration suite with real migrations, fake bounded providers, and deterministic queue/failure injection. Add browser tests for search, reviewer identity races, and chat persistence. Keep source guards where they enforce a real invariant, but require behavioral tests for repaired bugs. Maintain a separate human-judged, unseen relevance/grounding set covering positive, negative, filtered, historical, and adversarial cases; report recall, ranking, support, and abstention separately.
- **Verification:** Intentionally reintroducing H11, H18, M10, M11, or M17 should fail a behavioral test. Required integration jobs must fail on missing setup rather than silently report success through skips. Keep production corpus/provider tests a separate, explicit stage.
- **Difficulty / change risk:** **Large / Medium.** Test infrastructure is additive; avoid making every PR depend on paid inference or mutable production data.

### M20 — Gate deployments on schema readiness and the actual hosting adapter

- [ ] Open
- **Evidence:** [.github/workflows/ci.yml:37](/Users/cliftonnoble/Code/beedle-ai/.github/workflows/ci.yml:37), [.github/workflows/deploy-api.yml:56](/Users/cliftonnoble/Code/beedle-ai/.github/workflows/deploy-api.yml:56), [.github/workflows/apply-d1-migrations.yml:37](/Users/cliftonnoble/Code/beedle-ai/.github/workflows/apply-d1-migrations.yml:37), [apps/web/package.json:8](/Users/cliftonnoble/Code/beedle-ai/apps/web/package.json:8), [apps/web/package.json:24](/Users/cliftonnoble/Code/beedle-ai/apps/web/package.json:24), [apps/api/src/index.ts:64](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:64).
- **Problem and cause:** The API deploy and manual D1 migration workflows are independent; deployment does not verify the required schema/backfill version. The smoke check confirms HTTP success, while `/health` checks binding presence rather than query/index readiness and the search response is discarded. CI does not run the web build or Pages adapter build. The project still pins `next-on-pages`; a successful `next build` cannot establish that this older adapter supports the chosen patched framework in production.
- **Best solution:** Define compatible schema ranges and backfill readiness for each release. Rehearse additive migrations against populated fixtures, verify deployment prerequisites, and use a canary with semantic smoke assertions and rollback criteria. Build/test the actual web deployment output in CI. Make a separate architecture decision for the maintained Cloudflare path: the current official [Next.js Pages guide](https://developers.cloudflare.com/pages/framework-guides/nextjs/) recommends vinext on Workers for full-stack apps and static export for static Pages sites. Evaluate compatibility and migration cost rather than mechanically replacing the framework during H02.
- **Verification:** Missing/outdated schema, partial backfill, wrong binding, empty/broken search, adapter build failure, and rollback after an additive migration. Verify a real deployed preview using isolated resources from H17.
- **Difficulty / change risk:** **Large / High.** Hosting migration can alter routing, edge behavior, source URLs, secrets, and deploy ownership. Keep the immediate security update and a larger platform migration independently reviewable.

### M21 — Measure end-to-end latency, fallback rates, and durable-job health

- [ ] Open
- **Evidence:** Code-confirmed gaps. [apps/api/src/index.ts:148](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/index.ts:148), [apps/api/src/services/search.ts:124](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:124), [apps/api/src/services/assistant-chat.ts:275](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:275), [apps/api/wrangler.toml:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/wrangler.toml:1).
- **Problem and cause:** Search has useful opt-in stage diagnostics, but its timer starts after runtime index setup, hiding cold-start bootstrap work. There is no consistent request ID/span across HTTP, D1, AI, R2, and queue processing; the checked-in Worker config has no explicit observability policy. Many degradations are warnings or silently reduced capability. The assistant's fallback text always blames a rejected key, including unconfigured providers, rate limits, malformed responses, and network failures. There are no checked-in alert definitions for stuck jobs, indexing lag, empty-result regressions, or repeated fallbacks. Dashboard settings outside the repo were not inspected.
- **Best solution:** Add privacy-aware structured telemetry with an ingress timer and correlation/job IDs, provider result categories, query count/rows read, embedding work, and publication/index lag. Define initial SLOs from measured baseline data and actionable alerts. Report fallback status accurately without exposing provider payloads/secrets. Sample/redact sensitive query/document text; aggregate performance by query class and corpus revision.
- **Verification:** Trace a slow cold search and a queue retry end to end; detect stalled processing, missing vectors, and provider fallback. Client-visible total time should align with server stages plus transport. Confirm logs do not expose uploaded case facts by default.
- **Difficulty / change risk:** **Medium / Low–Medium.** Control telemetry volume and cardinality; do not add full-document logging to diagnose correctness.

### M22 — Make ingestion idempotent and distinguish duplicate files from new revisions

- [ ] Open
- **Evidence:** Code-confirmed. [apps/api/src/services/ingest.ts:50](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:50), [apps/api/src/services/ingest.ts:297](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:297), [apps/api/src/services/ingest.ts:397](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/ingest.ts:397).
- **Problem and cause:** Each attempt generates new document/source identities without a stable request-id/content-hash uniqueness contract. If the server commits but the client loses the response, retrying creates another document, object, and vector job. Duplicate corpus entries inflate counts/cost and can distort retrieval or outcome summaries. Downstream title/citation/prefix deduplication cannot reliably distinguish an identical upload from an amended decision or separate related case.
- **Best solution:** Accept an idempotency key scoped to caller and operation, persist the request fingerprint/result, and reject conflicting key reuse. Store source hashes and an explicit document/revision identity. Detect identical bytes while allowing different legal records to reference the same source and allowing intentional new versions. Return a stable status that can be polled after a timeout.
- **Verification:** Retry before/after every persistence boundary, simultaneous identical requests, same filename with different bytes, and amended decisions with the same citation. Replays must not duplicate embeddings or publish two logical copies.
- **Difficulty / change risk:** **Medium–Large / High.** Existing duplicates need a separate, reviewed reconciliation process; do not automatically delete documents solely because title or citation matches.

### M23 — Use a truthful fallback when taxonomy inference has no signal

- [ ] Open
- **Evidence:** **Reproduced.** [apps/api/src/services/taxonomy-inference.ts:43](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/taxonomy-inference.ts:43), [apps/api/src/services/taxonomy-inference.ts:88](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/taxonomy-inference.ts:88). With unrelated text and empty metadata, the current configuration returns `zoning_variance`, confidence `0.2`, and `fallback: false` while the signal says `weak_signal_fallback`.
- **Problem and cause:** The winner starts as the first taxonomy item with score −1. All-zero scores therefore select that item. The fallback flag is true only if the winner happens to be the configured default. This arbitrarily assigns a substantive case type and underreports fallback use. The configuration's case types also need to be reconciled with the actual rent-board corpus before suggestions can be trusted.
- **Best solution:** Require evidence above a calibrated threshold before selecting a substantive type. Use the configured general/unknown fallback for zero or ambiguous signals and make the confidence/fallback/reason internally consistent. Have domain reviewers validate the taxonomy and a labeled corpus; do not infer relevance from array ordering.
- **Verification:** Zero-signal input, ties, negated terms, conflicting codes, and positive cases for each supported type. Reordering the configuration must not change a zero-signal result.
- **Difficulty / change risk:** **Small–Medium / Medium.** The code fix is small; taxonomy changes can alter review queues and template selection, so retain prior suggestions for comparison.

### M24 — Neutralize spreadsheet formulas in reviewer CSV exports

- [ ] Open
- **Evidence:** Code-confirmed. [apps/api/src/services/reviewer-export.ts:97](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/reviewer-export.ts:97).
- **Problem and cause:** CSV escaping handles delimiters, quotes, and newlines but does not distinguish untrusted text from spreadsheet formulas. A title/citation/reviewer field beginning with a formula marker can be interpreted as a formula when a reviewer opens the export. CSV quoting alone does not force a cell to be text. Actual execution behavior depends on the spreadsheet application; no external payload was opened in this review.
- **Best solution:** Define a spreadsheet-safe text serialization policy, including leading whitespace/control characters and formula prefixes, or produce a typed spreadsheet with all untrusted fields stored as text. Keep raw machine-readable JSON separately for lossless round trips. Apply the rule consistently to queue exports and adjudication templates.
- **Verification:** Benign cells beginning with `=`, `+`, `-`, `@`, tabs, quotes, commas, and newlines stay literal in supported spreadsheet clients and re-import correctly. Include negative numbers as a separate typed case where appropriate.
- **Difficulty / change risk:** **Small / Low–Medium.** Prefixing text can affect downstream importers; document and test the round-trip contract.

### M25 — Reject malformed mutations and bound validation work

- [ ] Open
- **Evidence:** Code-confirmed; empty JSON error classification reproduced. [apps/api/src/routes/admin-references.ts:24](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/routes/admin-references.ts:24), [packages/shared/src/index.ts:832](/Users/cliftonnoble/Code/beedle-ai/packages/shared/src/index.ts:832), [apps/api/src/services/legal-references.ts:752](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/legal-references.ts:752), [apps/api/src/lib/http.ts:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/lib/http.ts:1).
- **Problem and cause:** Reference backfill converts any `readJson` failure into `{}` and then executes a default 500-row mutation. Invalid JSON, a rejected body, or a wrong content type can therefore trigger work instead of a 4xx response. Admin metadata reference arrays/strings lack the bounds used by other request schemas. The older normalized-reference validator issues one serial query per value, and some callers then load catalogs again for link building. Empty JSON can escape the reader's syntax-error translation and become a generic 500. Several service validation errors similarly lack typed client-error status.
- **Best solution:** Use bounded Zod schemas at every route, preserve body-reader errors, and distinguish an intentionally absent optional body from malformed input. Set explicit array/string/row limits and valid date semantics. Validate reference sets with bounded batched lookups or one revision-scoped catalog load reused through the operation. Adopt typed 400/404/409/413/415 errors without leaking internal messages.
- **Verification:** Empty/malformed/wrong-type/oversized JSON performs zero writes; extreme arrays have bounded query counts; nonexistent records and stale versions return the intended status; impossible calendar dates are rejected.
- **Difficulty / change risk:** **Medium / Medium.** Existing scripts sending permissive payloads may start failing; supply actionable validation details and update them together.

### M26 — Make migration and recovery scripts target-aware and verifiable

- [ ] Open
- **Evidence:** Code-confirmed. [apps/api/scripts/d1-remote-data-migrate.mjs:8](/Users/cliftonnoble/Code/beedle-ai/apps/api/scripts/d1-remote-data-migrate.mjs:8), [apps/api/scripts/d1-remote-data-migrate.mjs:74](/Users/cliftonnoble/Code/beedle-ai/apps/api/scripts/d1-remote-data-migrate.mjs:74), [apps/api/scripts/d1-remote-data-migrate.mjs:92](/Users/cliftonnoble/Code/beedle-ai/apps/api/scripts/d1-remote-data-migrate.mjs:92), [apps/api/scripts/d1-remote-data-migrate.mjs:130](/Users/cliftonnoble/Code/beedle-ai/apps/api/scripts/d1-remote-data-migrate.mjs:130), [apps/api/scripts/d1-remote-data-migrate.mjs:204](/Users/cliftonnoble/Code/beedle-ai/apps/api/scripts/d1-remote-data-migrate.mjs:204).
- **Problem and cause:** The remote data migration defaults to a machine-specific local SQLite path and the production database name, uses one fixed checkpoint file, and does not bind checkpoints to a source snapshot, schema, or target identity. Switching source/target can reuse stale “complete” flags and skip data. It reads a potentially live Wrangler SQLite file with `immutable=1`, which is unsafe as a snapshot assumption when WAL writes are active. `INSERT OR IGNORE` plus attempted-row counters does not prove target content equals the source. D1 transfer alone does not restore the matching R2 objects and vector generation.
- **Best solution:** Require an explicit target and a consistent SQLite backup/export snapshot. Bind the plan/checkpoint to source checksum, schema version, account/database identity, and run ID. Use a lock and atomic checkpoint writes; preflight constraints and report actual inserted/conflicting rows. Produce a manifest linking D1, R2 source objects, reference editions, and rebuildable vectors. Rehearse restore into isolated resources with reconciliation before claiming recovery readiness.
- **Verification:** Resume after interruption; wrong source/target must refuse the checkpoint; concurrent runs cannot share state; conflicting IDs with different content are reported; restored search/source/approval behavior matches the manifest.
- **Difficulty / change risk:** **Medium–Large / High.** These tools can write production data. Test with small disposable fixtures and reviewed plans before a real migration; do not run a repair script solely because its name suggests safety.

## Low severity

### L01 — Split policy, retrieval stages, and persistence behind tested contracts

- [ ] Open
- **Evidence:** [apps/api/src/services/search-scoring.ts:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-scoring.ts:1) (4,329 lines), [apps/api/src/services/search-query-analysis.ts:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search-query-analysis.ts:1) (2,401), [apps/api/src/services/search.ts:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/search.ts:1) (2,126), [apps/api/src/services/admin-ingestion.ts:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/admin-ingestion.ts:1) (2,549).
- **Problem and cause:** Large modules combine domain heuristics, candidate selection, ranking, fallback orchestration, presentation, and persistence. Many corpus-specific exceptions and repeated reviewer/normalization policies make interactions hard to reason about. A local patch can contradict a distant fallback, and source-based tests encourage preserving implementation shape rather than behavior. Module length itself is not a runtime bottleneck; the cost is repair risk and slow development.
- **Best solution:** First capture behavior and the corrected invariants from this audit. Then extract pure domain policies, query planning, bounded candidate retrieval, ranking, and presentation with typed inputs/outputs. Centralize eligibility and citation normalization rather than creating more helper copies. Keep end-to-end parity tests and change one boundary at a time.
- **Verification:** A module extraction should preserve the established behavior suite and query budgets. New rules should be expressible/testable in one policy location without editing several SQL/string paths.
- **Difficulty / change risk:** **Large / High.** This is low urgency, not an easy rewrite. Avoid combining architecture cleanup with ranking changes or data migrations in one PR.

### L02 — Generate binding types and validate external payloads at runtime

- [ ] Open
- **Evidence:** [apps/api/src/lib/types.ts:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/lib/types.ts:1), [apps/api/wrangler.toml:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/wrangler.toml:1), [apps/api/src/services/assistant-chat.ts:166](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/assistant-chat.ts:166), [apps/api/src/services/retrieval-activation.ts:114](/Users/cliftonnoble/Code/beedle-ai/apps/api/src/services/retrieval-activation.ts:114), [tsconfig.base.json:1](/Users/cliftonnoble/Code/beedle-ai/tsconfig.base.json:1).
- **Problem and cause:** Strict TypeScript is enabled, but hand-maintained environment bindings and `any`/unchecked casts at provider and activation boundaries bypass its guarantees. There is no checked-in lint gate for floating promises or unsafe asynchronous patterns. Compilation cannot prove that a provider payload, activation bundle, or deployed binding matches an interface.
- **Best solution:** Generate environment types from Wrangler configuration and keep runtime validation for untrusted payloads. Add a small, focused lint baseline for promise/error handling and unsafe casts, concentrating on touched code. Share versioned API schemas without forcing unrelated frontend/server concerns into one giant schema file.
- **Verification:** Missing/misnamed bindings fail the appropriate build/config check; malformed provider/bundle payloads receive controlled errors; intentional background work is tied to queue or `waitUntil` semantics. Avoid a broad formatting rewrite.
- **Difficulty / change risk:** **Small–Medium / Low.** Runtime schema tightening can expose existing invalid payloads, so add fixtures before enforcing it.

### L03 — Remove the external CSS font dependency from the critical render path

- [ ] Open
- **Evidence:** [apps/web/src/app/globals.css:1](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/globals.css:1) imports Google Fonts with CSS `@import`.
- **Problem and cause:** The main stylesheet triggers another stylesheet request before the font can be discovered. This adds an external render-path dependency and makes typography depend on that provider/network. No measured LCP regression is claimed here; the dependency chain is visible in source. The existing `display=swap` helps text rendering but does not remove the extra discovery step.
- **Best solution:** Use appropriately licensed local/subset fonts through the chosen framework's supported asset/font pipeline, or a system-font stack. Ship only needed weights and set compatible fallback metrics. Verify this in the actual adapter build before changing the loading mechanism.
- **Verification:** Browser network trace should show no stylesheet `@import` waterfall for fonts. Compare cold-cache LCP, layout shift, visual appearance, and font transfer size on a throttled connection.
- **Difficulty / change risk:** **Small / Low.** Check visual differences and weight availability.

### L04 — Complete keyboard and focus behavior in dialogs and reviewer controls

- [ ] Open
- **Evidence:** [apps/web/src/components/app-shell.tsx:195](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/components/app-shell.tsx:195), [apps/web/src/app/admin/ingestion/page.tsx:776](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/admin/ingestion/page.tsx:776), [apps/web/src/app/admin/ingestion/page.tsx:850](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/admin/ingestion/page.tsx:850).
- **Problem and cause:** The custom delete dialog has dialog ARIA attributes but no implemented focus trap, initial-focus/restoration, or Escape handling. Reviewer rows use click handlers on table rows without an equivalent keyboard control, and several metadata inputs rely on placeholders without persistent programmatic labels. These can prevent keyboard/screen-reader users from completing review. Existing reduced-motion CSS is present; this is not a claim that all accessibility support is missing.
- **Best solution:** Use a tested accessible dialog primitive or correctly managed native dialog; implement focus restoration and dismiss behavior. Put a real button/link in each selectable row and give inputs associated labels and clear error/status announcements. Preserve visible focus states and usable zoom/overflow behavior.
- **Verification:** Complete select/edit/save/reject and chat-delete flows with keyboard only; verify focus after dialog dismissal, accessible names, and screen-reader status updates. Run automated accessibility checks plus manual interaction checks.
- **Difficulty / change risk:** **Small–Medium / Low.** Coordinate row behavior with H18 so keyboard and pointer paths share the same identity checks.

### L05 — Make the unfinished intake page unambiguously noninteractive

- [ ] Open
- **Evidence:** [apps/web/src/components/add-decision-placeholder.tsx:31](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/components/add-decision-placeholder.tsx:31).
- **Problem and cause:** The page correctly labels the feature planned, but also says “Click to select files or drop them here,” “Encrypted intake,” and “Metadata extraction ready,” alongside an active-looking scanning timeline. There is no file input or intake operation. The mixed states invite failed actions and imply readiness/security properties that this screen cannot demonstrate.
- **Best solution:** Until intake is implemented, show one clear planned state with disabled/noninteractive affordances and the supported current intake route, if one exists. When wiring it up, use real server job states and truthful format/size/error messages. Do not expose the upload control before H01 and H03–H05 are addressed.
- **Verification:** Users can distinguish planned functionality from working controls; no file selection/drop appears accepted when nothing is uploaded; progress corresponds to actual jobs once implemented.
- **Difficulty / change risk:** **Small / Low.** Mostly product copy/state clarification.

### L06 — Consolidate current operations guidance and remove machine-specific assumptions

- [ ] Open
- **Evidence:** [README.md:52](/Users/cliftonnoble/Code/beedle-ai/README.md:52), [docs/cloudflare-deployment-runbook.md:1](/Users/cliftonnoble/Code/beedle-ai/docs/cloudflare-deployment-runbook.md:1), [scripts/test-search-stack.sh:4](/Users/cliftonnoble/Code/beedle-ai/scripts/test-search-stack.sh:4), [apps/api/scripts/README.md:1](/Users/cliftonnoble/Code/beedle-ai/apps/api/scripts/README.md:1).
- **Problem and cause:** The long README mixes setup, historical phases, and operating instructions. Several scripts/docs assume one absolute checkout/data path. Historical comments describe fixes such as auth/session or persistence behavior more broadly than current code supports. A new operator can follow stale instructions or infer a guarantee that has not been verified.
- **Best solution:** Keep a short current setup guide and separate architecture, deployment/recovery, corpus operations, and archived history. Derive repository paths from script location and require explicit data paths. Document command side effects, environment, prerequisites, expected output, and rollback. Link each claimed security/data invariant to a behavioral check rather than a historical issue label.
- **Verification:** A second machine can follow setup with a different checkout path; documented checks run without production writes by default; every live runbook command names its target and expected postcondition.
- **Difficulty / change risk:** **Small–Medium / Low.** Preserve historical notes in an archive instead of deleting potentially useful recovery context.

### L07 — Load decision detail independently of search context

- [ ] Open
- **Evidence:** [apps/web/src/app/search/decision/[documentId]/decision-detail-client.tsx:131](/Users/cliftonnoble/Code/beedle-ai/apps/web/src/app/search/decision/[documentId]/decision-detail-client.tsx:131).
- **Problem and cause:** The detail loader returns immediately without a query string even when the document ID is valid. When a query exists, preview and a fresh search are coupled in `Promise.all`; slow/failed search delays or prevents the already-available document preview. Navigation cancels state updates but not the underlying requests. This makes direct bookmarks fragile and incurs retrieval work merely to read a document.
- **Best solution:** Fetch authorized document detail by ID independently and render it as soon as available. Treat search matches/highlights as optional context, with separate loading/error states and cancellable requests. Reuse a valid query-session result where available, while preserving server access checks.
- **Verification:** Open an ID-only bookmark, load with search unavailable, navigate rapidly between documents, and use a stale query link. The document remains readable and stale requests cannot replace current state.
- **Difficulty / change risk:** **Small–Medium / Low–Medium.** Preserve source anchors and matched-passage navigation while decoupling their loading states.

## Recommended repair sequence

Severity determines urgency; dependencies determine safe implementation order. Several issues share a cause and should share a design, while their fixes remain small enough to review.

| Stage | Work | Exit condition |
| --- | --- | --- |
| 1. Contain immediate exposure and misleading results | H01–H05, H07–H09, H17–H18; patch H02 promptly; cover omitted expensive routes under M01 | Verified identity/perimeter, safe original downloads, controlled extraction failure, no unsupported “strong” evidence claims, isolated nonproduction resources, and no cross-document reviewer writes |
| 2. Land focused correctness fixes | H06, M11–M12, M15, M17, M23–M25; add regression tests from M19 as each lands | Citation suffixes preserved, filter truth tables pass, explicit null/false work, long chats survive, live state remains live, malformed requests perform no writes |
| 3. Establish revision and job invariants | H10–H16, M03–M04, M18, M22, M26 | One publishable revision per document; lease-protected idempotent jobs; atomic activation/catalog switches; reproducible recovery manifest |
| 4. Rebuild search foundations safely | M05–M10, M13, M16, L07 | Indexed complete recall channels, reconciled FTS, stable pagination, applied filters, bounded query work, independently measured relevance |
| 5. Strengthen delivery and quality | M14, M19–M21, L01–L06 | Actual-runtime gates, observable SLOs, verified grounding, accessible flows, maintained dependencies/runbooks |

Stages are not an instruction to postpone testing or telemetry. Add the relevant test/measurement with every repair. A reasonable first group of independently reviewable PRs is H02, H03, H18, H06, M11, M15, and M25, while the identity and revision designs are worked through. High-effort structural fixes should not block these contained improvements.

### Shared state model to settle before structural repairs

Define and test these invariants before changing ingestion/activation/search independently:

1. **Source identity:** an immutable original object plus content hash; a logical document may have multiple explicitly related revisions.
2. **Derived revision:** parser/chunker/reference/embedding versions identify exactly which artifacts were produced from which source.
3. **Review state:** confirmation and approval apply to that exact revision and an identified reviewer; edits cannot inherit obsolete approval silently.
4. **Publication state:** one central policy determines which revision readers, assistants, source routes, and administrators may access. Rejection and rollback have defined effects across every retrieval channel.
5. **Background ownership:** durable jobs have a state, attempt count, lease/fencing token, checkpoint, retry time, and terminal reason. A stale worker cannot publish over a newer revision.
6. **Cross-store recovery:** D1, R2, and Vectorize cannot share one transaction. Stage durable work, validate it, switch a small publication pointer, and reconcile or clean up orphaned work. Compensation alone is not atomicity.

Prefer an additive rollout: introduce versions/state, backfill with explicit unknowns, shadow-read and reconcile, switch readers, retain the old revision for rollback, and only then garbage-collect. D1 currently limits bound parameters to 100 per query and string/BLOB/row size to 2 MB; do not replace multi-batch code with an enormous statement that exceeds platform limits. Its per-database execution is serial, so unbounded parallel requests will not make one database execute writes concurrently. See the current [D1 limits](https://developers.cloudflare.com/d1/platform/limits/) and [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/).

### Performance measurements to collect before promising speedups

No production latency percentile or throughput claim is made in this audit. Use an isolated representative corpus and a fixed test manifest to establish these baselines:

| Surface | Scenarios | Measurements and guardrails |
| --- | --- | --- |
| Search | Cold/warm; common/rare/no-hit; exact citation; quoted phrase; judge + index code + date; narrow semantic filters; deep pagination | End-to-end p50/p95/p99, error/fallback rate, SQL calls and rows read, AI calls, response bytes, recall/ranking on holdout queries, stable pagination |
| Ingestion | Small/large DOCX, compressed/scanned PDF, malformed/archive-boundary inputs, replay, concurrent edits | Peak memory/CPU, extraction fidelity, time to reviewed publication, duplicate count, orphaned objects/artifacts, retry cost |
| Queue/indexing | Duplicate delivery, worker termination, partial upsert, missing AI/binding, permanent failure | Oldest queued/processing age, lease recovery, attempt count, eligible-versus-indexed chunk coverage, tokens/inferences repeated |
| Reviewer | Large groups, >4,000 candidates, rapid selection, concurrent reviewers | Payload/heap size, query cost, complete queue traversal, correct global totals, no stale form writes |
| Assistant/drafting | Long conversations, follow-ups, provider timeout/slow body, no evidence, contradictory/adversarial evidence | Time to usable answer, grounded-claim accuracy, abstention quality, citation precision, context/output size, model cost, persistence correctness |
| Web | Actual deployed adapter; cold mobile connection; blocked font/provider requests; direct detail URL | LCP/INP/CLS, navigation failures, keyboard completion, transferred JS/fonts, useful content during partial failure |

Use these measurements to decide on caching, model batching, or extra parallelism. Cache keys must include permissions, effective filters, corpus/reference revision, and ranking/model version; TTL alone does not make an outdated legal answer safe. Do not reduce ranking/evidence quality to improve a latency number.

## Review coverage and validation results

### Scope covered

The review traced the main data paths across the Worker entry point and route handlers, shared schemas, parsing and source storage, ingestion/review/approval, normalized legal references, retrieval foundation and activation, vector queue/sweeper, lexical/FTS/vector search and ranking, assistant/drafting/export, browser API clients/state and key screens, all 12 migrations, deployment configuration/workflows, and the operational script/test layout. High-risk migration/activation/reviewer scripts were read in detail; historical report generators were sampled for shared patterns rather than each being executed.

At the reviewed commit the repository had 480 tracked files, including 50 API TypeScript files, 25 web source files, 129 API test/fixture files, and 211 API script/support files. These counts describe the inventory, not proof of execution coverage. Most risk is concentrated in trust boundaries, state transitions, retrieval orchestration, and assumptions shared by many scripts.

### Checks actually run

| Check | Result | What it establishes / does not establish |
| --- | --- | --- |
| `pnpm -r typecheck` | Passed for all four workspace packages | Existing static types compile; does not validate external payloads or runtime state |
| API `test:source` | 84 passed | Current source guards pass; many assert source structure rather than behavior |
| API `test:utils` | 38 passed | Existing utility behavior passes |
| API phrase-relevance + queryability test command from CI | 10 passed | Targeted existing checks pass; overlaps broader suites, so not counted as 10 unique additional tests |
| Web `test:web` | 24 passed | Existing web test suite passes; does not replace rendered browser interaction tests |
| Web `build` | Passed on installed Next.js 15.2.0 | Next build succeeds; the Pages adapter and deployed runtime were not validated |
| API `test:search-golden` | 1 skipped, 0 passed | Local API server was unavailable; this is **not** a successful live golden evaluation |
| SQLite migration sequence and isolated SQL probes | Migrations applied; integrity check passed; defects below reproduced | Exercises local SQLite semantics, not production D1 performance/limits |
| Local targeted TypeScript probes | Defects below reproduced | Executes actual pure/service logic with isolated inputs and mocks where noted; no production writes |
| Static test-import inventory | No missing imports found | Avoids attributing skipped tests to nonexistent helper files |

The successful web build reported shared first-load JavaScript of approximately **117 kB**, with search about **160 kB**, assistant **159 kB**, ingestion admin **156 kB**, and decision detail **153 kB**. These are build estimates, not transfer measurements or evidence of a runtime performance defect. Local execution used Node 24.1.0/pnpm 9.15.1; checked-in CI uses Node 22/pnpm 9.12.0, so CI remains the authoritative runtime compatibility check.

### Reproductions to preserve as regression tests

| Finding | Isolated input / sequence | Observed behavior |
| --- | --- | --- |
| H05 | Synthetic `%PDF-1.7` document containing catalog/object syntax and no meaningful text | Parser returned a nonempty `Body` made from PDF source syntax |
| H06 | Markdown containing `Rule 10.10(c)(3)` and `Ordinance 37.2(g)` | Extracted `Rule 10.10` and `Ordinance 37.2`; a prefixed critical ordinance also failed critical-reference detection |
| H07 | Paragraph `Zebras orbit Jupiter.` with two unrelated authorities explicitly labeled conclusions/analysis | `buildParagraphSupport` returned **strong** support from section-label bonuses |
| H11 | Approval service with a mocked document having confirmed metadata and positive QC flags but no validated reference proof | Approval returned true; stronger reviewer-readiness policy was not consulted |
| H13 | Parent activation batch + child activation document, then SQLite `INSERT OR REPLACE` on the parent | Foreign-key cascade removed the child row |
| H14 | Two concurrent vector-job invocations with a mocked processing-state row and empty chunk set | Both acquired work and completed; demonstrates the missing exclusive claim, not a real Queue concurrency benchmark |
| M10 | Apply migrations 1–7; insert valid source chunk; apply migrations 8–12 | One source chunk, zero FTS rows |
| M07 | EXPLAIN FTS delete by UNINDEXED source/chunk fields versus rowid | Unconstrained virtual-table scan versus rowid-constrained lookup |
| M11 | Decision-only AND fallback OR low-confidence filter over decision and law rows | Low-confidence law row bypassed the decision-only condition |
| M12 | COALESCE metadata update with explicit null/false inputs | Prior value/confirmation remained rather than being cleared |
| M15 | Validate 21 alternating messages, ending with the eleventh user turn | Request rejected by the 20-message maximum |
| M23 | No taxonomy signal in text or metadata | First case type selected with `fallback: false` despite weak-signal reason |
| M25 | Empty JSON request passed to the common reader/error mapper | Syntax error classified as 500 rather than a client error |

These probes used synthetic local inputs, in-memory SQLite, and mocked external bindings. No user case files were sent to a model, and no production corpus/index was changed for this audit.

### Limits and follow-up verification

This is a detailed repository and local-execution audit, not a guarantee that every possible defect has been found. Passing checks are reported alongside failures; speculative optimization ideas are not presented as measured speedups.

- Production Cloudflare Access policies, alternate hostnames, IAM/service tokens, dashboard observability, deployed migration state, actual corpus quality, and backup retention were not inspected. H01/H17 and recovery readiness need that environment evidence.
- The original audit did not include browser interaction/accessibility checks. Subsequent local browser verification is recorded in the High remediation items. Production load testing, full populated-corpus relevance evaluation, external LLM quality evaluation and a production disaster-recovery drill remain outstanding.
- Full production dependency advisory scanning was **blocked by automatic approval review**: `pnpm audit --prod --json` would send package names and versions to the npm registry. Permission for that specific transmission was requested and remains pending. The scan was not run or bypassed. H02 is independently supported by the installed version and the official public Next.js security bulletin; transitive dependency vulnerabilities have not been exhaustively assessed.
- Public Cloudflare and Next.js documentation was retrieved for the platform/security checks. The Next.js bulletin establishes known fixes for its published issues; it is not a complete September 2026 dependency advisory inventory.
- The original audit changed only this Markdown document. Subsequent remediation records identify code/dependency changes and local checks; no deployment or production mutation is implied by closing a local implementation item.

### Repair record template

Copy beneath a finding when beginning work:

```markdown
- Owner / started:
- Fixing PR / commit:
- Scope and design decision:
- Regression fixture / acceptance evidence:
- Migration or backfill required:
- Compatibility and rollback procedure:
- Production verification / metrics:
- Remaining limitations:
- [ ] Verified and closed
```
