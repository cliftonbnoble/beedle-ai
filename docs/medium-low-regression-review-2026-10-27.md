# Medium and Low regression review — 2026-10-27

## Scope and disposition

Reviewed all **26 Medium and seven Low finding fixes**, including their interactions with the completed High fixes. Authentication/H01 is deferred. Five findings needed corrections: **M04, M12, M13, M26 and L07**. Corrections and regression tests belong to their existing finding commits; the final history retains one commit per finding. The High history through `6e70d5c` is unchanged.

The review started from `c47609b`, retained locally at `codex/medium-low-review-baseline-2026-10-27`. Review combined source and migration inspection, failure/retry/concurrency tracing, new failing regression fixtures, existing behavior checks, native Worker/D1/R2/Queue recovery and actual Pages/browser acceptance. Passing fixtures establish the tested invariants, not exhaustive correctness or production relevance.

## Corrections

### M04 — vector maintenance cannot progress under a valid small write budget

- **Problem:** Promotion and cleanup used fixed batches of 16 and 32 vectors. A configured per-minute or daily vector-write budget of one is valid, but reserving either fixed batch fails as larger than the policy. Retrying later never fixes this policy mismatch.
- **Correction:** Derive both metadata and deletion batch sizes from the shared configured compute-budget limit before selecting work. Claim/read cleanup batches use the same bound. Preserve ownership, acknowledgement verification and source-generation checks.
- **Evidence:** The new two-source fixture failed with `Compute batch exceeds configured policy` before the change. With a one-vector budget and simulated next-minute windows it now promotes, retires and deletes one item per delivery, holding readiness until all work completes. The combined generation/filter/cost tests passed (28 cases).
- **Compatibility and risk:** No migration or receipt rewrite. Very small budgets intentionally take more deliveries. Real Vectorize visibility and quota timing still require provider acceptance; uncertain acknowledgements continue to hold recall.

### M12 — a true no-op metadata save revokes confirmation because of JSON formatting

- **Problem:** Omitted or unchanged reference arrays and unchanged reviewer overrides were parsed and serialized again. Historical JSON with whitespace then differed byte-for-byte, firing document-change triggers and clearing metadata confirmation even though the reviewer changed no value.
- **Correction:** Retain the stored JSON bytes when references are omitted or have the same ordered values, and retain override bytes unless an override actually changes. Actual edits, explicit clears, revision fencing and atomic mutation receipts keep their previous semantics.
- **Evidence:** Before correction, the formatted historical fixture advanced revision 5 to 15. New omitted/explicit-identical cases retain revision, confirmation timestamp and original bytes. Combined metadata/mutation-contract coverage passed (21 cases).
- **Compatibility and risk:** No receipt digest or fingerprint algorithm changes. Existing retry identities remain valid. Reordered reference arrays remain changes; malformed legacy state still follows its existing validation hold. Actors remain unknown pending authentication.

### M13 — imported non-finite confidence creates an unusable next-page cursor

- **Problem:** SQLite can retain an imported infinite confidence. Sorting by it generated a cursor whose JSON encoded Infinity as null, then rejected that cursor on the next request. The invalid value could also exist in an already materialized queue.
- **Correction:** Reject non-finite source confidence before publishing materialized rows; keep the source dirty. Validate outgoing cursor sort values, including finiteness/type/size, and return a controlled queue-unavailable response for malformed cached rows. Repairing source data and refreshing the queue is required; the code does not invent a confidence value.
- **Evidence:** The new regression initially failed because materialization accepted the invalid value. It now verifies both the source hold and the controlled read failure for a corrupted cached row. Complete queue coverage still traverses 4,207 rows and all supported tie-heavy sorts.
- **Compatibility and risk:** No schema or policy-version change. Valid existing cursors retain their format. Previously invalid data now holds explicitly instead of returning a cursor that fails later; operators must repair it.

### M26 — incomplete D1 inspection output is treated as an empty target

- **Problem:** Recovery flattened absent `results` to an empty array and rejected only explicit `success:false`. Missing success/results fields and an empty top-level array could therefore pass as evidence of an empty target, without a successful inspection.
- **Correction:** Require a nonempty result envelope, explicit `success:true`, array results and object rows for every result. Invalid JSON, partial/missing envelopes and malformed rows fail before target claims or copy work. Valid empty query results remain supported.
- **Evidence:** New parser cases failed before the change and now reject empty/partial/wrong-type/mixed output. Portable recovery fixtures and the full native capture/plan/apply/verify/rebuild drill validate the real Wrangler output contract as well as the pure parser.
- **Compatibility and risk:** Bundle and durable receipt formats are unchanged. Unexpected CLI output now blocks the operation. Remote targets still need the documented isolated rehearsal, maintenance window and rollback plan; local success does not prove remote timing or atomicity.

### L07 — independent reader requests can mix corpus revisions

- **Problem:** Optional search starts after the document loads. A reviewer edit between requests could revoke/change the document while search returned a newer corpus revision, attaching newer passage matches or a misleading empty result to the older captured text. The preview had no freshness token.
- **Correction:** The preview route captures and rechecks the corpus epoch around its reads and returns it with `private, no-store`. Match context includes that identity and accepts only an equal search revision. Missing or changed evidence retains readable captured text, withholds matched-passage counts/indicators and offers a decision reload. Reload rechecks publication. Body loading remains independent of optional search failure.
- **Evidence:** The API regression initially received no revision and failed. It now verifies stable identity/no-store and a 409 on a mid-read epoch change. Actual Pages browser cases edit a real synthetic decision between preview/search, verify no newer matches attach, and confirm reload rejects its revoked publication. Missing preview/search token cases retain text without match indicators.
- **Compatibility and risk:** The shared preview field is optional for older/internal DTOs, but the UI fails closed for match association without freshness evidence. No stored browser data or URL migration. Already displayed text is a snapshot, not a live publication subscription; publication changes after both requests settle are checked on the next load. No cross-tab result cache is introduced. See [reader behavior and rollback](decision-reader-loading.md).

## Finding-by-finding review

“No additional defect found” means no actionable regression was demonstrated in this review; the listed acceptance limits still apply. Test names below refer to the API `*-behavior.test.mjs` suites unless otherwise specified.

| Finding | What was checked; outcome and evidence |
| --- | --- |
| M01 | Route cost classification, aliases, missing-limiter failure, atomic shared minute/day budget reservations and compute units. No additional defect found. `cost-control`, native route/provider checks; quotas do not establish identity. |
| M02 | Provider timeout through headers/body/parsing, UTF-8/size bounds, body cancellation and sanitized failures. No additional defect found. `provider-operation`, native malformed/oversized provider fixtures. Synchronous CPU cannot be preempted by an async timer. |
| M03 | Frozen finite plans, keyset progress, durable owner/lease checks, cancellation, source/catalog changes, late completion and observed parallel failures. No additional defect found. `maintenance-jobs`, runtime contract and recovery tests; old unverifiable plans remain held. |
| M04 | Profile/source/text identities, cached embeddings, visibility verification, lifecycle locking, retirement, uncertain acknowledgements and GC. Corrected small-budget progress as above. `vector-generations`, `vector-jobs`, `vector-outbox` and native Queue retry. Physical IDs are not reused after an uncertain initial write/deletion. |
| M05 | Metadata/cohort filters before top-K, exact fallback partitions, string/hash limits, date ranges, filter byte/partition bounds and readiness versions. No additional defect found. `vector-filter`, current Vectorize documentation/types and native probes. No claim of exhaustive semantic recall. |
| M06 | Indexed lexical/FTS candidate channels, bounded corrections/fallbacks, projected bytes/rows/statements/time, scope constraints and SQL/LIKE compatibility. No additional defect found. `search-indexed-recall`, `search-work-budget`, phrase/queryability guards; representative latency remains unmeasured. |
| M07 | Stable FTS identities across updates/deletes, shadow-index triggers and cutover/rebuild behavior. No additional defect found. `search-rowid`, native populated FTS recovery. No broad ranking rewrite. |
| M08 | Cursor request/scope/profile/epoch/expiry binding, immutable bounded slate, capacity reservation/cleanup, page-size changes and revision checks after reads. No additional defect found. `search-pagination`, native/browser pagination. Bounded candidate totals are not corpus totals. |
| M09 | Explicit migration/readiness probes, no request-time DDL and no sticky failed-isolate initialization. No additional defect found. `search-readiness`, native fresh database hold/recovery. |
| M10 | Finite build bounds, live-write triggers, owner fencing, failure/resume, lost postings, readiness coverage and rollback. No additional defect found. `search-index-recovery`, native rebuild including restored data. Archived derived FTS tables are not authoritative backups. |
| M11 | Low-confidence OR branches remain within surrounding AND scope, both before and after reviewer materialization. No additional defect found. `reviewer-filter`, native scoped list results. |
| M12 | Null/omitted/false patches, source revisions, confirmation invalidation, no-op behavior, atomic failure and lost-ack receipt replay. Corrected historical JSON no-ops as above. `reviewer-metadata`, `mutation-contracts`, native rollback/recovery receipt replay. |
| M13 | Fifteen sorts, tie traversal, variable page sizes, exact global counts, peer groups, dirty/epoch/catalog fences, lost derived rows and bounded metadata. Corrected invalid outgoing sort values as above. `reviewer-queue`, browser reviewer pagination; corpus-scale drain capacity remains a release measurement. |
| M14 | Unprivileged retrieved content, evidence ID allowlists, exact normalized quotes, provision/Part boundaries, number/negation checks and bounded fallback. No additional defect found. `assistant-evidence`, draft evidence/review and native provider rejection. Mechanical checks do not certify legal entailment. |
| M15 | Shared message/history/model budgets, safe surrogate truncation, transport limits and preserving stored history while preparing model context. No additional defect found. `assistant-context`, web store/session tests. |
| M16 | Follow-up scope/fingerprints, selected case sets, every accepted filter, named-case eligibility, complementary evidence and post-provider epoch checks. No additional defect found. `assistant-retrieval`, native/browser context checks. Unsupported fields fail strict DTO validation. |
| M17 | IndexedDB atomic revisions, pending identity, receipts/tombstones, conflicts/forks, late completions, storage failure recovery and bounded legacy imports. No additional defect found. Web store tests and actual Pages cross-tab/identity races. Persistence remains device-local. |
| M18 | Immutable descriptors/source hashes, jurisdiction/effective intervals, unknown/ambiguous applicability, supersession, digest aliasing and generation history. No additional defect found. `reference-editions`, generation/draft-law and native recovery. Catalog provenance is not legal certification. |
| M19 | Real runtime/browser CI gates, fresh fixtures/fake providers, bounded holdout collection, sealed labels, separate reviewer, digest/corpus/time checks and explicit human acceptance. No additional defect found. `independent-holdout`, native/browser suites. No independent labels or fresh provider-quality results were created. |
| M20 | Exact migration/config/code/vector/reviewer readiness, protected manual release, prior preview evidence, canary source checksum and explicit rollback limits. No additional defect found. `release-readiness`, native release tests, actual Pages build/browser, offline Worker dry run. Hosted protection/perimeter was not inspected. |
| M21 | Binding proxy receiver correctness, bounded labels/body state/correlation, failure-safe emission, sampling weights and durable job health. No additional defect found. `telemetry`, `operational-monitor`, native scheduled heartbeat. Quota-denial sampling is intentional and documented; degraded/5xx events remain unsampled. |
| M22 | UUID/fingerprint/predecessor identity, fenced leases and writes, immutable source keys, lost acknowledgement, replay correlation and causal source revisions. No additional defect found. `ingestion-idempotency`, native polling/recovery. No account-scoped identity or automatic case merge was added. |
| M23 | Jurisdiction review, no-signal/weak/tied/negated evidence, unknown fallback and truthful UI policy. No additional defect found. `taxonomy-inference`, native reviewed/unreviewed fixtures. Scores remain uncalibrated. |
| M24 | Formula prefixes after Unicode control/space/fullwidth characters, finite numeric cells, quotes/BOM/CRLF, logical multiline records and cell/row bounds. No additional defect found. `spreadsheet-csv`, worksheet utilities and native export. Real spreadsheet-client acceptance remains external. |
| M25 | Strict bounded mutation schemas/media/UTF-8, explicit empty commands, error privacy, real calendar dates, bounded captured reference plans and epoch fencing. No additional defect found. `mutation-contracts`, native malformed/date routes, maximum reference arrays and plan reuse. Existing receipt fingerprints are preserved. |
| M26 | WAL snapshot fidelity, typed row/schema/object seals, explicit targets, target claims, resumable receipts, SQL/file limits, import ordering and derived-state rebuild. Corrected incomplete inspection output as above. `recovery-bundle` and full native recovery rehearsal. |
| L01 | Extracted function parity, search/reviewer dependency boundaries, shared filters and unchanged scoring/policy contracts. No additional defect found. Boundary/parity behavior, source guards and native/browser suites; deliberate review corrections are described above. |
| L02 | Generated binding/config types, secret isolation, focused lint, unknown external payload parsing and stored plan/receipt identity. No additional defect found. Type/lint negative contracts, generated freshness and malformed payload behavior/native checks. No dependency/runtime upgrade was made during review. |
| L03 | All font declarations use the shared system stack; no external font chain; desktop/tablet/mobile overflow/network coverage. No additional defect found. Actual Pages typography browser check. Earlier single-run LCP did not improve; OS typography/manual acceptance remains. |
| L04 | Dialog Strict Mode cleanup, focus return when origin disappears, inert background, Tab/Shift-Tab, Escape/cancel, pointer crossing and reviewer labels/status. No additional defect found. Actual Pages keyboard/dialog/axe and cross-tab removal flows. Manual screen-reader/device acceptance remains. |
| L05 | Planned page has no chooser/fake progress/upload API; synthetic drop/click behavior and real reviewer navigation. No additional defect found. Actual Pages planned-intake/axe case. Browser intake remains unavailable. |
| L06 | Explicit readable regular SQLite path/header, canonical paths/spaces/relocation, no auto-discovery, guarded legacy entrypoints and current runbook links. No additional defect found. Operator-path behavior/contract tests and unchanged snapshot audit from implementation. Legacy raw SQL writers are unsupported live workflows. |
| L07 | Document/query identity, abort/late response, independent retry, source anchors, cross-profile mapping, publication checks and URL presentation reuse. Corrected cross-request freshness as above. Pure anchor checks, API publication behavior and actual Pages reader flows. |

## Hypotheses checked without changing behavior

- Vectorize string range filters are supported and lexicographic; ISO date ranges are not an unsupported-filter bug. Checked against [official metadata-filter documentation](https://developers.cloudflare.com/vectorize/reference/metadata-filtering/) and current published Worker types.
- Reviewer receipt canonicalization is an existing deterministic digest input, not a serialized interchange document. Reformatting it merely to resemble JSON would invalidate old replay fingerprints; no algorithm change was made.
- Uncertain vector acknowledgements deliberately hold visibility. Removing the hold for availability would weaken evidence integrity; unique physical incarnations prevent a delayed old deletion from deleting a fresh vector.
- The budget wrapper's unused `raw({columnNames:true})` edge is not exercised by application calls. No speculative unused-API expansion was added.
- Recovery's initial empty reviewer upper bound is followed by dirty-source processing and exact count verification. It does not prematurely promote an empty queue on a populated target.
- CSV protection, taxonomy scores and mechanical quote checks retain their documented limits. Passing synthetic fixtures does not justify stronger spreadsheet, calibrated confidence or legal-quality claims.

## Final verification

All final checks passed:

- **321 API behavior tests**, **84 API source guards**, **38 utility checks**, **10 phrase/queryability checks**, **49 web checks** and **four type/lint contract checks**.
- **All 16 native Worker/D1/R2/Queue/recovery cases**, including sealed restore, missing-checkpoint/lost-ack replay, changed-target detection, another fresh restore, FTS/reviewer rebuild and restored source/receipt verification (about 215 seconds).
- **All 21 Chrome flows against actual Pages adapter output**, including eight reader cases and the existing reviewer/chat/keyboard/axe/typography/intake flows. The new race uses a supported case-number edit through the real reviewer API, not a fixture-only publication bypass.
- Workspace and integration TypeScript; final web TypeScript; generated Worker binding/runtime freshness; focused API ESLint; framework security-floor guard; actual Pages build; production-configuration Worker **dry-run** bundle; whitespace/diff checks.

Reproduction commands, from the repository unless a directory is specified:

```sh
pnpm --filter @beedle/api test:behavior
pnpm --filter @beedle/api test:source
pnpm --filter @beedle/api test:utils
# Run from apps/api (these source guards resolve paths from their package):
node --test tests/search-phrase-relevance.test.mjs tests/retrieval-search-queryability-gate.test.mjs
pnpm --filter @beedle/web test:web
pnpm --filter @beedle/api test:contracts
pnpm typecheck
pnpm --filter @beedle/api test:integration:types
pnpm --filter @beedle/api types:check
pnpm --filter @beedle/api lint
pnpm --filter @beedle/web check:framework-security
pnpm --filter @beedle/api test:integration
NEXT_PUBLIC_APP_ENV=local NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8819 pnpm --filter @beedle/web pages:build
BEEDLE_E2E_ADAPTER=pages BEEDLE_TEST_CHROME_CHANNEL=chrome WRANGLER_SEND_METRICS=false pnpm --filter @beedle/web test:e2e
WRANGLER_SEND_METRICS=false pnpm --filter @beedle/api deploy:production --dry-run
```

The native suite and Pages build/browser suite ran in separate heavy stages. Browser fixtures require free loopback ports 5819/8819 and do not reuse an existing developer service. Logs for this session were captured in `/tmp/beedle-review-*`; browser screenshots/traces/reports are ignored generated artifacts under `output/playwright`. The durable conclusions are in this ledger and the finding records.

All fixtures use disposable local resources and synthetic source/provider data. No production requests, deployment, push, authentication implementation or paid inference are part of this review. History acceptance requires exactly 26 Medium plus seven Low commits after the unchanged High baseline, no remaining fixup commits, a clean checkout, and the same final tree as the tested pre-fold snapshot.

## Compatibility and release limits

No migration, dependency change, stored receipt rewrite or new authentication behavior is introduced. M13 can expose invalid historical data as a controlled hold; M26 rejects previously tolerated incomplete CLI output; L07 withholds match association when preview/search freshness cannot be established. These are intentional corrections, with operator repair/reload paths described above.

Representative legacy state, actual provider visibility/shapes, independent relevance judgments and source/legal review, capacity/SLOs, OS/screen-reader acceptance, hosted environment protection and isolated remote recovery remain release work. The adapter support gap remains [ADR 0001](decisions/0001-cloudflare-web-hosting.md). The previously rejected dependency-inventory registry audit was not retried or bypassed. Public current Cloudflare documentation/types were used for API contract checks; project dependencies were not changed.

Current primary contract references: [Worker best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/), [D1 database API](https://developers.cloudflare.com/d1/worker-api/d1-database/), [Queue delivery guarantees](https://developers.cloudflare.com/queues/reference/delivery-guarantees/), and [Vectorize filters](https://developers.cloudflare.com/vectorize/reference/metadata-filtering/). Environment observations and local fixtures, rather than these references alone, support the code-level conclusions.
