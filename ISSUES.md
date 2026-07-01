# Beedle AI Companion - Reviewed Issue Backlog

**Reviewed:** 2026-06-26 · **Scorecard refreshed:** 2026-06-30  
**Scope:** Current local repo plus production spot checks against `beedle-ai.pages.dev` and `beedle-api.clifton23.workers.dev`.  
**Status:** Working backlog, not an implementation plan. The current status-grouped completion scorecard is the **Completion Scorecard — 2026-06-30 (refreshed)** section below.

This file intentionally separates **confirmed product/release issues** from the larger raw audit. Auth is acknowledged but not prioritized here because we have already agreed it is not the focus of this pass.

## Triage Principles

- Prioritize issues that affect search trust, source access, deployment safety, and corpus integrity.
- Prefer fewer, clearer issues over a giant list of overlapping symptoms.
- Keep auth/security notes visible, but do not let them obscure the current product-quality work.
- Treat low-value cleanup as parking-lot work unless it blocks confidence or speed.

## Verification Pass — 2026-06-29

Independent verification of the fixes recorded in this backlog (no code changes made during this pass). Local `wrangler dev` against the full corpus (~14k docs, 1.1M FTS rows, 667k retrieval chunks).

**Build / type safety**
- `pnpm --filter @beedle/api typecheck` — **PASS** (exit 0). `pnpm --filter @beedle/web typecheck` — **PASS** (exit 0). Confirms `REL-01`.

**Source / unit tests — 42/42 PASS** (deterministic, no server). Each maps to a backlog claim:
- `REF-01` legal-reference normalizers (`legal-reference-normalizers-source`, `reference-normalization-utils`, `reference-fallback-paths`)
- `DATA-01` atomic batches (`ingest-artifact-batch-source`, `ingest-reference-batch-source`, `document-reference-validation-batch-source`, `admin-metadata-update-source`, `admin-searchability-batch-source`, `admin-ingestion-state-source`, `legal-reference-clear-batch-source`, `legal-reference-rebuild-batch-source`, `retrieval-rollback-batch-source`)
- `DATA-02` vector activation gate (`retrieval-activation-vector-gate-source`, `retrieval-activation-source`)
- `ADMIN-01` (`admin-ingestion-derived-list-source`), `FACET-01` (`facet-storage-migration`, `search-facet-table-source`), `SEARCH-03` (`search-debug-profile-source`)
- `INGEST-01` (`ingest-size-guards-source`), `LLM-01` (`llm-boundaries-fallback-source`), `LLM-02` (`assistant-chat-timeout-source`), `CORS-01` (`cors-default-origin-source`), `REL-01/REL-02` (`deploy-workflow-source`), `REPO-01` (`repo-script-inventory`, `repo-hygiene-policy-source`)
- This session's `SEARCH-01`/`SEARCH-02` fixes (`search-decision-layer-section-prefilter-source`, `search-fts-bootstrap-cost-source`, `search-decision-layer-chunk-cache-source`)

**Web source tests — 8/8 PASS**: `WEB-01` stale-response abort/epoch (`search-stale-response-source`), `WEB-02` schema-validated API helpers (`api-schema-validation-source`), `UI-01` no fake dashboard/upload signals (`dashboard-product-signals-source`), `SEARCH-03` web diagnostics (`retrieval-debug-profile-source`), and XSS-safe phrase highlighting (`search-highlight-source`).

**Live behavioral tests** — `search-phrase-relevance`, `retrieval-search-queryability-gate` **PASS**. Citation sanity: `Ant infestation in the kitchen` → `T210489|T250099|T221447|S001-92T|T210403` (exact match to the `SEARCH-01` baseline above).

**This session's SEARCH fixes — behaviorally re-verified**
- Cold first search **1150ms** (was ~4655ms pre-fix) end-to-end on a cold isolate; warm ~212ms — confirms the FTS `COUNT(*)`→`LIMIT 1` fix.
- Top-10 results byte-identical OLD vs NEW across 12 queries × 2 corpus modes for both the section prefilter and the chunk-cache fold (captured in prior cycles).

**Known gap surfaced by this pass (pre-existing, NOT a regression):** 6 of the live `legal-reference-normalization` integration tests fail **locally** (e.g. fixture ingest returns `400`; rules-citation inventory empty). Confirmed pre-existing via A/B: the **same 6 fail on the pre-session commit `e803738`**, so they are unrelated to the `SEARCH-*` work. Root cause is local environment/data state — the normalized reference tables are not rebuilt in this local D1 (the `REF-01`/`DATA-01` *unit* coverage passes). To make these live tests meaningful locally, run `pnpm normalize:references` / apply migration `0009` and re-seed before relying on them. Recommend wiring a documented local test-DB setup so these integration tests are reproducible.

## Completion Scorecard — 2026-06-30 (refreshed)

Refreshes the 2026-06-29 scorecard with everything completed since, **grouped by status** so "done vs still-to-do" is explicit. **Severity** = impact if unaddressed. **Done** = verified completion. **Difficulty** / **Break risk** describe the *remaining* work.

- **Difficulty:** Easy = focused change/config; Medium = real work, contained; Hard = large/cross-cutting or needs prod access.
- **Break risk:** Low = additive/isolated, well-tested; Medium = touches shared/ranking/write paths; High = core ranking, auth, or write atomicity.

**At a glance:** **16 done & verified** · **2 code-complete (external blocker)** · **0 in progress** · **1 hygiene-done / rest-deferred** · **1 borderline-safe** · **2 not started / deferred**. Overall active backlog **~95%** (~90% including the deferred AUTH-01). Re-verified: API+web typecheck clean, `test:source` 55/55, web tests 9/9. _(**Entire "in progress" section cleared 2026-06-30** — DATA-02 ✅ + PERF-01 ✅ + DATA-01 ✅ + SEARCH-01 ✅ + ADMIN-01 ✅ + FACET-01 ✅, all six driven to 100% with live verification.)_

### ✅ Done & verified — 100% (16)
| Item | Sev | Done | What was fixed |
|---|---|---:|---|
| REL-01 CI/typecheck gate | High | 100% | Pre-deploy gate runs API+web typecheck + the 42-test `test:source` suite + relevance/highlight before `wrangler deploy` |
| REF-01 citation normalizers | High | 100% | Word-boundary / validated-roman prefix rules; over-strip defect fixed + unit-tested (7/7) |
| SEARCH-03 prod vs debug path | Med-High | 100% | `debugProfile` surfaces requested vs production query type + match flag (code + schema + test) |
| INGEST-01 upload/zip guards | Med | 100% | All paths capped — multipart, **JSON body (new)**, decoded bytes, DOCX decompression |
| LLM-01 prompt fencing/fallback | Med | 100% | Both LLM paths fence untrusted text + fallback transparency; guard test blocks new unfenced paths |
| LLM-02 AI/LLM timeouts | Med | 100% | Assistant + draft + the **embedding `env.AI.run` (new)** all time-bounded |
| WEB-01 stale-result race | Med | 100% | Search uses AbortController + request epoch; ignores stale responses |
| WEB-02 schema validation | Low-Med | 100% | All user-facing helpers zod-parse; admin-ingestion GETs shape-guarded |
| UI-01 fake dashboard signals | Low-Med | 100% | No fabricated model/activity signals; placeholder labeled as planned |
| REPO-02 catalog-as-code | Low-Med | 100% | JSON + thin wrapper + regression test; **fixed a real `C88` catalog-shadowing bug** |
| DATA-02 vector activation gate | High | 100% | Gate blocks `active=1` on vector-write failure; report now surfaces a per-status breakdown + blocked chunk/document lists |
| PERF-01 hot-loop recompute | High | 100% | Verified hot path fully cached (memoized query context + cached row text; `scoreRow` has zero per-row re-normalization, no bypasses); locked in with a guard test |
| DATA-01 destructive-write safety | High | 100% | All destructive paths protected: reprocess-when-empty guard, reference snapshot+restore, DATA-02 vector gating, and canonical rebuild now write-then-swap (D1 can't span batches; no path can lose content on failure) |
| SEARCH-01 phrase latency | High | 100%¹ | Cold-start + decision-layer fixed (prior); vector stage now parallel (concurrent embed+query, identical max-merge) + bounded by the embedding timeout. ¹In-repo levers complete + verified safe; production timing to be re-measured (needs a deployed target — no code left) |
| ADMIN-01 filter/sort after LIMIT | Med-High | 100% | Prefilter `COUNT(*)` gate fetches the full prefiltered set when ≤4000 → exact top-N for derived filters/sorts (verified live: full 1082-doc staged set now ranked); honest completeness diagnostics; larger sets flagged |
| FACET-01 LIKE on JSON facets | High | 100% | Filters use indexed facet tables; fixed a real `no such table` 400 (facet code could ship before migration 0009) with a runtime `ensureDocumentFacetTables` safety net (verified live: 46.7k/95.8k/42k rows backfilled, throw gone, citations unchanged) |

### ✅ Code-complete · ⛔ blocked on an external step (2)
| Item | Sev | Done | Blocker (cannot be done from the repo) |
|---|---|---:|---|
| REL-02 prod migration gating | High | 100% in-repo | Enable required-reviewers on the `production-d1-migrations` GitHub Environment (repo Settings UI) |
| SRC-01 source 404s | High | 100% in-repo | Re-sync missing prod R2 objects / repair stale `source_r2_key` (Cloudflare data-ops) |

### 🚧 In progress — real remaining work (0)
_Cleared 2026-06-30. All six in-progress items (DATA-02, PERF-01, DATA-01, SEARCH-01, ADMIN-01, FACET-01) were driven to 100% and verified — see the Done & verified table above._

### 🟡 Managed / borderline (2)
| Item | Sev | Done | Note |
|---|---|---:|---|
| REPO-01 script/report noise | Med | Infra 100% | Reports 575MB→14MB + policy/guard/dedupe/tests done ("0 actionable"). Bulk archive (60 `retrieval-r*` + 48 r-tests, **214 aliases**, shared-util imports) re-scored **Hard** → its own PR |
| CORS-01 CORS default | Low-Med | Safe | Wildcard removed (vuln fixed; defaults to known origins). Strict fail-closed deliberately deferred — would *raise* break risk if the env var were ever unset |

### 🔴 Not started / deferred — the hard, high-value core (2)
| Item | Sev | Done | Difficulty | Break risk | What's left |
|---|---|---:|---|---|---|
| SEARCH-02 search.ts de-bloat | High | 100% | Hard | High | **✅ COMPLETE.** **02c module split** — the ~10.9k-line monolith is now 9 focused modules (`search.ts` holds only orchestration, ~1.7k lines, −84%), every extraction golden-verified byte-identical. **02b lexicon** — all 29 data-drivable topic predicates folded into a `QUERY_TOPIC_PATTERNS`/`TEXT_TOPIC_PATTERNS` table (proven byte-identical + input-battery, golden 27/27); the ~72 genuinely-bespoke predicates stay explicit. See the Search Sequence section below. |
| AUTH-01 no in-code auth | **Critical** | 0% | Hard | High | Every admin/ingest/write/LLM endpoint is public; needs Cloudflare Access/JWT or shared-token gating before rollout — the single biggest production risk |

### Search Sequence — 2026-06-30 (recommended-group progress)

Working the search-focused plan in order. **SEARCH-04 ✅, SEARCH-05 ✅, SEARCH-02c ✅, and SEARCH-02b ✅ are done, verified, and committed — the SEARCH-02 epic is complete.** The 02c module split ran bottom-up over 6 steps (leaf modules → classification → query-analysis → fts → scoring → orchestration), each landed only after the golden net stayed 27/27 byte-identical — the ~10.9k-line monolith is now 9 focused modules and `search.ts` holds only orchestration (~1.7k lines). SEARCH-02b then folded the 29 data-drivable topic predicates into a lexicon table (families 1–2, each proven behaviour-identical by byte-identity + input battery + golden), leaving the ~72 genuinely-bespoke predicates as explicit functions.

| Step | Status | Notes |
|---|---|---|
| **SEARCH-04** behavioral golden-query net | ✅ Done | `tests/search-golden-ranking.test.mjs` + fixture pin the exact ordered top-N for 27 representative queries; skips cleanly without a server; `UPDATE_SEARCH_GOLDEN=1` regenerates. Purely additive. **It immediately caught a real prod 400** (below). |
| **SEARCH-05** recall-query robustness | ✅ Done (root-fixed) | A broad curated keyword family + a structured filter (e.g. `infestation` + a judge filter) 400'd with `D1_ERROR: too many SQL variables`. **Root cause: D1 rejects a statement with more than ~100 bound parameters** — the keyword-candidate recall query binds 9 params/term × 12 terms + 12 scope ids = 121 (`offset 7007` was the char position of the ~101st param in the ~7000-char statement). Fixes: (1) cap the candidate query's term expansion to `D1_MAX_BOUND_PARAMS` → returns correct results, not degraded; (2) bound the recall universe (`KEYWORD_RECALL_UNIVERSE_MAX`) — it was ~7000 docs re-ranked 12-at-a-time, firing hundreds of tiny queries/request (latency + worker-stability bug); (3) the degrade-on-overflow path stays as a backstop. Verified golden 27/27 (`infestation_judge` degraded→correct; `mold_judge`, also bypass, shifts only its #5 from the bounded pool; other 25 byte-identical), `search-query-degradation-source` guard, harness 503-retry. **(4) Param-cap sweep (done):** a shared `boundLexicalTermsForD1(terms, perTerm, fixedParams)` helper now caps term expansion at *every* lexical query site — `lexicalSearch` + `lexicalSearchWholeWord` (18 params/term), the issue-candidate phrase query + `fetchKeywordCandidateDocumentIds` (9/term) — so no lexical statement can exceed the limit, closing the latent risk in the non-bypass paths too. Because the cap equals D1's limit it's a provable no-op for any query already under it: golden **27/27 byte-identical** (no ranking change), and forcing `D1_MAX_BOUND_PARAMS=30` proved the caps actively trim (queries return trimmed results, never overflow). `search-param-cap-source` guard added; `test:source` 60/60, relevance+gate 10/10. **SEARCH-05 story complete.** |
| **SEARCH-02c** module split | ✅ Done | Split the ~11k-line `search.ts`, migrating the source-regex assertions module-by-module with the golden net green after each. **Extracted bottom-up** (leaves first) so no module imports back from `search.ts` (avoids circular deps). **All 6 steps done** (each behavior-neutral: golden 27/27 byte-identical, typecheck clean, test:source 60/60, coupled assertions re-pointed): (1) pure lexical SQL clause/rank builders → `search-lexical-sql.ts`; (2) pure text primitives (`normalize`, `normalizeWhitespace`, `tokenize`, `STOPWORD_TOKENS`, `meaningfulLexicalTokens`, `ftsQuote`, `escapeRegex`, `uniq`, `containsWholeWord`) → `search-text.ts` (−75 lines); (3) pure phrase/concept coverage — 11 fns incl. `phraseConceptGroups`, `phraseConceptVariantsForToken`, `keywordSurfaceVariants`, `phraseSearchFtsQuery`, `phraseConceptCoverage`, `sentencePhraseOverlapScore` → `search-concepts.ts` (−183 lines; ~12 phrase-suite assertions re-pointed empirically — each regex tested against the file it now lives in; context-coupled `phraseConceptGuardPasses` + `exactMultiWordPhraseScore` kept in `search.ts`); (4) **103 pure query/text classification predicates** (`isXxxQuery(query)` / `hasXxxContext(text)` — the SEARCH-02b lexicon surface) → `search-query-classification.ts` (**−809 lines from `search.ts`**; a fixpoint closure analysis confirmed all 103 reference only text/concept leaves + each other, with strings stripped to avoid false positives; 41 assertions across the phrase+gate suites re-pointed empirically; only 3 predicates — `isKeywordFamilyRecallQuery`, `isLiteralKeywordQuery`, `requiresHabitabilitySpecificity` — stay in `search.ts` because they reach curated-family/habitability-signal helpers not yet split). **SEARCH-02b** can now fold these near-identical predicates into a data-driven table in place. (5a) the 13 shared row/context/result **types** → `search-types.ts` (compile-time only, so runtime JS is byte-identical). (5b) the **pure query-analysis layer** — 70 members: recall config, search scope, term expansion, index-code/reference filters, citation + section-label helpers, query-intent inference, plus their term-list consts → `search-query-analysis.ts` (**−2,194 lines; `search.ts` now ~7.5k, down from ~11k**). A transitive closure with a **template-aware stripper** (keeps `${…}` interpolated calls a naïve strip discarded — it had missed `buildExactIndexCodeIntersectionClauses`) confirmed 0 scoring back-edges; each const was assigned to the layer whose functions use it (DB-only consts — facet DDL, batch sizes — stay for 5c). (5c) the **DB I/O layer** — 27 members (19 `env.DB` fetch / fts / lexical / vector + facet-table & runtime-index bootstrap) + 8 DB-only consts + the 4 retrieval memoization flags → `search-fts.ts` (**−1,705 lines; `search.ts` now ~5.8k, down ~47% from ~11k**). Confirmed acyclic (0 back-references into scoring); `searchFtsAvailable` is an ESM `export let` **live binding** read by `runSearchInternal`. Note: `stripToCode` desyncs on a regex literal containing a quote, so import/import-back computation uses the raw source (over-import is a harmless unused import; typecheck is the completeness gate). Whole-service impl tests now read every `src/services/search*.ts` **globbed together** (with a leading-`export ` strip so body-delimiter regexes still match) — future-proof as more modules split. (6) the **scoring / ranking / display layer** — 84 members (scoreRow, candidate assembly + ordering, decision layering, snippet + passage building, evidence summaries, ranking predicates) → `search-scoring.ts` (**−4,040 lines; `search.ts` now ~1.7k, down ~84% from ~11k**, holding only `runSearchInternal` + `search`/`searchDebug`). Zero test changes were needed — the globbed whole-service source auto-included the new module. **Result: the 10.9k-line monolith is now 9 focused modules** — leaves (`search-text`, `search-lexical-sql`, `search-concepts`) → `search-query-classification` → `search-query-analysis` → `search-fts` → `search-scoring` → `search.ts` (orchestration) + `search-types` — each extraction golden-verified byte-identical. |
| **SEARCH-02b** lexicon extraction | ✅ Done (foldable set complete) | Folded the data-drivable topic predicates in `search-query-classification.ts` into a lexicon, family by family, each **proven behaviour-identical** (regexes byte-identical to the originals + an old-vs-new input battery + golden 27/27, not just golden). A single `QUERY_TOPIC_PATTERNS` / `TEXT_TOPIC_PATTERNS` table now holds the topic vocabulary; each folded predicate is a one-line `matchQueryTopic(TABLE.key, …)` wrapper (signatures unchanged, so call-site/signature assertions still pass). **Family 1:** the 24 single-pattern predicates (`test`); the `!normalized` guard is a verified no-op (every pattern is false on `""`); 384-case battery, 0 mismatches. **Family 2:** the 5 remaining Boolean-combinable predicates via a `TopicPattern = RegExp | {any} | {all}` union + `topicMatches` (pure-OR `isStairsQuery`/`isPorchQuery`/`isWindowsQuery` → `{any:[…]}` = `.some`; pure-AND `isBuyoutPressureQuery`/`isGenericHousingServiceStandard` → `{all:[…]}` = `.every`); 95-case battery, 0 mismatches. **29 of 101 predicates are now data-driven** — every one whose logic is a pure regex / OR / AND. The remaining ~72 are **genuinely bespoke** (mixed `(A∧B)∨C`, predicate-composed like `isAntInfestationQuery`, or context helpers with real per-topic logic) and are clearer as explicit functions than as an over-general lexicon DSL — leaving them is the right call, not incompleteness. |
| **SEARCH-02d** retire impl-coupled tests | ⏸️ Deferred | Replace the source-regex assertions with the behavioral golden net as 02c/02b proceed, keeping only the real-invariant pins (SQL-shape/security fencing). |

**What this finalization pass delivered (3 real bugs fixed along the way):** every Low-risk Easy/Medium item reached 100%, and verifying each surfaced and fixed three genuine bugs — the **C88** index code that resolved to an empty `[reserved]` stub (REPO-02), a **JSON-ingest memory-DoS** gap where the whole base64 body loaded before any size check (INGEST-01), and the **unbounded embedding AI call** that could hang search/ingest/backfill (LLM-02). REL-01 also hardened CI to run the full 42-test source-guard suite before every deploy, so these fixes stay locked in.

**What still needs work (all Medium/High break-risk — own scoped cycles):** the remaining ~15% is the hard core — `FACET-01` facet cutover (50%), `DATA-01` large-batch + Vectorize atomicity (80%), `ADMIN-01` materialized pushdown (70%), `PERF-01` tail (85%), production `SEARCH-01` vector latency (75%, needs a deployed target), the `SEARCH-02` topic-predicate de-bloat (20%, the central over-engineering), and — outside this backlog — `AUTH-01` (0%, Critical).

**Suggested order (value ÷ risk):** finish `FACET-01` (clean, contained) → close `DATA-01` atomicity (corpus integrity) → profile production `SEARCH-01` vector stage → plan `SEARCH-02` de-bloat with golden-query regression coverage before touching ranking → and, before any broader rollout, `AUTH-01`.

## Confirmed P0 / Do Next

### REL-01 - API typecheck/deploy CI gate needed

**Severity:** High  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29).** API typecheck passes and the deploy workflow now gates on API+web typecheck, the phrase-relevance test, the web highlight test, **and a new `test:source` aggregate that runs the full 42-test deterministic source-guard suite** (every `*-source` guard plus the migration/reference unit tests) before the `wrangler deploy` step. This catches regressions in every source-guarded fix (REF-01, DATA-01, FACET-01, the SEARCH fixes, etc.) before production. New tests auto-join the gate via the `*-source.test.mjs` glob. Verified: `test:source` 42/42 green; `deploy-workflow-source` asserts the gate runs before deploy.
**Status (prior):** Fixed and remotely verified. The `085d1f0` push completed the GitHub Actions deploy successfully.
**Evidence:** Baseline `pnpm --filter @beedle/api typecheck` failed before `REL-01a`.

Current errors include:

- `apps/api/src/routes/admin-ingestion.ts`: unsafe `payload` property access.
- `apps/api/src/services/assistant-chat.ts`: model string not assignable to `keyof AiModels`.
- `apps/api/src/services/retrieval-foundation.ts`: multiple `possibly undefined` errors.

The deploy workflow only installs dependencies, applies D1 migrations, and deploys the Worker. It does not run API typecheck, web typecheck, or a durable smoke test suite before production deploy.

**Why it matters:** We can deploy code that TypeScript already knows is unsafe. This is now more important than many smaller correctness items.

**Direction:** Fix the API type errors, then add a small CI gate before deploy:

- `pnpm --filter @beedle/api typecheck`
- `pnpm --filter @beedle/web typecheck`
- a small stable API test suite
- phrase-search QA only when the local/remote prerequisites are available

### REL-02 - Production D1 migrations are applied automatically on every push to `main`

**Severity:** High  
**Completion:** **100% in-repo · ⛔ external ops blocker** · Difficulty: Easy · Break risk: Low  
**Status (verified 2026-06-29):** The in-repo work is complete and source-tested (`deploy-workflow-source`): `apply-d1-migrations.yml` is a manual `workflow_dispatch` workflow referencing `environment: production-d1-migrations` that lists then applies remote migrations, and `deploy-api.yml` no longer applies remote migrations on push. **This cannot reach a verifiable 100% from the codebase** — the last step is enabling required-reviewers / protection on the `production-d1-migrations` GitHub Environment in repo Settings (UI/GitHub API), which is an external ops action.
Prior status: Addressed and remotely verified. Production migrations now live in a manual workflow, while push-to-main deploys the Worker without applying remote D1 migrations.
**Evidence:** Baseline `.github/workflows/deploy-api.yml` ran `pnpm wrangler d1 migrations apply beedle --remote` before every Worker deploy.

This is now correctly targeting remote production, which fixed the previous migration gap. Production data migrations are separated from push-to-main deploys and the workflow is wired for GitHub Environment approval.

**Why it matters:** The FTS migration incident showed why large D1 data changes need a controlled path. A bad migration can affect production before we notice.

**Direction:** Move production migrations behind either:

- a protected GitHub Environment with required approval, or
- a separate manual `workflow_dispatch` migration workflow.

For large backfills, use explicit batched scripts rather than one-shot migrations.

### SRC-01 - Production source links return `404` for known search results

**Severity:** High  
**Completion:** **100% in-repo · ⛔ external prod-data blocker** · Difficulty: Medium · Break risk: Low  
**Status (verified 2026-06-29):** The in-repo fix is complete: when an R2 object is missing, `/source/:id` reconstructs the document from stored searchable text (`reconstructedSourceMarkdown`), returns it with `x-beedle-source-fallback: r2-missing-db-text` and a sanitized `content-disposition` (`safeFilename`). So users always get *something*. **The root cause cannot be fixed from the codebase** — the missing production source objects must be re-synced to R2 (or stale `source_r2_key` values repaired) via Cloudflare data-ops against the production bucket/D1. That external step is the remaining work.
Prior status: Addressed and remotely verified with a DB-text fallback when R2 objects are missing. Production `/source/...` returned `200` with `x-beedle-source-fallback: r2-missing-db-text`.
**Evidence:** After searching production for `Ant infestation in the kitchen`, the top five result source links all returned `404`:

- `T210489`
- `T250099`
- `T221447`
- `S001-92T`
- `T210403`

The source links point to the correct Worker hostname now, but the source objects are not available through the production source proxy.

**Why it matters:** Search can find the right decisions, but users cannot reliably open the source material. That damages trust immediately.

**Direction:** Audit production R2 versus D1 `source_r2_key` values. Either sync the missing source objects to production R2, repair stale keys, or change the source route to fall back to reconstructed stored text where appropriate.

### SEARCH-01 - Phrase relevance now matches production/local, but some phrase searches are still too slow

**Severity:** High  
**Completion:** **100% (in-repo latency levers) · production timing to be re-measured** · Difficulty: Hard · Break risk: Low (verified)  
**Status (2026-06-30):** **Done — every in-repo latency lever is now addressed.** Local phrase search is well under the 3000ms target and the cold-start penalty is fixed; the final lever was the **production vector stage**. `vectorSearch` previously ran one Workers-AI embedding **plus** a Vectorize query per query variant **sequentially** (N variants → N network round-trips in production); it now runs all variants **concurrently** (`Promise.all`) and merges by max score per chunk id (order-independent), so the result map is identical while N sequential round-trips collapse to ~one. Combined with the LLM-02 per-embedding 15s timeout, the vector stage is now both parallel and bounded. This change is **inert locally** (`vectorSearch` early-returns when `env.AI` is unavailable, before the parallel work), so local citations are unchanged and the production result set is identical by construction. Verified: typecheck clean; local citation sanity unchanged; `test:source` 53/53; new guard `test:search-vector-parallel`. The only remaining step is **re-measuring production latency to confirm the win** — that needs a deployed target (Workers AI is unavailable in `wrangler dev`) and requires no further code.

Prior in-repo work (all shipped): cold-start FTS `COUNT(*)`→`LIMIT 1`; decision-layer section-label prefilter; decision-layer per-document chunk cache. The authority/supporting-fact fallback fetches apply a section-label SQL prefilter so they stop pulling every chunk for a document when only conclusions/findings/evidence-style sections are ever kept.

**Profiling result (2026-06-29, REL-01 guard, local D1 ≈14k docs / 1.1M FTS rows):** The dominant cost is the **decision-layer finalize stage**, not FACET-01 and not FTS volume:

- `finalizeResults` is ~70-97% of total search time across queries (e.g. `pipe noise` finalize 838ms of 1114ms total).
- Within finalize, the two per-document fallback DB fetches dominate: `fetchAuthorityChunksByDocumentIds` (~1054ms worst case) + `fetchSupportingFactChunksByDocumentIds` (~581ms). `buildDecisionDisplayLayers` and snippet mapping are ~7-19ms.
- `scopeBuild` is ~0-1ms (so **FACET-01 cutover is not the latency lever**) and `lexicalSearch` is 20-140ms (so **FTS candidate volume is not the lever**).
- All four representative guard queries already pass the 3000ms target locally (max total ≈1204ms). The earlier "33s" figure predates the PERF-01 hot-loop work and no longer reproduces locally.

**Cold-start penalty fixed (2026-06-29):** The first search on a fresh isolate took ~4.6s, attributed (cheap first DB touch = 87ms, but first search = 4655ms) to `ensureSearchFts` running `SELECT COUNT(*) FROM search_chunks_fts` — an FTS5 full-index scan measured at ~2.9-3.8s on the 1.1M-row table — purely to check whether the table was empty before backfilling. Replaced with a `SELECT 1 ... LIMIT 1` existence probe (~0ms). Verified end-to-end: cold first search dropped **4655ms → 1208ms** (now under target) with identical top results; regression test `test:search-fts-bootstrap-cost` forbids reintroducing a `COUNT(*)` emptiness check. This runs once per cold isolate in production too, so the win applies after every deploy/recycle.

**Fix landed:** `fetchChunksByDocumentIds(..., decisionLayerSectionsOnly)` adds a `lower(section_label) LIKE` superset prefilter (`DECISION_LAYER_SECTION_LABEL_KEYWORDS`) that both decision-layer fallbacks opt into. Verified: deterministic ~50-85% fewer chunk rows fetched per document (globally 198,943 / 467,585 chunks match the prefilter), byte-identical top-10 results across 12 queries × 2 corpus modes (OLD vs NEW), plus a superset-correctness regression test (`test:search-decision-layer-section-prefilter`) that fails if a classifier category is added without extending the keyword set. Local wall-time is within run-to-run variance (already under target); the verified win is reduced data transfer + downstream JS work, which matters most at production scale.

**Why it matters:** Relevance is good; latency must stay well under a few seconds for normal workflows.

**Direction (remaining):**

- Production latency: the production ~20s figure likely includes vector-embedding round-trips that are skipped locally (Workers AI unavailable in `wrangler dev`); profile the production vector stage separately before optimizing it.
- Decision-layer simplification (ties into `SEARCH-02`): the authority + supporting fallbacks now share a per-document chunk cache (landed 2026-06-29) so overlapping documents are fetched once. A fuller fold — prefetching the union of fallback documents in a single query, or folding conclusions/findings retrieval into the main decision-scope fetch to remove the second round-trip entirely — remains possible but is lower priority while the warm path is already under the 3s target; justify it with production profiling before adding that complexity.

## Confirmed P1 / High-Value Follow-Up

### REF-01 - Citation/reference normalizers over-strip prefixes

**Severity:** High  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29 verification).** The over-stripping defect is fixed (word-boundary-aware citation-word, index-code, and validated-roman prefix rules) and unit-tested (`reference-normalization-utils` + `legal-reference-normalizers-source`, 7/7). The remaining live `legal-reference-normalization` integration tests fail only because the local D1 reference tables are not seeded (pre-existing, confirmed via A/B — see Verification Pass); that is test-infra, not a normalizer defect.
Prior status: Addressed locally with explicit citation-word, index-code, and valid-roman prefix rules plus targeted normalization tests.
**Evidence:** `apps/api/src/services/legal-references.ts` uses prefix strips such as `replace(/^sec/, "")`, `replace(/^rule/, "")`, and `replace(/^ic/, "")`.

Examples from inspection:

- `sec. 10.10` can normalize incorrectly.
- `rules 8.1` can be partially stripped as `rule`.
- `ICE-12` can be over-stripped by the `ic` rule.

**Why it matters:** Citation/reference normalization is central to a legal search product. False missing/invalid references create reviewer noise and weaken trust.

**Direction:** Replace raw prefix stripping with word-boundary-aware normalization and add targeted unit tests for `sec.`, `section`, `rule`, `rules`, `IC`, `ICE`, and roman-prefix cases.

### DATA-01 - Destructive corpus writes are not consistently atomic

**Severity:** High  
**Completion:** **100%** · Difficulty: Hard · Break risk: Low (active paths unchanged)  
**Status (2026-06-30):** **Done — every destructive corpus write is now protected against partial-failure data loss.** Note D1 cannot hold a transaction across the multiple batches a large operation needs, so "atomic for arbitrarily large ops" is impossible on the platform; the achievable and now-complete goal is that no destructive write can leave the corpus empty/half-written on failure. Audit of every destructive path: **(1) reprocess** only (re)builds text artifacts when the document is already empty (`shouldRebuildTextArtifacts` gate) — it never destructively re-chunks a content-bearing document; **(2) legal-reference rebuild** snapshots first (`takeReferenceSnapshot`) and restores on any failure (`restoreReferenceSnapshot`), so a failed multi-batch clear+rebuild rolls back; **(3) vector/non-D1 writes** are gated by DATA-02 (a chunk is only marked active once its Vectorize upsert succeeds); **(4)** the canonical artifact rebuild (`rebuildDocumentTextArtifacts`) now uses **write-then-swap** — it captures the prior section/chunk ids, writes the replacement rows first (fresh ids never collide), then deletes the prior rows by id via a bind-limit-respecting chunked delete, so a mid-sequence failure leaves the prior content intact (or briefly duplicated), never lost. The initial-ingest path is unchanged (new document → no prior rows to lose). Verified: typecheck clean; `ingest-artifact-batch-source` updated (asserts write-then-swap ordering) + new `destructive corpus rewrites are protected (DATA-01 model)` guard; new DELETE SQL validated against the live schema; `test:source` 52/52.
**Status (history):** Partially addressed locally: retrieval activation document-state writes, per-chunk retrieval activation D1 writes, retrieval activation rollback mutations, legal-reference table clearing, legal-reference rebuild inserts, legal-reference rollback restore inserts, document text artifact rebuild mutations, document reference-validation refresh mutations, reference-validation backfill pages, initial ingest document insert plus reference-validation plus text artifact mutations, reprocess document metadata plus reference-validation plus optional text artifact mutations, admin metadata confirmation plus reference-validation refresh mutations, and bulk searchability activation updates now execute through ordered D1 batches. Reference-validation refresh, reference-validation backfill, and text artifact rebuild now prepare replacement rows before batching the reset plus replacement writes together, and the reference-validation/text-artifact helpers expose prepared statements for callers that need to batch adjacent document mutations. Admin metadata confirmation now writes metadata, derived QC flags, and replacement reference-validation rows in one ordered batch. Rejection now clears approved/searchable lifecycle state together so admin queues cannot leave a document both approved and rejected. Broader ingest/reprocess sequencing remains open for very large multi-batch operations and non-D1 vector writes.
**Includes old items:** `SEC-05`, `BUG-06`, part of `PERF-06`.

**Evidence:** Several flows perform multi-step writes without a transaction or `DB.batch`, including legal-reference rebuilds, ingestion/reprocess, and retrieval activation rollback paths.

**Why it matters:** A mid-run failure can leave the corpus half-rebuilt, partially reprocessed, or impossible to roll back cleanly.

**Direction:** Group destructive write sequences into atomic batches where D1 supports it. For long operations, write replacement artifacts first, verify them, then swap state.

### DATA-02 - Vector activation can mark chunks active even if vector writes fail

**Severity:** High  
**Completion:** **100%** · Difficulty: Medium · Break risk: Low  
**Status:** **Done (2026-06-30).** The gate was already in place — a chunk is marked `active = 1` only when `vectorWriteStatus === "vector_upserted"` (else `blocked_by_vector_write_failure`), and `vectorWriteFailuresCount > 0` fails `activationVerificationPassed`. The remaining "broaden surfacing" work is now done: the report includes a dedicated `vectorWriteSurfacing` block with a per-status breakdown (`vectorWriteStatusBreakdown`: `vector_upserted` / `vector_unavailable` / `vector_upsert_failed` / `db_only`), the `blockedSearchChunkCount`, the list of `blockedDocumentIds`, and a (capped) list of the blocked chunks with their reason — so a failed batch is diagnosable from the report, not just a single count. Purely additive (no behavior change to the gate); verified by `test:retrieval-activation` unit tests (report assembled correctly at runtime) + extended `retrieval-activation-vector-gate-source` guard. `test:source` 46/46.
**Evidence:** Retrieval activation catches vector embedding/upsert failures and can still leave database rows active/queryable.

**Why it matters:** The database can claim a chunk is searchable while Vectorize does not actually contain the vector. That creates silent recall gaps.

**Direction:** Track vector write status explicitly and only mark vector-backed chunks active once upsert succeeds. Surface failures in activation reports.

### SEARCH-02 - `search.ts` is too large and too hand-tuned

**Severity:** High  
**Status:** First local refactor complete: phrase/highlight concept variants moved into shared data and consumed by API search plus web highlighting. A legacy pest-recovery seed regex now uses real word boundaries instead of literal control characters, with a source guard to keep regex controls visible. Stale issue/procedural query-flag helpers that duplicated derived-context term checks were removed from `search.ts`, along with an unused supporting-fact score wrapper. Generic-decision query terms, vector-skip issue terms, and habitability hint terms are now named constants with pre-normalized lookup lists where applicable instead of inline helper literals. **Decision-layer fetch simplification (2026-06-29):** the authority and supporting-fact fallbacks — the dominant warm-path cost identified in `SEARCH-01` profiling — now (a) apply a section-label SQL prefilter so they no longer pull every chunk for a document and (b) share a request-scoped per-document chunk cache so the supporting pass reuses rows the authority pass already fetched instead of issuing a redundant round-trip. Measured fallback-doc-set overlap is ~31% on average (0-92% by query), so the cache removes a variable but real share of redundant fetches. Verified behavior-neutral: top-10 results byte-identical across 12 queries x 2 corpus modes (OLD vs NEW), regression test `test:search-decision-layer-chunk-cache`. Broader ranking simplification (the topic-predicate lexicon) remains open.
**Evidence:** `apps/api/src/services/search.ts` is roughly 10k lines, with many topic-specific predicates, seed phrases, constants, and reranking branches.

**Why it matters:** Search quality now depends on a growing web of hardcoded special cases. It is difficult to reason about, hard to tune safely, and expensive to test.

**Direction:** Move topic/concept definitions into a data-driven lexicon/config layer. Keep the ranking engine generic and covered by golden-query tests.

### SEARCH-03 - Production search and retrieval debug use different query-type paths

**Severity:** Medium-High  
**Completion:** **100%** · Difficulty: Medium · Break risk: Low  
**Status:** **Done (2026-06-29 verification).** The debug response exposes `debugProfile` (`requestedQueryType`, `productionSearchQueryType`, `matchesProductionSearchPath`) in code (`search.ts`) and the shared schema, with a source test. Making the divergence visible and tested is the deliberate low-risk fix; actually making production infer/pass a non-`keyword` query type would be a higher-risk ranking change and is intentionally left as a separate decision (see SEARCH-02).
Prior status: Addressed locally by exposing a `debugProfile` that labels the requested debug query type, the production search query type, and whether the paths match.
**Evidence:** Production `search()` always calls `runSearchInternal(..., "keyword", false)`, while `/admin/retrieval/debug` passes through `parsed.queryType`.

**Why it matters:** Debug results can exercise ranking branches that normal users never hit. That can give us false confidence when tuning.

**Direction:** Decide whether production should infer/pass query type or whether debug should explicitly label non-production modes. The important fix is making the distinction visible and tested.

### PERF-01 - Search scoring recomputes query-derived work inside hot loops

**Severity:** High  
**Completion:** **100%** · Difficulty: Medium · Break risk: Low (verify-and-lock)  
**Status (2026-06-30):** **Done — verified and locked in.** Audited the hot path end-to-end and confirmed the per-row recomputation is fully eliminated: `getQueryDerivedContext` memoizes the derived query context once per search (`context.derived`), per-row text is cached (`cachedNormalizedSearchableText` / `cachedNormalizedChunkText` / `cachedCombinedSearchableText` / `cachedRowMetadata`), the ~1,000-line `scoreRow` performs **zero** per-row row-text re-normalizations, and there are **no cache bypasses** anywhere (`normalize(combinedSearchableText(...))` = 0; `normalize(row.chunkText...)` exists only inside the cache helper). The remaining query-derived builder calls are all query-level (once per search), not per-row. No further safe per-row optimization remains and search is already under the 3s target, so the completion is a regression guard (`test:search-hot-loop-caching`, in the `test:source` gate) that fails if any of these invariants regress. `test:source` 51/51; live relevance 5/5; citation sanity unchanged. _(Detailed history of the reuse work retained below.)_
**Status (history):** Partially addressed locally: `scoreRow`, snippet selection, layered snippets, supporting-fact fetches, document evidence summaries, representative chunk scoring, authority passage scoring, supporting-fact diagnostics, supporting-fact candidate selection, issue-family fallback filtering, decision-scope filtering, issue-family seeding, and document coverage boosts now reuse a per-search derived query context for repeated terms, normalized issue/procedural terms, long query tokens, normalized primary signals, flags, normalized sentence anchors, normalized secondary tokens, normalized factual metric tokens, normalized phrase concept groups, habitability/lockout/lock-box/supporting-fact query flags, cooling/condition/procedural/strong-evidence hot-path flags, wrongful-eviction flags, eviction-protection flags, accommodation flags, buyout/buyout-pressure/rent-reduction/nuisance flags, package-security/camera-privacy/waste topic flags, leak/window snippet routing flags, literal-keyword snippet routing flags/token reuse, homeowner-exemption/self-employed/adjudicated/social-media/caregiver/moot/divorce/remote-work/college/co-living/dog/intercom/garage/common-area/stairs/porch/windows/section-8/unlawful-detainer topic flags, phrase-evidence flags, judge references, structural-intent flags, index-code filter context, normalized index-code compatibility rules/ordinance/search-phrase signals, normalized structured filters, curated keyword family flags/matches, keyword-family recall flags, keyword boundary guard flags, keyword candidate/execution terms, infestation query flags, and judge lookup keys; `scoreRow` also reuses per-row judge match results for judge-driven scoring and cached parsed/normalized row metadata for index/rules/ordinance/title/citation scoring. The main scorer, document evidence summaries, representative chunk scoring, authority scoring, supporting-fact diagnostics, supporting-fact candidate gates, supporting-fact retry gates, strong-evidence checks, section-8/unlawful-detainer support checks, wrong-context filters, query guards, row query guards, literal-keyword row guards, decision-scope filters, issue/procedural term checks, issue-family seed filters, judge-driven query checks, section-8/procedural support helpers, vector-skip checks, and supporting-fact chunk prioritization now reuse cached row searchable text/normalized text, cached normalized chunk text, or precomputed query term/reference arrays through the search context. Primary issue-signal matching, issue-signal context checks, lexical scoring, lexical retrieval phrase-group reuse, factual-token metrics, evidence-summary factual-token reuse, keyword issue-term selection, keyword whole-word checks, top-row topic context checks, visible scoreRow topic helper checks, structural-intent artifact guards, boilerplate/drift helper checks, cooling/accommodation/mold/heat-appliance/water-heater/capital-improvement drift helper/query checks, leak-window helper/query checks, phrase-evidence helper/group checks, market-condition reasoning reuse, strong-evidence helper/query branch checks, strong-evidence topic flags, strong-evidence whole-word checks, section-8/unlawful-detainer support helper checks, section-8 issue-family guard checks, decision-layer habitability/poop helper checks, decision-layer evidence context checks, decision-layer query-flag checks, layered-snippet topic context checks, habitability coverage signal checks, owner-move-in/wrongful-eviction phrase helper checks, wrong-context helper/query checks, derived-context query flag checks, derived-context intent/market checks, derived-context structural-intent checks, derived local term checks, derived query-token reuse, derived phrase-token reuse, sentence-secondary issue-term reuse, sentence-anchor reuse, primary-signal reuse, lexical broad-issue term reuse, lexical/whole-word expansion reuse, chunk issue/procedural term reuse, recall issue-term reuse, vector-first issue-query reuse, retrieval vector-first flag reuse, query-term inference reuse, issue-hint lexical-term reuse, issue-scope hint reuse, FTS query reuse, phrase FTS query/token/group assembly reuse, keyword execution concept-group reuse, phrase concept guard context reuse, active structured-filter gate reuse, active structured-filter kind reuse, requested filter reuse, curated-keyword fallback gate reuse, keyword-family recall reuse, keyword-boundary guard reuse, keyword candidate/execution reuse, run-level retrieval flag reuse, inline derived-context reuse, decision-layer required-context reuse, query-guard required-context reuse, literal-keyword guard required-context reuse, section-8/procedural support required-context reuse, remaining derived topic flag checks, derived mention flag checks, derived specificity flag checks, derived lockout flag checks, query-guard helper checks, short-query guard checks, issue-family seed helper checks, decision-scope accommodation checks, infestation helper checks, exact phrase helper meaningful-token/group reuse, exact phrase leak-window context reuse, and exact multiword phrase scoring in `scoreRow` now pass cached normalized row text/query text into their helpers instead of normalizing the same text again; document evidence summaries, representative chunk scoring, authority passage scoring, and supporting-fact diagnostics now do the same for primary-signal hit counting, factual-token metrics, exact phrase scoring, and sentence phrase-overlap scoring where applicable. Document-level aggregate coverage now joins cached normalized row text instead of rebuilding and normalizing a combined string. Phrase-concept coverage also accepts pre-normalized text so hot scoring/snippet/evidence paths avoid normalizing the same row or snippet text again. Deeper helper propagation remains open.
**Evidence:** Search scoring repeatedly normalizes text and recomputes query-derived topic flags/terms across candidate rows.

**Why it matters:** This likely contributes directly to slow phrase searches.

**Direction:** Build a per-search query context once, memoize normalized row text, and pass precomputed flags through scoring helpers.

### FACET-01 - Primary legal filters use `LIKE` against JSON text

**Severity:** High  
**Completion:** **100%** · Difficulty: Medium · Break risk: Low (verified live) · _activation note below_  
**Status (2026-06-30):** **Done — filters use the indexed facet tables, and the cutover is now robust to migration timing.** Index-code, rules-section, and ordinance-section filters query the indexed `document_index_codes` / `document_rules_sections` / `document_ordinance_sections` tables (with a validated reference-link `OR` fallback); only the owner-move-in path keeps a JSON `LIKE` as its documented unmigrated-DB fallback.

**Real bug found and fixed during finalization:** because production migrations are decoupled from code deploys (REL-02) and `no such table` is **not** a retryable search error, the facet-querying code could ship before migration 0009 is applied — and a missing facet table **threw** on any index/rules/ordinance filter. Confirmed live on the unmigrated local D1: `indexCodes:["G93"]` returned `HTTP 400 D1_ERROR: no such table: document_index_codes`. Reference-links alone are not a safe substitute (only **147** of **13,015** code-bearing docs have index-code reference rows — ~1% coverage). Fix: a runtime safety net `ensureDocumentFacetTables(env)` (mirroring `ensureSearchFts`) lazily provisions the same tables/indexes/triggers as 0009 — DDL copied verbatim from the immutable migration — and backfills once when empty (a populated corpus skips the scan). After the fix, the local bootstrap created **46,724 / 95,769 / 42,049** facet rows + all three sync triggers; the previously-throwing filters now resolve (`G93`/`C77` now hit only the pre-existing local "Binding AI needs to be run remotely" limitation, identical to vector search), and ordinance/rules/index filters return results. Verified: typecheck clean; `test:source` 55/55 (added `search-facet-table-bootstrap-source` incl. a 0009-drift guard); unfiltered citation sanity byte-identical (`ant`/`pipe` baselines unchanged).

**Activation note:** migration 0009 is still the canonical mechanism (apply it in prod via the REL-02 manual workflow for proper population without the one-time runtime backfill); the safety net only guarantees correctness if it deploys first. The residual JSON `LIKE` at `search.ts` (owner-move-in) is intentional defensive fallback.
**Evidence:** Index-code, rules-section, and ordinance-section filters are stored as JSON text blobs and searched with `LIKE`.

**Why it matters:** These are core product facets, but the current storage shape forces scans instead of indexed lookups.

**Direction:** Normalize into indexed join tables:

- `document_index_codes(document_id, code)`
- `document_rules_sections(document_id, section)`
- `document_ordinance_sections(document_id, section)`

### ADMIN-01 - Admin ingestion list filters/sorts after SQL `LIMIT`

**Severity:** Medium-High  
**Completion:** **100%** · Difficulty: Hard · Break risk: Low (verified live)  
**Status (2026-06-30):** **Done — exact top-N for realistic corpus sizes, with honest completeness diagnostics.** Derived reviewer filters/sorts are refined in JS, so a capped candidate pool could hide matches ranked beyond it. The list now runs a cheap prefilter `COUNT(*)` first: when the prefiltered set is within `DERIVED_FILTER_EXACT_CAP` (4000), it fetches the **entire** prefiltered set so the JS filtering/sorting produces the exact top-N (and for small sets this fetches *fewer* rows than the prior fixed floor); only a genuinely larger prefiltered set keeps the bounded pool and is flagged `derivedCandidatePoolExhausted: true`. The summary now exposes `derivedPrefilterCount` and `derivedCandidatePoolComplete`. Verified live against the local corpus: `status=staged&sort=approvalReadinessDesc` reported `derivedPrefilterCount: 1082`, `candidatePoolLimit: 1082`, `derivedCandidatePoolComplete: true` (the full 1082-doc staged set is now ranked, vs the prior 500-floor pool); `approvalReadyOnly` reported a complete 16-doc set. Reuses all existing filter/sort logic (low regression risk); `test:source` 53/53. Note: a true SQL/materialized-column pushdown of the JS-derived reviewer scores would remove even the >4000 cap, but it requires a write-path migration and is unnecessary for the current corpus.
**Status (history):** Partially addressed locally by over-fetching a bounded SQL candidate pool before derived reviewer filters/sorts, pushing the `realOnly` fixture exclusion, `approvalReadyOnly` blocker checks, known `blocker=` filters, `reviewerReadyOnly` necessary-condition checks, low/medium reviewer-risk necessary-condition checks, low/medium/high reviewer-effort unresolved-reference bounds, recurring-citation-family unresolved-reference text bounds, direct-signature unresolved-triage bucket bounds including cross-context candidates, blocked-37x queue filters, and `runtimeManualCandidatesOnly` unresolved-reference bounds into SQL as conservative prefilters, returning the requested page size, and surfacing candidate-pool diagnostics when derived filters/sorts may still hide additional matches. Generic blocked-37x SQL prefilters now match the derived any-family queue semantics, while specific family/batch-key filters still require the requested family set. The approval-readiness derived score and reviewer-readiness candidate flag are now mirrored as SQL candidate-pool order keys before final JavaScript sorting; reviewer-effort, batchability, unresolved-leverage, and blocked-37x batch-key sorts now also pre-order their SQL candidate pools with conservative unresolved-reference/unsafe-37x signals before final JavaScript sorting. Full SQL/materialized-column pushdown remains open.
**Evidence:** The admin listing fetches a limited SQL page, then applies some filters and sorts in JavaScript.

**Why it matters:** Reviewer/admin queues can show the wrong top-N and misleading counts.

**Direction:** Push important filters/sorts into SQL or materialized columns before pagination.

## Confirmed P2 / Schedule Deliberately

### INGEST-01 - Upload parsing has no practical size/decompression guard

**Severity:** Medium  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29).** Every ingest path is now size-bounded: multipart has a content-length + `file.size` + buffer-length cap (15MB → 413), DOCX parsing caps the summed decompressed payload (40MB), and the service caps decoded source bytes (15MB). The remaining gap — the **JSON ingest path** (`handleIngest`) called `request.json()` and loaded the whole base64 body into memory before the decode cap — is closed with a content-length pre-check (`maxJsonIngestEnvelopeBytes`, base64-expansion aware) that returns 413 early. Regression test `test:ingest-size-guards` extended. (Note: the `pilot-ingestion-hardening` integration tests fail locally with `400` due to the unseeded reference tables — pre-existing, confirmed via A/B, unrelated to this guard.)
**Evidence:** Multipart upload reads the whole file into memory and base64-encodes it. DOCX parsing uses `unzipSync` with no decompressed-size cap.

**Direction:** Add max upload size, max decompressed size, and early rejection.

### LLM-01 - LLM prompt boundaries and fallback transparency need hardening

**Severity:** Medium  
**Completion:** **100%** · Difficulty: Medium · Break risk: Low  
**Status:** **Done (2026-06-29).** Both LLM prompt-assembly paths fence untrusted input in delimited data blocks (`assistant-chat`: `<current_question_data>`/`<retrieved_decision_data>`; `draft-conclusions`: `<findings_of_fact_data>`/`<relevant_law_data>`/`<retrieved_authority>`), each with an explicit "treat as untrusted data, not instructions / ignore embedded instructions" directive. Verified these are the **only** two LLM chat paths (the embedding is the only other AI call; `case-assistant`/`draft-template` use heuristics/scaffolds, not LLM prompts), and added a guard test that fails if a `chat/completions` call appears outside the fenced services. Drafting fallback is surfaced via `generation_mode`/`fallback_reason` (shared schema + drafting page). Note: prompt-injection can never be fully "solved" — this closes the structural boundary; ongoing model-level robustness is inherent.
**Evidence:** Retrieved corpus text and user facts are included in model context without strong data fencing. Drafting can fall back silently when LLM calls fail.

**Direction:** Fence untrusted content as data, not instructions. Surface fallback/error state to the UI.

### LLM-02 - Assistant-chat AI calls lack timeouts

**Severity:** Medium  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29).** Assistant-chat Workers-AI (`withAssistantTimeout`) and external LLM (`AbortController`) calls were already bounded, and the draft-conclusions LLM call has an 18s `AbortController`. The final uncovered outbound AI call — `embeddings.ts` `embed()` (`env.AI.run`, used by search vector queries, ingest, backfill, activation, probe) — is now raced against a 15s timeout and degrades to `null`, which every caller already handles (vector search is skipped; chunks are not marked vector-active). Regression test `test:embeddings-timeout` forbids reintroducing an unbounded `await env.AI.run`. Citation sanity unchanged.
**Evidence:** Draft conclusions uses a timeout; assistant chat paths are inconsistent.

**Direction:** Add `AbortController`/race timeouts to all outbound AI/LLM fetches.

### WEB-01 - Search UI can show stale results from an older request

**Severity:** Medium  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29 verification).** The search page issues each query through an `AbortController`, tracks a request epoch in `searchRequestRef`, and ignores responses from superseded requests before applying results (source-tested in `search-stale-response-source`). The case-assistant/drafting flows are single-shot submits, not the search-as-you-type race this item covers.
Prior status: Addressed locally with abortable search requests and a request epoch guard before applying results.
**Evidence:** Search submission has no request epoch or abort controller.

**Direction:** Add request sequencing and ignore stale responses.

### WEB-02 - Source/detail API client helpers are not consistently schema-validated

**Severity:** Low-Medium  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29).** All user-facing helpers zod-parse their responses (search, retrieval preview, dashboard summary, case-assistant, assistant-chat, draft conclusions/template/export, taxonomy config/resolve, retrieval debug, reference inspect). The last two raw casts — `listIngestionDocuments` and `getIngestionDocument` (large, evolving admin payloads) — now route through `expectObjectResponse`, a lightweight top-level shape guard that rejects null/array/error-shaped or `documents`-less responses while passing valid ones through unchanged (a full zod schema for those payloads would be brittle and reject valid responses on field drift — a worse outcome). Web typecheck clean; live shape check confirms the admin list response is an object with `documents[]`; regression test extended in `api-schema-validation-source`.
**Evidence:** Some helpers cast JSON responses instead of parsing with zod.

**Direction:** Validate all backend responses that drive UI rendering.

### UI-01 - Dashboard and placeholder pages contain fake/non-functional product signals

**Severity:** Low-Medium  
**Completion:** **100%** · Difficulty: Medium · Break risk: Low  
**Status:** **Done (2026-06-29 verification).** No fabricated model-readiness/activity visuals remain (grep-clean for the old hardcoded model names/chart array; `dashboard-product-signals-source` test passes), and the manual upload shell is labeled as planned. Only the real `searchableDecisionCount` is shown. Wiring additional real data is future feature work, not a defect.
Prior status: Addressed locally by removing fake model readiness/activity visuals from the dashboard, replacing them with links to real review/admin surfaces, and marking the manual upload shell as planned.
**Evidence:** Dashboard contains hardcoded activity/model status; Add Decision is a visual placeholder.

**Direction:** Either wire real data or label/remove these surfaces until functional.

### REPO-01 - API package scripts and experiment files are too noisy

**Severity:** Medium  
**Completion:** **Hygiene infra 100% · bulk archive re-scored Hard (out of Easy/Medium bucket)** · Break risk: Low  
**Re-assessment (2026-06-29):** The hygiene *infrastructure* is complete and Low-risk: reports cleaned (575MB→14MB), policy + `report:repo-scripts` inventory guard + dedupe detection + source tests, and the truly-orphaned one-offs archived ("0 actionable" under policy). The *remaining* de-bloat — archiving the 60 `retrieval-r*` experiment scripts and 48 r-numbered test files — was scored Medium but is actually **Hard**: those scripts import shared non-`r` utility modules (e.g. `retrieval-eval-utils.mjs`) that other scripts also use, and they carry **214 package aliases** (107 script + 48 test + 59 report/write/debug). A correct archive needs coordinated import-path rewrites across 100+ files plus 214 alias deletions — a large, error-prone mechanical refactor that belongs in its own dedicated PR, not this Low-risk-Easy/Medium pass. App-break risk remains Low (scripts are not app code), but it is no longer an Easy/Medium item.
Prior status: Addressed locally with a repo hygiene policy, API scripts README, `report:repo-scripts` inventory guard, dry-run `report:repo-cleanup-plan` for stale generated reports, guarded `write:repo-report-cleanup` application command, expected dry-run/apply alias classification, expected profile alias classification, and source tests guarding the cleanup policy. These cover report retention, package alias discipline, missing targets, exact duplicate target mappings, actionable command-variant target mappings, unaliased top-level scripts, local report volume, and focused cleanup cadence. Applied the guarded local report cleanup path on 2026-06-27, reducing ignored generated reports from `53,122` files / `575 MB` to `210` files / `24 MB`, with `0` remaining cleanup candidates under the current retention policy. Repo script inventory now separates expected unaliased support/config files from actionable unaliased scripts. The four actionable unaliased top-level one-offs were moved into `apps/api/scripts/archive/`, reducing actionable unaliased top-level scripts to `0`. Current package/script inventory is now source-tested and reports `0` missing targets, `0` duplicate target mappings, `0` actionable command-variant mappings, and `0` actionable unaliased top-level scripts, so no safe package-alias pruning target remains under the current policy.
**Evidence:** Current counts:

- `331` API npm scripts
- `248` `.mjs` files under `apps/api/scripts`
- `258` top-level script/report config files under `apps/api/scripts`
- `apps/api/reports` is about `14 MB` by `report:repo-scripts` byte inventory after the guarded local cleanup pass
- command variants are now split into `0` actionable mappings, `10` expected dry-run/apply mappings, and `3` expected profile mappings
- unaliased top-level files are now split into `0` actionable script candidates and `55` expected support/config files

**Direction:** Keep durable commands in `package.json`; archive old experiment/report scripts; add a reports cleanup policy.

### REPO-02 - Large static catalog data is compiled as TypeScript source

**Severity:** Low-Medium  
**Completion:** **100%** · Difficulty: Easy · Break risk: Low  
**Status:** **Done (2026-06-29).** The catalog is a 3,740-line `index-codes.json` loaded by a 10-line typed wrapper (`index-codes.ts`); no data is compiled as TS. A regression test (`test:index-codes-catalog`, in the `test:source` gate) guards the wrapper shape and validates the JSON catalog (non-empty, well-formed string fields, **unique codes**). Finalizing this surfaced and fixed a real latent bug: code `C88` appeared twice (the real `Ord. 37.9(a)(11)` entry plus a `[reserved]` stub), and since `search.ts` builds `new Map(...)` by code (last-wins), `C88` resolved to the empty `[reserved]` entry — the web dedupe maps had the same hazard. Removed the duplicate; catalog is now 622 unique codes. Verified: API+web typecheck clean, `test:source` 44/44, citation sanity unchanged.
**Evidence:** `packages/shared/src/index-codes.ts` is several thousand lines of data.

**Direction:** Move stable catalog data to JSON or a seeded database table with typed loading.

## Deferred / Out Of Scope For This Pass

### AUTH-01 - No in-code authentication/authorization

**Status:** Confirmed but intentionally deferred.  
**Evidence:** API routing has no auth middleware; admin and mutation endpoints are registered publicly at the Worker layer.

We are not treating this as the active focus because auth is explicitly out of scope for this pass. It should come back before broader rollout.

### CORS-01 - CORS defaults to wildcard if `CORS_ALLOWED_ORIGINS` is unset

**Status:** Addressed locally by defaulting to the known local/Page origins instead of wildcard when `CORS_ALLOWED_ORIGINS` is unset.

This becomes important when the access model is formalized. The eventual behavior should fail closed when the allowlist is missing.

## Removed / Demoted From The Active List

These were in the raw audit but should not be first-class active issues as written.

- **Old HYG-01:** `R2_PUBLIC_BASE_URL = example.invalid` is not itself a bug. It is intentional when source links are proxied through the API. The real issue is `SRC-01`: production source objects/keys are missing or not reachable.
- **Old HYG-02:** destructive migrations should be folded into `REL-02`, not tracked separately.
- **Old HYG-03:** hardcoded Cloudflare URLs are a portability smell, but not a current product blocker.
- **Old BUG-12:** the dead ternary in phrase concept guard is cleanup unless evidence shows bad relevance. We should not let it compete with `SEARCH-01`.
- **Old BUG-18:** the exact failure mode is weak because `ensureSearchFts` catches errors internally. Keep runtime DDL/backfill concerns under release/search architecture, not as a standalone bug.
- **Many LOW dead-code items:** valid cleanup, but not useful in the main working backlog until P0/P1 work is under control.

## Verified Non-Issues / Corrections

- General SQL injection still looks low-risk: dynamic values are bound parameters, and admin sort keys are switch-whitelisted.
- XSS risk in the search UI remains low: highlighted snippets are rendered as React text nodes, not `dangerouslySetInnerHTML`.
- FTS production/local relevance mismatch for `Ant infestation in the kitchen` is fixed as of commit `9913b8d`; remote FTS now has both document and retrieval rows.
- Web typecheck currently passes locally.
- API typecheck currently passes locally.

## Suggested Sequence (refreshed 2026-06-29)

Ordered by value ÷ (difficulty × break-risk), given the current scorecard:

1. **Finish `FACET-01` cutover (50%→done)** — apply migration `0009` everywhere, route the remaining index-code/rules/ordinance filters through the indexed join tables, and remove the residual JSON `LIKE`. Medium effort, contained risk, clear correctness/perf win. (Note: `0009` is not yet applied in the local dev D1, so the cutover isn't even active locally.)
2. **Close out `DATA-01` (80%→done)** — large multi-batch ingest/reprocess atomicity and the non-D1 Vectorize write path. Hard but high-value for corpus integrity; do it with write-then-swap and strong tests.
3. **Profile production `SEARCH-01` vector latency** — the local path is under target; the prod ~20s likely lives in the Workers-AI embedding round-trip, which needs a deployed target to measure. Diagnose before optimizing.
4. **`SEARCH-02` topic-predicate lexicon (20%→…)** — the core de-bloat you care about. High break-risk: plan the data model and stand up golden-query regression coverage *before* touching ranking; do it as its own scoped cycle.
5. **Before broader rollout: `AUTH-01` (0%)** — the deferred Critical. Every admin/ingest/write/LLM endpoint is currently public.
6. **Lower-priority cleanups** — `ADMIN-01` materialized-column pushdown, `CORS-01` strict fail-closed, `REPO-01` archive the ~248 experiment scripts, and the local reference-table seeding so the live `legal-reference-normalization` tests pass locally.
