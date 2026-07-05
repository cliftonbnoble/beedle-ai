# Beedle AI — Issue Backlog & Resolution Log

**Single source of truth.** Consolidated 2026-07-04 — merges the product/audit backlog with the search-quality deep dive (formerly `NEW-ISSUES.md`).

This document is organized **open work first, history second**:

1. **[Open work](#1-open-work)** — everything genuinely left, grouped by who owns it.
2. **[Resolved log](#2-resolved-log)** — a compact record of what's been fixed (full prose lives in git history / commit messages).
3. **[Reference](#3-reference)** — verification baselines, the REL-02 / SRC-01 ops runbooks, verified non-issues, and demoted items.

**ID namespaces** are preserved as-is because commits reference them: `REL/REF/DATA/SEARCH/FACET/ADMIN/INGEST/LLM/WEB/UI/REPO/CORS/API/ARCH/PERF/CONF/CODE/TEST/CI` (product + 2026-07-02 audit) and `NS-01…NS-36` (search-quality deep dive). Auth is tracked (`AUTH-01`) but was explicitly out of scope for these passes.

**Current verification state:** golden net 27/27 byte-identical · judged eval 17/17 (mean P@5 **0.933** / MRR **1.000**) · `test:source` 74/74 · `test:utils` 38/38 · `test:web` 16/16 · `test:case-assistant` 4/4 · API + web typecheck clean (`noUnusedLocals`/`noUnusedParameters` enforced).

---

## 1. Open work

### 1A. Needs you — external ops or a product decision (no repo work left, or a call only you can make)

| ID | Sev | What's needed | Detail |
|---|---|---|---|
| **AUTH-01** | **Critical** | Gate every admin/ingest/write/LLM endpoint before rollout | All endpoints are public at the Worker layer. Needs Cloudflare Access / JWT / shared-token. Deferred by agreement; the single biggest production risk. |
| **REL-02** | High | Enable required-reviewers on the `production-d1-migrations` GitHub Environment | 100% in-repo. ~5-min Settings-UI step — [runbook in §3B](#3b-rel-02-runbook-github-ui-5-min). |
| **SRC-01** | High | Re-sync missing prod R2 source objects / repair stale `source_r2_key` | 100% in-repo (DB-text fallback ships now). Root fix is Cloudflare data-ops — [runbook in §3C](#3c-src-01-runbook-cloudflare-data-ops). |
| **FTS index rebuild** (NS-28/30/31 residual) | High | Add `title`/`author` columns to `search_chunks_fts` via migration | The **last slow class**: multi-term curated families ("mold", 40–80s) can't get scan-parity because their top matches are title/author-weighted and those columns aren't indexed. A rebuild lets NS-30's FTS routing cover them. Migrations are manual/decoupled — the code needs the runtime-safety-net pattern (like `ensureDocumentFacetTables`) so it's correct before the migration lands. **NS-32** (`documents.is_trusted` materialization + composite index) can ride the same migration to kill the correlated trust-tier `EXISTS` scan-tax. |
| **NS-34** | Med | Corpus data cleanup (two items) | (1) Retire duplicate remand doc `doc_3d98c3ec-d98…` for T150579 (identical twin, transposed title). (2) Re-extract citations for the **11 docs sharing bogus citation "316928"** (real citations are in their titles). *The golden "twins" that look like dupes are legitimate original+remand pairs — leave those.* |
| **NS-12** | Med | Decide include/exclude for 1,084 staged-invisible docs (7.7% of corpus) | `searchable_at IS NULL AND rejected_at IS NULL`. If real decisions are stuck in QC, no ranking fix can surface them. Reviewer-readiness tooling already exists; then bulk-activate. |
| **CONF-03** (compat-date half) | Low | Bump API `compatibility_date` 2025-02-15 → 2026-04-03 during a supervised deploy | Flips 14 months of runtime flags; do it with the CI-01 post-deploy smoke watching. (The `minify=true` half already shipped.) |
| Local test-DB seed | Low | Seed local D1 reference tables so 6 `legal-reference-normalization` **live** tests pass locally | Pre-existing, confirmed via A/B (not a regression). Run `pnpm normalize:references` / apply `0009` + re-seed, or document the setup. Unit coverage already passes. |

### 1B. Needs a deployed environment — one remote A/B session

`env.AI` is inert in `wrangler dev`, so the vector channel's *prod effect* can't be measured locally. Everything below is either shipped-and-locally-inert (proven byte-stable, needs a prod number) or un-startable until real cosines exist. **One deploy + a golden/eval run against the live target unblocks all of it.**

| ID | State | What the session confirms / unblocks |
|---|---|---|
| **NS-22** | Code shipped | bge query-instruction prefix — confirm the retrieval-quality lift vs citation baselines. |
| **NS-16** (vector term) | Code shipped | Fixed-affine cosine calibration (fusion-term-only) — its ranking effect ships with NS-22. |
| **NS-10 / NS-21** (caps) | Code shipped | topK 25→100, page-independent recall floor — confirm depth helps, doesn't add noise. |
| **NS-25** | Code half shipped | Synonym soup already out of the embedder; **remaining:** rewrite the ~20 curated keyword-bag variants as short NL reformulations (needs real embeds to judge). |
| **NS-21** (skip heuristic) | Open | Decide whether to downweight instead of skip vector when phrase-FTS "has enough" — a tuning judgment needing measurement. |
| **NS-23** | Open | Context-enriched re-embed (`"{title} — {sectionLabel}\n{chunkText}"`) + hard-split >1,600-char paragraphs. Requires a re-embedding backfill (machinery exists); best validated remote. |
| **NS-24** | Open | Exempt high-vector rows from lexical guards — needs the real cosine distribution to set the bar. |
| **NS-26** | Open | Vectorize metadata filtering (push documentId/fileType/active into the query `filter`; oversample topK when filters active) — needs a metadata backfill + real vector. |

### 1C. Deferred pending a judged driving case (code-side, no evidence yet)

Every judged eval entry is at ceiling, so changing these without a failing case is re-pin churn. Re-open each by **adding the eval query first**.

| ID | Sev | Why deferred |
|---|---|---|
| **NS-18** | Med-High | Doc-block ordering. Live probes show only defensible mild passage inversions (winner holds strongest lexical evidence + multiple chunks), not the weak-chunks-over-0.85 pathology as written. The citation-neighbor-guard sub-item is dead surface (`citation_lookup` unreachable from `/search`). |
| **NS-15** | High | Doc-constant field weights flood the recall caps. Real, but inherited from the scan's own weighting semantics by design; touches the same ordering as NS-18. |
| **NS-16** (field-aware half) | High | SQL recall-rank vs `scoreRow` field-agnostic disagreement. Larger, separately eval-gated; the high-leverage vector half already shipped. |
| **NS-17** (remaining) | High | Increment A landed (per-chunk phrase-concept eliminate → demote). Remaining: `literalKeywordQuery` all-tokens-in-one-chunk gate + topic literal gates — defensible precision without a counter-example. |
| **NS-14** | Low | Positional token retention in `meaningfulLexicalTokens`. Small; superseded in practice by the NS-04/NS-07 selectivity work. |
| **PERF-05** | Low | 93KB `index-codes.json` in client bundles — genuinely-used filter data; lazy-loading changes load timing on 6 pages for a modest gz saving. Risk/benefit doesn't clear the bar. |
| **ARCH-02** (redesign half) | Med | The two raw sentinel emitters now route through `effectiveSourceLink`; the deeper "stop persisting derived URLs" redesign + prod-row backfill is ops-coupled. |
| **PERF-04** (b/c) | Med | Ingest-parallelization core shipped; embed/Vectorize batching (failure-granularity change on the ingest quality path) and the multipart base64 pass-through (shared-schema contract change) deliberately left as risky. |

### 1D. Fresh-audit backlog — 2026-07-04 second deep audit (verified findings, ordered by value)

Method: 4 parallel code sweeps (orchestration seams, SQL/data layer, scoring/decision layer, query-analysis) + live probes on previously-unprofiled query classes (date filters, chunkType, deep offsets, trusted_only combos, judge-name-in-text). Every load-bearing claim below was **verified first-party** (code read + probe) before listing; claims that failed verification are recorded in §3D so they aren't re-litigated. Two findings were fixed on the spot (see §2A, commit `60e22e5`).

| ID | Sev · Cx · Risk | Finding + direction |
|---|---|---|
| **NS-37** | Med · Med · Low-Med | **Ultra-common-token parity fetch costs 10-12s.** 3+-token AND-empty queries where one token matches most of the corpus ("Katayama mold **decision**", "harassment under 37.9(a)(2)" via the harassment-family OR arm): the scan-parity FTS fetch computes the weighted-instr rank over every matched row (~500k for `decision*`) before LIMIT. The k=2 relaxed tier can't always rescue — the token-LENGTH selectivity proxy prefers ubiquitous-but-long words. **Direction:** rank relaxation groups by real document frequency via an `fts5vocab` table probe (one cheap lookup per token), or pre-LIMIT the parity recall per-term; results are correct today, this is latency-only. |
| **NS-38** | Low-Med · Med · Med | **`orderDecisionFirst` runs twice per request** (search.ts — once for `topDecisionIds`, once layer-aware) — the first pass computes full grouping/evidence summaries the second recomputes (~15-30ms est.); and `buildDecisionDisplayLayers`' authority/supporting pickers each re-scan the same candidate list (up to ~20 re-scorings per group). **Direction:** single-pass refactor or precomputed passage scores. Careful: this is the presentation contract (NS-18 territory) — behavior must stay byte-identical, golden-gated. |
| **NS-39** | Med · Med · Med | **Scope-candidate GROUP BY queries materialize every matching chunk before aggregating** (`fetchHabitabilityCandidateDocumentIds` / lockout / issue variants): inner subquery scans all LIKE-matching chunks corpus-wide, then GROUP BY + full sort before LIMIT. Hits the habitability/lockout/issue-guided classes (currently masked by their doc scopes staying small). **Direction:** top-K aggregation or a bounded inner LIMIT; measure first — these classes are currently fast because scopes bind early. |
| **NS-40** | Low · Low · Low | **Finalize-stage micro-batch** (bundled small wins, ~10-20ms/request combined): `chooseSnippetForTargets` re-normalizes/sorts the identical target list per result row; `orderDecisionFirst` normalizes layer/authority/support text up to 4× per group; `sentenceFactualTokenMetrics` recomputes token positions in 3-4 downstream callers; `habitabilityCoverageSignals` runs 11 sequential `.match()` chains per call. **Direction:** one pass, precompute-and-pass. |
| **NS-41** | Low · Low · Low | **Small verified odds-and-ends:** (a) gate the issue-family seed fetches on reranked-pool cardinality (they run even when the pool is already rich — measured 0ms when no family matches, so payoff exists only on matched-family queries); (b) authority/supporting-fact fallback caps are hardcoded `limit+offset+10`, uncoupled from the NS-19 3× scope; (c) `matchedCuratedKeywordFamilies` evaluated ~3× per query in `lexicalTerms` (sub-ms — memoize when touching that file); (d) `decisionLayerSectionLabelClause` rebuilds a static 12-LIKE string per call; (e) the `citation_lookup` decision-scope path (`useMergedOnlyDecisionScope=false`) fetches ALL chunks per scope doc — debug-only surface today. |

---

## 2. Resolved log

### 2A. Search quality & latency deep dive (NS-*, 2026-07-04)

Judged-eval scoreboard moved **mean P@5 0.533 → 0.933, MRR 0.750 → 1.000**; every slow class except the migration-blocked family class is now sub-2s. Two bugs were discovered *during* the work and are recorded as NS-35/NS-36.

| ID | Commit | What landed |
|---|---|---|
| 2nd-audit fixes | `60e22e5` | Retired the LAST full-corpus scan trigger (whole-word rescue → scan-parity FTS when available; was 83s on broad vocabularies); unified the futility probe + parity fetch into ONE round trip (sparse misspellings 167ms → 66ms); relaxed-AND ladder extended to k=2 and made guard-aware (a tier only wins if its rows survive the row guard). |
| NS-13 | `0f706c6` | Judged relevance eval harness (P@5/MRR per-query regression gate + latency budgets; `pnpm test:search-eval`). The measurement foundation. |
| NS-29 | `9f460fc` | Futility probe — an OR-of-prefix FTS probe (LIMIT 1) proves emptiness before the 667k-row scan. Gibberish 57.9s → 48ms, byte-identical empty. |
| NS-30 | `d86459a`,`ae32a13` | Single-execution-term keyword queries FTS-served with scan-parity ranking. "rent" 38s → 8-12s byte-identical; "habitibility" 103s → 20ms. |
| NS-30c | `59fb2d4` | Probe-matched sparse queries fetch rows from the index in scan-parity mode instead of scanning. "harrassment by lanlord" 30.4s → 167ms; "illegal lockout" ~84s → 1.6s exact pins. |
| NS-03 | `538835b` | Quoted queries upgrade keyword→`exact_phrase`; fixed the duplicate-result-row bug it exposed; two goldens re-pinned off the enshrined dupes. quiet_enjoyment_quoted p5 0.20 → 1.00. |
| NS-04 / NS-07 | `edd8ebc` | Long NL questions keep phrase understanding (`selectivePhraseConceptGroups` + relaxed AND tiers; stopwords extended with interrogatives/modals). NL benchmark 43-67s/p5 0 → ~200-400ms/p5 0.60. |
| NS-05 | `16df11e` | Phrase-FTS candidate search survives structured filters (intersects the filter scope). "quiet enjoyment"+judge: 96-of-233-doc slice → full scope, p5 1.00. |
| NS-08 | `236702f` | Dotted section refs ("37.9(a)(2)") become exact FTS adjacent-token phrase arms. 33s → 1.4s with heaviest-citing decisions on top; nonexistent refs fast-empty. |
| NS-09 / NS-09b | `5e6f5f5`,`17c7ad4` | Anchored 53 alternation regexes (`/\bA\|B\|C\b/` → `/\b(?:A\|B\|C\b)/`) across all 7 search modules + a source-guard test. "theater" no longer heat-scopes. |
| NS-17 (incr. A) | `4f644cb` | Per-chunk phrase-concept hard-eliminate removed (the -0.28 penalty already demoted the same rows). Golden byte-identical — the eliminate only destroyed recall depth. |
| NS-01 | `df16617` | Zero-result queries retry once through a ~30-entry domain spell map (full-pipeline re-run). "habitibility" 103s/empty → 434ms/p5 1.00. |
| NS-27 | `21ea794`,`edc4d81`,`67a9714` | Vector-first class lexically rescues on an empty channel (bare + multi-token); `vectorErrored`/`vectorErrorMessage` in diagnostics+logs; namespace-less retry gated to namespace-specific errors; ingest failures logged. |
| NS-36 | `59fb2d4`,`edc4d81` | Dead-vector kill bar (empty vector channel → only lexical<0.35 rows die) + phrase-first rescue + span instrumentation. landlord_harassment empty → p5 1.00 at ~400ms. |
| NS-16 (vector) | `741dfee` | `calibratedVectorFusionScore` spreads bge's 0.55-0.95 band across [0,1] before the 0.23 weight — fixed affine (pagination-stable), fusion-term-only (guards keep raw cosine). |
| NS-10 / NS-21 | `dac12ff` | Vector-first check hoisted above the ≤2-token skip; topK 25→100 (metadata dropped); `vectorSearchLimit` floors at 50, decoupled from page size. |
| NS-19 | `abf4215` | Decision scope admits 3× the limit so ±0.16-0.42 layer boosts can reorder; `exact_phrase` keeps the tight scope (eval caught topical docs displacing phrase-containing ones). |
| NS-11 | `3cf66a1` | Family+judge universe qualifies by FTS match before the recency cap (`fetchFtsMatchingDocumentIds`). D1-adjudicated re-pins on the two judge goldens. |
| NS-20 | `33dd109` | Content-stable `chunkId` tiebreak on all 3 score sorts — re-ingestion can't reshuffle equal scores. Pagination probe consistent. |
| NS-25 (code) / NS-33 | `3dbb0ce` | Synonym expansion no longer reaches the embedder (raw query + curated variant only); scope filters use Sets. Seed-fetch parallelization + `scoreRow` memoization rejected on measurement/hazard. |
| NS-22 (code) | `67a9714` | `embed()` takes `isQuery`; search + explicit-probe queries carry the bge s2p prefix; passage-side pinned raw by a source test. |
| NS-35 | `edd8ebc` | FTS kill-switch hardening: only structural errors disable `searchFtsAvailable`; transient errors return an identity sentinel affecting one query. Was the root cause of golden rank-flapping under load. |
| NS-02 | via NS-29/30/30c | Early-futility detection everywhere; the ~25-28s zero-hit/broad-token ladder is gone except the migration-blocked family class. |
| NS-06 | deprioritized (`07572c0`) | Both uncovered-topic eval entries (security_deposit, ellis_act) hit p5 1.00 after the recall fixes — the FTS phrase path handles un-lexiconed topics. Curated families are optional enrichment now. |

### 2B. Product backlog & 2026-07-02 audit (all resolved unless flagged in §1)

**Original P0–P2 backlog — done & verified:**

| ID | What was fixed |
|---|---|
| REL-01 | Pre-deploy CI gate: API+web typecheck + full `test:source` suite + relevance/highlight before `wrangler deploy`. |
| REF-01 | Citation normalizers no longer over-strip (word-boundary / validated-roman rules); unit-tested 7/7. |
| SEARCH-03 | `debugProfile` surfaces requested vs production query type + match flag (code + schema + test). |
| INGEST-01 | Every ingest path size-bounded — multipart, JSON body, decoded bytes, DOCX decompression. |
| LLM-01 | Both LLM paths fence untrusted text as data + fallback transparency; guard test blocks new unfenced paths. |
| LLM-02 | Assistant + draft + the embedding `env.AI.run` all time-bounded (15s race → degrade). |
| WEB-01 | Search uses AbortController + request epoch; ignores stale responses. |
| WEB-02 | User-facing helpers zod-parse; admin-ingestion GETs shape-guarded. |
| UI-01 | No fabricated dashboard/model signals; placeholder labeled as planned. |
| DATA-02 | Activation gate blocks `active=1` on vector-write failure; report surfaces per-status breakdown + blocked lists. |
| PERF-01 | Hot path fully cached (memoized query context + cached row text; zero per-row re-normalization); locked by a guard test. |
| DATA-01 | All destructive writes partial-failure-safe (reprocess-when-empty gate, reference snapshot+restore, DATA-02 gating, write-then-swap). *Caveat: `rebuildDocumentTextArtifacts` is unwired — see CODE-01.* |
| SEARCH-01 | Cold-start + decision-layer fixed; vector stage now parallel + timeout-bounded. *Caveat: prod vector timing to re-measure on a deployed target.* |
| ADMIN-01 | Prefilter `COUNT(*)` gate → exact top-N for ≤4000 derived sets (full 1082-doc staged set ranked live); honest completeness diagnostics. |
| FACET-01 | Filters use indexed facet tables + runtime `ensureDocumentFacetTables` safety net. *Caveat: migration 0009 must deploy before the code; residual owner-move-in JSON `LIKE` is intentional fallback.* |
| SEARCH-02 (a–d) | The ~10.9k-line monolith → 9 focused modules (`search.ts` −84%, orchestration only), every extraction golden byte-identical; 29 topic predicates folded into a data-driven lexicon; 90 fragile signature-pin tests retired. |
| SEARCH-04 | 27-query golden ranking net (byte-identical top-N), skip-guarded, `UPDATE_SEARCH_GOLDEN=1` regenerates. |
| SEARCH-05 | D1 ~100-bound-param overflow root-fixed: `boundLexicalTermsForD1` caps term expansion at every lexical site + bounded recall universe; degrade-on-overflow backstop. |
| CORS-01 | Fail-closed — config-only allowlist, no hardcoded fallback; unset ⇒ no cross-origin. |
| REPO-01 / REPO-02 | Experiment cluster archived (−22.9k lines, 341→234 aliases); catalog is JSON + typed wrapper (fixed a real C88 shadowing bug). |

**2026-07-02 audit — High (all ✅):** SRC-02 `5956dd5` (source-link arg swap — every "Open source" link 404'd) · DATA-03 (retrieval activation/rollback D1 bind overflow; `SQLITE_BIND_LIMIT` 200→90 + chunked) · API-02 `9ce8c8f` (error handling: infra faults → 500, not raw-message 400) · WEB-03 `7a7dced` (ingestion admin approve-onto-wrong-doc race).

**2026-07-02 audit — Medium (all ✅):** API-03 `943dd08` · PERF-03 `a3da281` · API-05 `9d37d2c` · ARCH-02 `c00230d` *(redesign half open — §1C)* · API-04 `afc6ed2` · WEB-06 `7a728a7` · WEB-04 `73a8c55` · WEB-05 `c17837d` · PERF-04 `29a06a3` *(b/c deferred — §1C)* · PERF-02 `56542e6` · CONF-02 `10452d1` · TEST-02 `f9ebbbf` · CI-01 `041ce6e` · DATA-04 `1323c3a` · PERF-06 (scoring hot-loop pattern memoization) · CODE-01 (`noUnusedLocals`/`noUnusedParameters` enabled; 154 diagnostics + dead clusters removed).

**2026-07-02 audit — Low (9/10 ✅):** WEB-08 `b08e808` · WEB-07 `3ccc69e` · CONF-04 `dcfdc52` · WEB-09 `8f9f5be` · WEB-11 `dc09972` · CODE-03 `b29b404` · TEST-03 `78b5bf0` · CODE-02 `a30b612` · CONF-03 `93b6a75` *(safe half; compat-date deferred — §1A)*. PERF-05 deferred (§1C).

---

## 3. Reference

### 3A. Verification baselines & scoreboard

- **Golden net:** `tests/search-golden-ranking.test.mjs` — 27 queries, byte-identical ordered top-N. `UPDATE_SEARCH_GOLDEN=1` re-pins (do it deliberately, document the rationale).
- **Judged eval:** `pnpm test:search-eval` — 17 queries, P@5/MRR floors + latency budgets vs committed baseline. `UPDATE_SEARCH_EVAL_BASELINE=1` re-baselines. Current: mean P@5 0.933 / MRR 1.000.
- **Deterministic gate (`test:source`, 74):** every `*-source.test.mjs` guard — runs in the deploy gate.
- **Suites:** `test:utils` 38 · `test:web` 16 · `test:case-assistant` 4 · phrase+gate live suites skip cleanly with no server.
- **Local env note:** `wrangler dev --local` on the full corpus (~14k docs / 1.13M FTS rows / 667k retrieval chunks). `env.AI` throws "needs to be run remotely" — vector search degrades to null, so all vector-side changes are locally inert/byte-stable by construction.

### 3B. REL-02 runbook (GitHub UI, ~5 min)

In-repo work is complete (`apply-d1-migrations.yml` is a manual `workflow_dispatch` gated on `environment: production-d1-migrations`; `deploy-api.yml` no longer applies remote migrations on push). To close it:

1. **Secrets** — Settings → Secrets and variables → Actions: confirm `CLOUDFLARE_API_TOKEN` (needs **D1 → Edit** on the `beedle` DB) and `CLOUDFLARE_ACCOUNT_ID`.
2. **Environment** — Settings → Environments → New environment, named **exactly** `production-d1-migrations` (a typo silently skips protection).
3. **Protection** — inside it, tick **Required reviewers** (add yourself + ideally a second); optional wait-timer; restrict **Deployment branches** to `main`. Optionally move the two Cloudflare secrets here as environment secrets.
4. **Run when needed** — Actions → "Apply D1 Migrations" → Run workflow. It lists pending migrations, **pauses for your approval**, then applies. Nothing touches prod D1 until you approve.
5. **Verify** — after approval, `wrangler d1 migrations list beedle --remote` shows **0 pending**. Closes REL-02.

### 3C. SRC-01 runbook (Cloudflare data-ops)

The route reads `SELECT source_r2_key FROM documents WHERE id=?` then `SOURCE_BUCKET.get(key)`. Prod bindings: D1 `beedle`, R2 `beedle-sources`. Run from `apps/api/`.

1. **Expected keys:** `pnpm wrangler d1 execute beedle --remote --json --command "SELECT id, source_r2_key FROM documents WHERE source_r2_key IS NOT NULL AND source_r2_key != ''" > /tmp/expected.json`.
2. **Present objects:** `pnpm wrangler r2 object list beedle-sources --json > /tmp/present.json` (paginate `--cursor` if large).
3. **Diff** — keys in `expected` but absent from `present` = the exact 404 set (sanity-check it includes T210489, T250099, T221447, S001-92T, T210403).
4. **Fix each:** stale/renamed key → `UPDATE documents SET source_r2_key='<correct-key>' WHERE id='<id>'`; genuinely missing → `wrangler r2 object put beedle-sources/<key> --file <original>`.
5. **Verify** — `curl -sD - https://beedle-api.clifton23.workers.dev/source/T210489 -o /dev/null` returns 200 **without** the `x-beedle-source-fallback: r2-missing-db-text` header. Re-run the diff → 0 missing. Closes SRC-01.
6. **Prevent recurrence** — confirm ingest always uploads the R2 object in the same step it writes `source_r2_key`; add a periodic D1↔R2 reconciler if drift recurs.

### 3D. Verified non-issues / corrections

- **SQL injection** — low risk: dynamic values are bound params; admin sort keys are switch-whitelisted.
- **XSS** — search highlights render as React text nodes, not `dangerouslySetInnerHTML`.
- **Migrations 0001–0009** sequential + non-destructive; 0009 DDL exactly matches the runtime `ensureDocumentFacetTables` copy (12/12).
- **Search module graph** — strictly layered/acyclic (no module imports `./search`; scoring doesn't import fts); `searchFtsAvailable` is an ESM live binding written only inside search-fts; no TODO/FIXME/commented-out code in the 9 modules.
- `JSON.parse` of DB JSON guarded at all sites; assistant-chat/draft degrade paths behave as designed; no N+1 web fetch patterns; packages/shared + prompts have zero dead exports.
- **2026-07-04 second-audit claims that FAILED verification** (recorded so they aren't re-litigated): `ensureSearchRuntimeIndexes` double-run on the spell retry (it is module-memoized — costs one boolean); `bindIndexCodeMatchValues` "double-loop bug" (intentional — the clause builder emits two placeholder groups, facet + reference, and counts match); vector-first queries leaking into the provisional scan (probed: trusted_only "buyout"/"harassment" run 70-310ms); JSON facet columns "unused" in recall SELECTs (scoreRow's cachedRowMetadata parses them for metadata boosts); layer-boost work running when decisionLayerMap is undefined (the `if (layers)` guard already skips it); `primaryIssueSignals` "double-run" (the two calls take different inputs — query vs retrievalQuery — by design); the trust-tier EXISTS "cannot use the index" (the (document_id, active, batch_id) composite serves the (document_id, active) prefix).

### 3E. Demoted from the active list (recorded so they aren't re-litigated)

- **HYG-01** — `R2_PUBLIC_BASE_URL = example.invalid` is intentional (source links proxy through the API); the real issue was SRC-01/SRC-02.
- **HYG-02** — destructive migrations fold into REL-02.
- **HYG-03** — hardcoded Cloudflare URLs are a portability smell, not a product blocker.
- **BUG-12 / BUG-18** — dead phrase-concept ternary + `ensureSearchFts` error-catch concern: cleanup, not standalone bugs.
