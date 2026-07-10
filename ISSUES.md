# Beedle AI — Issue Backlog & Resolution Log

**Single source of truth.** Consolidated 2026-07-04 — merges the product/audit backlog with the search-quality deep dive (formerly `NEW-ISSUES.md`).

This document is organized **open work first, history second**:

1. **[Open work](#1-open-work)** — everything genuinely left, grouped by who owns it.
2. **[Resolved log](#2-resolved-log)** — a compact record of what's been fixed (full prose lives in git history / commit messages).
3. **[Reference](#3-reference)** — verification baselines, the REL-02 runbook, the SRC-01 completion note, verified non-issues, and demoted items.

**ID namespaces** are preserved as-is because commits reference them: `REL/REF/DATA/SEARCH/FACET/ADMIN/INGEST/LLM/WEB/UI/REPO/CORS/API/ARCH/PERF/CONF/CODE/TEST/CI` (product + 2026-07-02 audit), `NS-01…NS-36` (search-quality deep dive), and `SEC/ABUSE` (2026-07-09 hardening review). Auth is tracked (`AUTH-01`) but was explicitly out of scope for these passes.

**Current verification state:** `main` deployed successfully to Cloudflare 2026-07-07 (`Deploy API` green at merge commit `1f32ac8`) · prod `/health` green (`aiAvailable=true`, vector binding present) · remote D1 has **0 pending migrations** · R2/source smoke green (sample `/source/:documentId` responses are 200 markdown with 0 fallback headers; unsafe source keys = 0). Local deterministic gates remain green: golden net 27/27 byte-identical · `test:source` 74/74 · `test:utils` 38/38 · `test:web` 16/16 · API + web typecheck clean. **Production search eval is not green yet:** remote judged eval 13/17 tests passed, mean P@5 **0.814** / MRR **0.857**, with failures tracked in §1B.

---

## 1. Open work

### 1A. Needs you — external ops or a product decision (no repo work left, or a call only you can make)

| ID | Sev | What's needed | Detail |
|---|---|---|---|
| **AUTH-01** | **Critical** | Configure and deploy the new app-native auth layer | Code foundation landed in `10b526e`: Worker username/password login, PBKDF2 password-hash secret, signed HttpOnly session cookie, CSRF token for unsafe requests, auth-aware rate-limit keys, web login/logout/session handling, and setup helper. Still open until the three Cloudflare secrets are set (`AUTH_USERNAME`, `AUTH_PASSWORD_HASH`, `AUTH_SESSION_SECRET`), the branch is deployed, and production smoke confirms unauthenticated requests are blocked while login succeeds. |
| **REL-02** | High | Enable required-reviewers on the `production-d1-migrations` GitHub Environment | 100% in-repo and latest remote check shows 0 pending migrations. Still open because GitHub API reports `protection_rules: []`; add required reviewers via [runbook in §3B](#3b-rel-02-runbook-github-ui-5-min). |
| **FTS index rebuild** (NS-28/30/31 residual) | High | Add `title`/`author` columns to `search_chunks_fts` via migration | The **last slow class**: multi-term curated families ("mold", 40–80s) can't get scan-parity because their top matches are title/author-weighted and those columns aren't indexed. A rebuild lets NS-30's FTS routing cover them. Migrations are manual/decoupled — the code needs the runtime-safety-net pattern (like `ensureDocumentFacetTables`) so it's correct before the migration lands. **NS-32** (`documents.is_trusted` materialization + composite index) can ride the same migration to kill the correlated trust-tier `EXISTS` scan-tax. |
| **NS-34** | Med | Corpus data cleanup (two items) | (1) Retire duplicate remand doc `doc_3d98c3ec-d98…` for T150579 (identical twin, transposed title). (2) Re-extract citations for the **11 docs sharing bogus citation "316928"** (real citations are in their titles). *The golden "twins" that look like dupes are legitimate original+remand pairs — leave those.* |
| **NS-12** | Med | Decide include/exclude for 1,084 staged-invisible docs (7.7% of corpus) | `searchable_at IS NULL AND rejected_at IS NULL`. If real decisions are stuck in QC, no ranking fix can surface them. Reviewer-readiness tooling already exists; then bulk-activate. |
| **CONF-03** (compat-date half) | Low | Bump API `compatibility_date` 2025-02-15 → 2026-04-03 during a supervised deploy | Flips 14 months of runtime flags; do it with the CI-01 post-deploy smoke watching. (The `minify=true` half already shipped.) |
| Local test-DB seed | Low | Seed local D1 reference tables so 6 `legal-reference-normalization` **live** tests pass locally | Pre-existing, confirmed via A/B (not a regression). Run `pnpm normalize:references` / apply `0009` + re-seed, or document the setup. Unit coverage already passes. |

### 1A.1. Security and cost controls — 2026-07-09 review

| ID | Sev | What's needed | Detail |
|---|---|---|---|
| **SEC-01** | **High** | Validate uploaded file signatures and serve sources safely | Multipart ingestion accepts a client-provided MIME type, persists it, and `/source/:documentId` serves it inline. An attacker can store HTML/JS under the API origin. Allow only verified DOCX/PDF/Markdown types; use attachment delivery plus `nosniff`/CSP. This is separate from search-result XSS, which remains a verified non-issue. |

### 1B. Production search tuning — remote eval failures now measured

The 2026-07-07 Cloudflare deploy unblocked the real-vector / real-D1 measurement pass. The app is healthy and usable, but the production search eval is **not** at the committed local baseline yet. Treat this as the next engineering lane before calling search production-ready.

| ID | State | What the remote run showed / next direction |
|---|---|---|
| **PROD-SEARCH-01** | New high-priority follow-up | Remote judged eval against `https://beedle-api.clifton23.workers.dev` passed 13/17 tests. Failures: `mold_topic` P@5 0.6→0, `landlord_harassment` P@5 1.0→0.6, `nl_rent_increase_question` P@5 0.6→0, and `zero_hit_gibberish` returned 5 results instead of 0. First step: reproduce each with `debugProfile`, compare local/prod candidate sets, then fix behind the judged eval. |
| **PROD-PERF-01** | New high-priority follow-up | Production phrase QA passed 16/25; failures are mostly timeouts/slow paths. Phrase performance guard warned on `pipe noise` (12.6s total, finalizeResults bottleneck) and `ceiling leak in bedroom` (7.9s total, finalizeResults bottleneck). Broader QA also flagged slow lexical searches for `bathroom window leak` and `plumbing noise`. |
| **FTS index rebuild** (NS-28/30/31 residual) | Still open | Remote results keep validating this as the main structural search item: title/author are still outside `search_chunks_fts`, which especially hurts broad/curated families such as mold. Pair with NS-32 materialized trust-tier indexing in one supervised migration. |
| **NS-22 / NS-16 / NS-10 / NS-21** | Shipped, needs remote tuning | bge query prefix, fixed vector calibration, topK 100, and recall floor are now live. Remote eval shows they did not automatically preserve the local relevance baseline, so tune with production candidates/cosines rather than assuming the local inert-vector baseline holds. |
| **NS-25** | Code half shipped | Synonym soup is out of the embedder. Remaining: rewrite curated keyword-bag variants as short natural-language reformulations and judge them against production vector behavior. |
| **NS-21** (skip heuristic) | Open | Decide whether to downweight instead of skip vector when phrase-FTS "has enough"; now measurable on production. |
| **NS-23** | Open | Context-enriched re-embed (`"{title} — {sectionLabel}\n{chunkText}"`) + hard-split >1,600-char paragraphs. Requires a re-embedding backfill (machinery exists); validate against production eval before rollout. |
| **NS-24** | Open | Exempt high-vector rows from lexical guards only after inspecting real cosine distributions from production failures. |
| **NS-26** | Open | Vectorize metadata filtering (push documentId/fileType/active into query `filter`; oversample topK when filters active) remains useful but needs metadata backfill + production measurement. |

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
| **NS-37** | ✅ RESOLVED (`1c4c82a`) | Real document frequencies via a lazily-created fts5vocab reader: the k=2 relaxed tier picks the two RAREST groups (k=4/3 keep the eval-tuned length proxy — DF-reordering them starved the NL benchmark, eval-caught and scoped back); the parity fetch and vector-first rescue prune >50k-row tokens from their OR vocabularies (rarest always kept; empty pruned fetch retries unpruned). "Katayama mold decision" 10.4s → 2.4s with the slate flipping to the judge's actual mold decisions; "harassment under 37.9(a)(2)" 13s → 0.8s with all-topical top-5 (old top-3 had zero harassment chunks). |
| **NS-38** | Low-Med · Med · Med | **`orderDecisionFirst` runs twice per request** — but the two passes take DIFFERENT candidate sets (raw reranked rows for topDecisionIds; guard-filtered + doc-boosted decision-scoped rows layer-aware), so evidence summaries legitimately differ and this is NOT a pure memoization — a single pass needs a structural refactor of the presentation flow (~15-30ms at stake). The display-layer picker "re-scan" half was over-counted by the audit agent: the two pickers apply DIFFERENT scoring functions (authority vs supporting-fact). Keep only if a latency budget demands it. |
| **NS-39** | Med · Med · Med | **Scope-candidate GROUP BY queries materialize every matching chunk before aggregating** (`fetchHabitabilityCandidateDocumentIds` / lockout / issue variants): inner subquery scans all LIKE-matching chunks corpus-wide, then GROUP BY + full sort before LIMIT. Hits the habitability/lockout/issue-guided classes (currently masked by their doc scopes staying small). **Direction:** top-K aggregation or a bounded inner LIMIT; measure first — these classes are currently fast because scopes bind early. |
| **NS-40** | ◐ partial (`f8003c3`) | Snippet-target preparation now cached per request (was rebuilt per row — the largest item in this bundle). Remaining, smaller: layer/authority/support text normalized up to 4× per group in orderDecisionFirst; sentenceFactualTokenMetrics recomputed in 3-4 downstream callers; habitabilityCoverageSignals' 11 sequential .match() chains. |
| **NS-41** | ◐ partial (`f8003c3`) | (c) matchedCuratedKeywordFamilies memoized and (d) decisionLayerSectionLabelClause built once — done. Deliberately left: (a) seed-fetch cardinality gating and (b) the authority-cap retune (both behavior changes on golden-covered paths for ~ms gains) and (e) the citation_lookup all-chunks fetch (debug-only surface). |

---

## 2. Resolved log

### 2A. 2026-07-09 reliability and input hardening

| ID | Commit | What landed |
|---|---|---|
| AUTH-01 code foundation | `10b526e` | Added app-native authentication at the Worker boundary: only `/health` and auth endpoints remain public; all search, source, admin, ingest, and LLM endpoints require a signed HttpOnly session. Password verification uses a secret-backed PBKDF2-SHA256 hash, unsafe authenticated methods require a per-session CSRF header, login attempts are rate-limited, cost controls now key by authenticated username, and the web app has login/logout/session handling. Operational closeout remains in §1A until secrets are configured and production smoke passes. |
| DATA-05 / INGEST-01 follow-up | `e68c932` | New ingests now parse before persistence, remain non-searchable until R2 is written, and compensate both the generated R2 object and D1 document graph on a failed artifact/source write. Vector failures after commit degrade to the existing repair path. |
| SEARCH-06 | `146037f`, `e93bea2` | Removed request-time FTS DDL/backfill, eliminating concurrent cold-isolate duplication. Migration `0010_search_fts_backfill.sql` is a fast compatibility checkpoint: `0008` owns FTS schema/trigger creation, the existing production index is already populated, and triggers maintain future changes. The original full-corpus dedupe/backfill exceeded remote D1 CPU and was intentionally replaced; the corrected protected migration run then succeeded. |
| API-06 | `4047a73` | `readJson` now streams and caps chunked as well as declared JSON bodies (1 MiB default; ingestion retains its explicit larger cap). Search, drafting, and assistant request schemas now bound text and collection sizes. |
| API-07 | `a1c0dde` | Only Zod and explicitly classified request-validation errors are client-visible. All other service errors are logged and return a generic 500, preventing provider/storage/SQL detail leakage. |
| ABUSE-01 | `244ee4c` | Added Cloudflare Rate Limiting bindings and fail-closed enforcement before routing costly POSTs: search/debug (60/min), ingestion/vector work (3/min), LLM work (6/min), and destructive admin writes (6/min), keyed by client IP until AUTH-01 provides a user subject. Rejections return `429`/`Retry-After`; unavailable enforcement returns `503` rather than allowing unbounded cost. |
| INGEST-02 | `4947213` | Replaced synchronous per-chunk ingestion embeddings with durable `document_vector_jobs` and a Cloudflare Queue consumer. Ingest writes the job with the document graph, then returns after enqueueing; the consumer handles one document per message (max two concurrent invocations), embeds at bounded concurrency 4, upserts in batches of 100, retries with exponential backoff, records a final failed state, and the five-minute cron re-enqueues stale queued jobs. Queue provisioning and protected migrations are complete; deployment remains gated on PR checks. |
| INGEST-03 | Operations complete 2026-07-09 | Created Cloudflare Queue `beedle-vector-jobs` (`aebcf19ac5884a82ae95331b1542dfe1`). Protected workflow [29058359454](https://github.com/cliftonbnoble/beedle-ai/actions/runs/29058359454) applied migrations `0010` and `0011`; a second protected run [29058397374](https://github.com/cliftonbnoble/beedle-ai/actions/runs/29058397374) verified that no migrations remain. |
| CI-02 | `44b7758` | Cloudflare Pages had invoked `npx @cloudflare/next-on-pages@1` during every build, which resolved a fresh incompatible Wrangler/types tree. Pinned the adapter and its declared build peers in `apps/web`, then invoked the local binary. The exact Pages build, web typecheck, and 16 web tests pass locally; the hosted Pages preview and GitHub CI gate both passed. |

### 2B. Search quality & latency deep dive (NS-*, 2026-07-04)

Local judged-eval scoreboard moved **mean P@5 0.533 → 0.933, MRR 0.750 → 1.000**; this remains the deterministic local baseline. The 2026-07-07 production run is lower (see §1B), so future search work should use the remote failures as the driving cases rather than re-baselining them away. Two bugs were discovered *during* the work and are recorded as NS-35/NS-36.

| ID | Commit | What landed |
|---|---|---|
| dead-code sweep | `d6c3e84` | Deleted 3 uncalled exports (refreshDocumentReferenceValidation, canonicalJudgeNames, formatAdjudicationTemplateJson) and de-exported 7 internal-only members; tsc unused-flags prove the rest alive. |
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

### 2C. Product backlog & 2026-07-02 audit (all resolved unless flagged in §1)

**Original P0–P2 backlog — done & verified:**

| ID | What was fixed |
|---|---|
| REL-01 | Pre-deploy CI gate: API+web typecheck + full `test:source` suite + relevance/highlight before `wrangler deploy`. |
| SRC-01 | Prod R2 source repair complete: all 14,071 document source keys are backed by R2 markdown objects, 73 stale/unsafe legacy keys were repaired in D1, and a full `/source/:id` audit returned 14,071/14,071 200 markdown responses with 0 `x-beedle-source-fallback` headers. Preventive sanitizer fix: `593aa8a`. |
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
| DATA-01 | All destructive writes partial-failure-safe (reprocess-when-empty gate, reference snapshot+restore, DATA-02 gating, write-then-swap). *(The old "rebuildDocumentTextArtifacts is unwired" caveat is stale — it was deleted in the CODE-01 commit `8661826`.)* |
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

In-repo work is complete (`apply-d1-migrations.yml` is a manual `workflow_dispatch` gated on `environment: production-d1-migrations`; `deploy-api.yml` no longer applies remote migrations on push). 2026-07-07 remote check shows **0 pending migrations**, but GitHub still reports `protection_rules: []` for the environment, so REL-02 remains open until required reviewers are enabled. To close it:

1. **Secrets** — Settings → Secrets and variables → Actions: confirm `CLOUDFLARE_API_TOKEN` (needs **D1 → Edit** on the `beedle` DB) and `CLOUDFLARE_ACCOUNT_ID`.
2. **Environment** — Settings → Environments → New environment, named **exactly** `production-d1-migrations` (a typo silently skips protection).
3. **Protection** — inside it, tick **Required reviewers** (add yourself + ideally a second); optional wait-timer; restrict **Deployment branches** to `main`. Optionally move the two Cloudflare secrets here as environment secrets.
4. **Run when needed** — Actions → "Apply D1 Migrations" → Run workflow. It lists pending migrations, **pauses for your approval**, then applies. Nothing touches prod D1 until you approve.
5. **Verify** — after approval, `wrangler d1 migrations list beedle --remote` shows **0 pending**. Closes REL-02.

### 3C. SRC-01 completion note / future drift audit

Completed 2026-07-06. Prod bindings: D1 `beedle`, R2 `beedle-sources`; source route: `https://beedle-api.clifton23.workers.dev/source/:documentId`.

- D1 has 14,071 documents and 14,071 non-empty `source_r2_key` values. `source_link` remains the stored sentinel/original R2-style URL; the API response rewrites it through `effectiveSourceLink` to `https://beedle-api.clifton23.workers.dev/source/:documentId`.
- The empty R2 bucket was backfilled from `import-batches/markdown-corpus`.
- 73 legacy keys containing `..`, `#`, or control characters were uploaded under sanitized keys and repaired in D1.
- Direct R2 probes passed for both a normal object and a repaired object.
- Full production source-route audit passed: 14,071/14,071 returned `200`, `text/markdown`, and no `x-beedle-source-fallback` header.
- Preventive code fix: `593aa8a` collapses repeated dots and trims leading/trailing dots/underscores in new ingest source filenames.

Future drift check:

1. Export prod document IDs/source keys from D1.
2. Reject any `source_r2_key` containing `..`, `#`, or control characters.
3. Audit `GET /source/:documentId` for every document ID; any non-200 or `x-beedle-source-fallback` header is a real R2/D1 drift candidate.
4. For repaired objects, prefer uploading under a sanitized key and updating D1 rather than persisting unsafe object names.

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
