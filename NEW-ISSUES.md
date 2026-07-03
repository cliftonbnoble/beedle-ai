# NEW-ISSUES — Search Slowness & Result-Quality Findings

**Created:** 2026-07-04 · **Scope:** ONLY (a) slowness and (b) causes of suboptimal/irrelevant search results — with special depth on **phrase understanding** (a user types a phrase; the system should understand it and search with its key context). General code-quality items live in ISSUES.md; nothing here duplicates the closed 2026-07-02 audit.

**Method:** live measurements against the full local corpus (~14k docs / 667k retrieval chunks; local D1 is slower than prod but relative patterns hold), first-party code verification of every phrase-pipeline gate, plus focused sub-audits (recall/query understanding; ranking caps/fusion/scoring; vector path; slow-path tracing). Every finding carries file:line evidence.

**Ratings:** each finding gets **Severity** (impact on users) · **Complexity** (effort to fix) · **Break-risk** (chance the fix regresses ranking/behavior — anything touching scoring is verified against the 27-query golden net, which catches ranking shifts byte-for-byte).

---

## Status at a glance (updated 2026-07-04)

**Scoreboard:** judged-eval mean P@5 **0.533 → 0.933**, mean MRR **0.750 → 1.000** (17 judged queries, every scored query places a relevant document at rank 1). Golden net 27/27; every re-pin documented in its commit. All latency classes sub-2s except the migration-blocked one below.

**Resolved (all verified against golden + eval + source/utils suites):** NS-01 (spell rescue), NS-02 (futility ladder), NS-03 (quoted phrases + dup rows), NS-04/NS-07 (NL phrase understanding), NS-05 (filtered phrases), NS-08 (section references), NS-09/NS-09b (regex anchoring, 53 literals + guard test), NS-13 (eval harness), NS-17 increment A (eliminate→demote), NS-27 (vector-first lexical rescue), NS-29/NS-30/NS-30c (futility probe, scan-parity FTS routing), NS-35 (FTS kill switch), NS-36 (dead-vector guard bar). NS-06 deprioritized after measurement; NS-34 investigated (remand pairs are legitimate results).

**Blocked on user action:**
1. **FTS index rebuild migration** (multi-term family class — "mold" 40-80s): add title/author columns to search_chunks_fts so scan-parity covers document-level matches; migrations are applied manually (decoupled from deploy).
2. **Corpus data ops** (NS-34): retire duplicate remand doc `doc_3d98c3ec-d98…` (T150579); re-extract citations for the eleven docs sharing citation "316928" (real citations are in their titles).
3. **NS-12 staged docs**: 1,084 staged-invisible documents need an include/exclude decision.
4. **Remote vector channel** (NS-10/11/16/19/21/22/23/24/26): query-prefix, topK, fusion normalization, and re-embed work all need the deployed AI binding — local env.AI is inert, so none of it can be verified here.

**Open, deliberately deferred (no failing judged query drives them):** NS-14 (positional lexical-token retention), NS-15 (per-doc rank flood — measured in NS-36, inherited by design from scan semantics), NS-17 topic literal gates (defensible precision), NS-18/NS-20/NS-25 (presentation/pagination), NS-33 (seed-fetch parallelization + micro-perf).

---

## Measured baseline (why this document exists)

**Latency by query class** (golden suite, 4 runs averaged, local):

| Query class | Example | Avg latency |
|---|---|---|
| Topic keyword (covered) | "section 8 voucher termination" | **77 ms** |
| Two-term topic | "pipe noise", "noise nuisance" | 200–310 ms |
| Judge-filtered broad | "mold" + judge | 660 ms |
| Phrase (concept-rich) | "breach of quiet enjoyment" | 1,965 ms |
| Rent topic | "annual rent increase" | 4,258 ms |
| **Broad single token** | **"rent"** | **26,625 ms** |
| **Broad in trusted_only mode** | **"mold" (trusted_only)** | **25,342 ms** |
| **Zero-hit** | gibberish / misspellings | **28,005 ms** |

The slow classes are **30–300× the median**. Worse, the slow classes are exactly the ones frustrated users hit: misspellings and unusual phrasings fall into the zero-hit path.

**Relevance probes (live):**
- `habitability` → 5 relevant citations. `habitibility` (misspelling) → **zero results**, after the ~28s zero-hit path. Same for `harrassment`.
- `rent increase notice` (keywords) → 5 results fast. `can my landlord raise rent twice in one year?` (natural language) → fell into the slow path (>2 min locally).
- Quoted `"quiet enjoyment"` behaves identically to unquoted — quotes are stripped; explicit phrase intent is ignored.

**Corpus/index state (local mirror):** 14,169 documents; 13,085 searchable; **1,084 (7.7%) staged-invisible**; 467,650 document chunks + 667,180 retrieval chunks (all active); FTS 1,134,830 rows (both populations indexed ✓); 667,191 embedding rows (full row-level coverage ✓).

---

## 🔴 HIGH severity — direct causes of bad results or extreme slowness

### NS-01 · Misspellings and out-of-lexicon phrasings return zero results — and pay the slowest path — ◐ LARGELY RESOLVED (df16617 + NS-29/NS-30 futility skips)
**Status:** zero-result queries retry once through a ~30-entry domain spell map with a full-pipeline re-run (habitibility 103s/empty → 434ms/p5 1.00); futility skips already made un-mapped zero-hits fast. Still open: out-of-map misspellings (prefix rescue rejected — it gives gibberish results; a proper edit-distance/vector fallback needs the remote vector channel, NS-22/NS-27) and vector zero-hit fallback.
**Severity: High · Complexity: Medium · Break-risk: Low** (additive last-resort recall; fires only when current recall found nothing)
Every recall channel is exact-match: `tokenize` is exact chars ([search-text.ts:30](apps/api/src/services/search-text.ts)); lexical recall is `instr(lower(chunk_text), term)` substring ([search-lexical-sql.ts:16](apps/api/src/services/search-lexical-sql.ts)); FTS is quoted exact tokens on `unicode61` with **no stemming and no prefix operator** ([migrations/0008](apps/api/migrations/0008_search_fts.sql): `tokenize = 'unicode61'`; [search-text.ts:82](apps/api/src/services/search-text.ts) `ftsQuote` emits `"..."` phrases only); and the one fuzzy channel — vector search — is **hard-disabled for 1–2-token queries** (`shouldSkipVectorSearch`: `if (tokenCount <= 2) return true`). The zero-hit rescue paths reuse the same misspelled terms. **Measured:** `habitibility`/`harrassment` → 0 results + ~28s.
**Direction:** (1) last-resort FTS prefix recall (`token*`) for ≥5-char tokens when primary recall is empty; (2) a small domain spell-map (habitability, harassment, eviction, tenant, landlord, nuisance…) applied only on zero-hit; (3) run vector search as a zero-hit fallback regardless of token count. All three are additive: they only run when today's answer is "nothing."

### NS-02 · The zero-hit / broad-token fallback chain costs 25–28s — futility is never detected early — ✅ RESOLVED via NS-29/NS-30/NS-30c
**Status:** futility is now detected early everywhere: FTS-proven emptiness skips the ladder (~20-60ms); probe-matched sparse queries fetch their rows from the index in scan-parity mode instead of scanning (30s → 167ms); section references short-circuit. The only remaining scan class is multi-term curated families (mold, 40-80s) — blocked on the FTS index rebuild (title/author columns; migration = user ops).
**Severity: High · Complexity: Medium · Break-risk: Medium** (touches orchestration control flow; golden must stay byte-identical)
Zero-hit and broad single-token queries fall through the full serial fallback ladder in `runSearchInternal` ([search.ts](apps/api/src/services/search.ts)) — scoped recall → unscoped lexical over 667k chunks → whole-word variant → decision-scoped candidates → issue-family fallbacks → relaxed recovery — each stage running expensive D1 scans after the previous found nothing. A query that will end with 0 results does ~300× the work of a hit. (Sub-audit will attach the stage-by-stage breakdown; see NS-slow-path addendum.)
**Direction:** early-futility detection (if primary + first fallback both return 0 rows for a multi-channel query, skip the remaining ladder), plus a cheap corpus-presence probe (single FTS `token* OR token*` existence check) to classify "this term simply doesn't exist" before scanning.

### NS-03 · Explicit phrase intent is ignored: quotes are stripped and the `exact_phrase` path is unreachable — ✅ RESOLVED (538835b)
**Status:** wholly-quoted queries (straight or curly quotes, 2+ tokens) now upgrade keyword→exact_phrase inside runSearchInternal (public + debug production-parity mode). Eval quiet_enjoyment_quoted p5 0.20 → 1.00 at 2.4s. Also fixed the duplicate-result-row bug this exposed (diversify allowed 2 chunks/doc for exact_phrase; the decision-layer overlay collapsed them into identical rows) and re-pinned the two goldens that had enshrined the duplicates (backfill docs D1-verified topical). Mixed quoted/unquoted spans still keyword — mandatory-term handling remains future work.
**Severity: High · Complexity: Low-Medium · Break-risk: Low** (new opt-in path; unquoted queries untouched)
Legal users (trained on Westlaw/Lexis) quote phrases to mean exact match. Here `tokenize` splits on `[^a-z0-9_:-]` and `ftsQuote` strips punctuation — quotes vanish before any stage sees them ([search-text.ts:30,82](apps/api/src/services/search-text.ts)). The schema even defines an `exact_phrase` queryType with real scoring differences, but the public route hardcodes `queryType:"keyword"` ([search.ts:1515](apps/api/src/services/search.ts) area) — it is dead surface for users.
**Direction:** detect `"..."` spans at entry; route them as mandatory FTS phrase terms (`"quiet enjoyment"` is already valid FTS5 syntax) + a hard containment filter on candidates; expose via the existing `exact_phrase` type. Golden-verify unquoted queries unchanged.

### NS-04 · The phrase engine has hard entry cliffs: <2 or >6 meaningful tokens = no phrase understanding at all — ✅ RESOLVED for the user-query channel (edd8ebc)
**Status:** selectivePhraseConceptGroups keeps the 6 longest tokens for 7+-token USER queries (retrieval-expansion channel deliberately keeps the cliff — golden-pinned no-op); relaxed AND tiers (4→3 most selective groups) rescue AND-empty long queries before any scan. <2-token side is NS-30 (done). Also fixed en route: searchFtsAvailable no longer flips permanently on transient errors (rank-flapping root cause).
**Severity: High · Complexity: Low · Break-risk: Medium** (changes which queries enter the phrase path; golden + phrase suite must pass)
`phraseConceptGroups` returns `[]` unless meaningful tokens (≥3 chars, non-stopword) number 2–6 ([search-concepts.ts:84-89](apps/api/src/services/search-concepts.ts)). Below/above that: no phrase-FTS recall, no concept coverage, no proximity boosts — the query silently degrades to bag-of-substrings. A 7-word phrase ("can landlord raise the rent twice in one year") loses ALL phrase treatment while a 6-word one gets it. `meaningfulPhraseTokens` computes 8 tokens then discards the run when >6.
**Direction:** instead of hard-failing >6, select the 6 most selective tokens (longest/rarest — corpus DF if available, length as proxy); for single-token queries fall through to concept-variant OR-expansion rather than nothing.

### NS-05 · ANY structured filter disables phrase-FTS candidate search — ✅ RESOLVED (16df11e)
**Status:** the !activeStructuredKinds gate is gone; phrase-FTS intersects the filter scope via the WHERE it already carries. "quiet enjoyment" + judge went from an arbitrary 96-of-233-doc slice (2 on-topic results) to the full filter scope (p5 1.00 at ~380ms); new filtered_phrase_judge eval entry pins it.
**Severity: High · Complexity: Medium · Break-risk: Medium** (recall-path change for filtered queries; needs golden + judge-filter fixtures)
`phraseFtsCandidateSearch` requires `!activeStructuredKinds.length` ([search.ts:156-159](apps/api/src/services/search.ts)), and `activeStructuredFilterKinds` counts judge, index_code, rules_section, ordinance_section, party_name, AND date ranges ([search-query-analysis.ts:1739-1751](apps/api/src/services/search-query-analysis.ts)). So "breach of quiet enjoyment" + a judge filter (a bread-and-butter legal query) loses the phrase-optimized recall path entirely and falls back to scoped substring matching.
**Direction:** allow phrase-FTS candidates under filters by intersecting the FTS hits with the filter scope (the FTS table carries document_id; the scope is already computed) instead of abandoning the channel.

### NS-06 · Core rent-board topics have no synonym coverage — recall quality is a lexicon lottery — ◐ DEPRIORITIZED after measurement (07572c0)
**Status:** both uncovered-topic eval entries (security_deposit, ellis_act) score p5 1.00 after the Phase 1/2 recall fixes — the FTS phrase path handles un-lexiconed topics well. Curated family expansion remains optional enrichment; add judged queries for a topic first if it underperforms (relocation, subletting, passthroughs are candidates).
**Severity: High · Complexity: Medium (data work, not code) · Break-risk: Low-Medium** (additive lexicon entries; each family golden-checked)
The concept lexicon is 33 variant rules + 28 curated families, skewed to physical habitability (heat/leak/mold/pests). Verified absent: **Ellis Act (0 mentions in the search layer), security deposits, rent increase/banked/annual limit, passthroughs (O&M/bond/utility), relocation payments, condo conversion, demolition, subletting** — "security deposit" and "rent increase" appear only inside drift-EXCLUSION regexes ([search-query-classification.ts:288-292,612-620](apps/api/src/services/search-query-classification.ts)). Queries on the eviction/money half of rent law get raw substring matching with the semantic channel off (≤2 tokens) — e.g. "ellis act", "security deposit interest", "subletting without consent" (no verb stemming: "subletting" cannot match "sublet"/"sublease" — `tokenSurfaceVariants` handles only s/es/ies).
**Direction:** seed new families/concept groups from the index-code catalog descriptions (the G-code taxonomy already enumerates these topics and is parsed at [search-query-analysis.ts:196-225](apps/api/src/services/search-query-analysis.ts)); add one family at a time with golden + a new fixture query each.

### NS-07 · Natural-language questions lose their distinguishing constraints — ✅ RESOLVED (edd8ebc; lexicon topic remains NS-06)
**Status:** stopwords extended with interrogatives/modals/auxiliaries/pronouns (golden-collision-checked); combined with NS-04 tiers the NL benchmark went 43-67s/p5 0.00 → ~200-400ms/p5 0.60/MRR 1.00. Positional token retention in meaningfulLexicalTokens and whole-sentence variant slots remain open (lower value now); the rent-increase topic entry stays with NS-06.
**Severity: High · Complexity: Medium · Break-risk: Medium** (touches term selection for long queries)
Measured: the NL form of a query times out into the slow path while its keyword form answers instantly. Mechanics: the stopword list is 34 words — interrogatives/modals/possessives ("what/does/should/about/without/can/my") mostly survive as junk `instr` terms; `meaningfulLexicalTokens` keeps the **first 8 by position** ([search-text.ts:74-80](apps/api/src/services/search-text.ts)) so late-sentence constraints fall off; `lexicalTerms` prepends unsatisfiable whole-sentence + dash-joined variants that waste term slots ([search-concepts.ts:22-33](apps/api/src/services/search-concepts.ts)); and >6 meaningful tokens kills the phrase engine (NS-04). "…twice in one year" → the anniversary-date/banked-increase concept exists in scoring but there is **no rent-increase topic** to expand into it.
**Direction:** extend stopwords with interrogatives/modals; rank kept tokens by selectivity not position; drop whole-sentence variants for >4-token queries; add the rent-increase topic (NS-06).

---

## 🟠 MEDIUM severity

### NS-08 · Citation-shaped queries get no citation treatment — and numeric refs are truncated — ✅ RESOLVED for section refs (236702f)
**Status:** dotted refs (37.9(a)(2), 1942.4) are detected at entry and become mandatory FTS adjacent-token phrase arms: 33s scan → 1.4s with the heaviest-citing decisions on top (section_reference eval entry p5 1.00); nonexistent refs fast-empty. Case-citation lookups (T240553) already worked (~45ms). Remaining from the original finding: free-text ref → facet-table augmentation (optional enrichment; the FTS phrase already achieves exact matching).
**Severity: Medium-High · Complexity: Medium · Break-risk: Low-Medium** (additive detection at entry)
"37.9(a)(2)" tokenizes to `["37"]` (sub-tokens <2 chars dropped) → substring noise matching "537"/"1937"; "1942.4" → `["1942"]`, making 1942.4 (habitability) and 1942.5 (retaliation) near-identical to the ranker ([search-query-analysis.ts:613-644](apps/api/src/services/search-query-analysis.ts)). The `citation_lookup`/`rules_ordinance` queryTypes exist but only re-weight scoring, are unreachable from `/search` (always "keyword"), and nothing parses a free-text section ref into the facet tables that serve explicit filters ([search-query-analysis.ts:1701-1709](apps/api/src/services/search-query-analysis.ts); single hardcoded 37.9/37.10B hint at :1949).
**Direction:** regex-detect `§?\d+\.\d+[A-Za-z]?(\([a-z0-9]+\))*` at entry (reuse `extractCatalogReferenceCitations`), keep the dotted token whole as a mandatory term, and augment recall via the rules/ordinance facet lookup.

### NS-09 · Unanchored regex alternations mis-route queries into the wrong issue scope (with vector off) — ✅ RESOLVED (5e6f5f5)
**Status:** 40 alternation literals anchored via codemod (/\\bA|B|C\\b/ → /\\b(?:A|B|C\\b)/, end-behavior preserved per alternative); source-guard test added to the CI glob. "theater" no longer triggers heat scope.
**Severity: Medium · Complexity: Low · Break-risk: Low** (pure regex-anchoring fix; behavior only changes for the mis-matched words)
`inferIssueTerms` uses `/\bheat|heating|…|hot water\b/` — middle alternatives lack word boundaries, so "t**heater**" matches `heater`, "b**leak**" matches `leak` ([search-query-analysis.ts:741,744,747,750,789](apps/api/src/services/search-query-analysis.ts); same idiom in QUERY_TOPIC_PATTERNS [search-query-classification.ts:24,28](apps/api/src/services/search-query-classification.ts)). Consequence: issueTerms>0 → issue-guided recall scoped to the wrong topic's documents AND vector search disabled (≤12-token issue-guided skip). "noise from theater next door" gets heat-scoped, vector-less recall.
**Direction:** anchor as `\b(?:heat|heating|…)\b` across the ~20 affected regexes; add a source-guard test asserting no unanchored alternation in these files.

### NS-10 · Vector recall is a 25-chunk afterthought, and "vector-first" topics can't reach it
**Severity: Medium-High · Complexity: Medium · Break-risk: Medium** (recall-mix change; golden-sensitive)
`topK = Math.min(25, …)` caps semantic recall at 25 chunks over 667k ([search-fts.ts:1322](apps/api/src/services/search-fts.ts)). The `VECTOR_FIRST` topics (harassment/buyout/capital improvement) deliberately skip lexical when scope is empty — but `shouldSkipVectorSearch` returns true for ≤2 tokens BEFORE the vector-first check at ≤3, so bare "harassment"/"buyout" gets **neither** channel properly. And for ≤12-token issue-guided queries vector is skipped wholesale, so hand lexicons are the only synonym source exactly where users need semantic help.
**Direction:** hoist the vector-first check above the ≤2-token skip; raise topK (≥100) on vector-first and zero-hit paths; treat "no issue terms + no curated family + ≤2 tokens" as vector-eligible instead of vector-skipped.

### NS-11 · The keyword-family + judge "universe" is the 200 newest docs, not the 200 best
**Severity: Medium · Complexity: Medium · Break-risk: Medium** (changes which docs are reachable for family+judge queries)
`bypassScopedKeywordRecall` builds its re-rank universe ordered by trusted-tier then `decision_date DESC`, capped at `KEYWORD_RECALL_UNIVERSE_MAX = 200` ([search-fts.ts:1416-1423](apps/api/src/services/search-fts.ts), [search-scoring.ts:153](apps/api/src/services/search-scoring.ts)). A prolific judge's older mold/infestation decisions are unreachable for "infestation + Judge X" regardless of match strength — recency masquerading as relevance.
**Direction:** pre-filter the universe with one cheap lexical EXISTS before the recency cut, or scale the cap when a judge filter already shrinks the pool.

### NS-12 · 1,084 documents (7.7% of the corpus) are staged-invisible to every search
**Severity: Medium (potentially High if real cases are stuck) · Complexity: Low (triage) · Break-risk: Low**
`searchable_at IS NULL AND rejected_at IS NULL` = 1,084 docs users can never retrieve (measured). If even a fraction are real decisions awaiting routine QC, "the best case for this search" may simply not be in the searchable set — no ranking fix can surface it.
**Direction:** triage the staged pile (the admin QC tooling + reviewer-readiness reports already exist); make "staged >N days" an operational alert; consider a reviewer sweep for auto-approvable items.

---

## 🟢 LOW severity (quality-of-life / verification)

### NS-13 · The golden net enshrines today's behavior and covers none of the failure classes above — ✅ RESOLVED (0f706c6)
**Status:** judged eval harness landed: 13 queries across the failure classes (provenance-tagged judgments), P@5/MRR per-query regression gate vs committed baseline (`pnpm test:search-eval`, UPDATE_SEARCH_EVAL_BASELINE=1 to re-baseline deliberately), latencyBudgetMs enforcement as perf fixes land. Pre-fix baseline: mean P@5 0.567 / MRR 0.750.
**Severity: Low (meta, but it gates everything else) · Complexity: Low-Medium · Break-risk: Low** (test-only)
All 27 golden queries are covered-topic keyword/phrase queries. Zero coverage: citations, NL questions, misspellings, quoted phrases, date/party filters. And byte-identity testing means the net *preserves* today's suboptimal orderings — there is no graded relevance measurement at all (the r60 goldset machinery was purged with the experiment cluster).
**Direction:** add a judged eval set (~50–100 queries across the classes above, graded 0-2 per doc), compute P@5/MRR in a CI-runnable harness (skip-guarded live suite, like the golden net), and extend the golden fixture with the new classes as fixes land — each NS fix should add its regression query.

### NS-14 · Tokens 7–8 are computed then silently discarded; token selection is positional
**Severity: Low · Complexity: Low · Break-risk: Low** — covered by NS-04/NS-07 mechanics; listed separately because the fix (selectivity-ranked token retention in `meaningfulLexicalTokens`/`meaningfulPhraseTokens`) is small and self-contained.

---

---

## Sub-audit: ranking pipeline (caps · fusion · guards · assembly)

### NS-15 · Doc-constant field weights flood the chunk recall caps — the best-content document never gets fetched
**Severity: High · Complexity: Medium · Break-risk: Medium**
The lexical rank SQL gives **every chunk** of a document +2.4 (title) / +2.0 (citation) / +1.9 (author) per term, body text only 1.0 ([search-lexical-sql.ts:40-44](apps/api/src/services/search-lexical-sql.ts)); recall orders by this and cuts at `lexicalSearchLimit` as low as **48–96** for issue classes ([search-fts.ts:1023,1060](apps/api/src/services/search-fts.ts); [search-query-analysis.ts:2073-2096](apps/api/src/services/search-query-analysis.ts)). Example: "mold habitability" + judge filter (cap 48) — a decision merely *titled* "Mold at 123 Main St" pushes ALL its chunks above every 1.0-ranked body chunk and exhausts the cap; the decision with the detailed mold findings under an unrelated title is never fetched, and **no later stage can recover it**.
**Direction:** per-document row cap in the recall SQL (window function), or apply the title/citation/author bonus to only one chunk per document.

### NS-16 · Fusion is incoherent: SQL rank weights never reach final scoring; vector is a near-binary +0.15
**Severity: High · Complexity: Medium · Break-risk: HIGH** (the most-tuned constants in the ranker; requires golden + a judged eval set — see NS-13)
The final combination ([search-scoring.ts:1900-1909](apps/api/src/services/search-scoring.ts)): `rerank = lexical*0.42 + vectorScore*0.23 + exactPhraseBoost + citationBoost + metadataBoost + sectionBoost + partyNameBoost + judgeNameBoost + trustTierBoost`. Two structural problems. (a) The 2.4/2.0/1.9/1.4/1.0 SQL field weights are used ONLY to pick which rows survive the recall LIMIT — `scoreRow` re-scores field-agnostically, so recall ordering and final scoring systematically disagree. (b) `vectorScore` is the raw clamped cosine; bge-base cosines live in ~0.55–0.9, so the vector term contributes 0.127–0.207 — a differentiation spread of ~0.08, smaller than a single sectionBoost — i.e., in practice "**+0.15 if the chunk was in top-25, else 0**", not a ranking signal. Every vector threshold in the codebase (0.16/0.18/0.2/0.3/0.45 gates) sits **below the bge floor**, so they all degenerate to "was retrieved at all."
**Direction:** normalize vector scores per-query (min-max over the candidate set, or `(s-0.55)/0.35`) or move to rank-based fusion (RRF); then re-derive the gate constants. Do this only WITH the judged eval set in place.

### NS-17 · Hard guards eliminate relevant rows instead of demoting them (the "every token in one chunk" rule) — ◐ INCREMENT A LANDED (4f644cb)
**Status:** increment A landed: the per-chunk phrase-concept hard-eliminate is gone (the -0.28 undercoverage penalty already demoted the same rows — golden 27/27 byte-identical, so the elimination only destroyed recall depth). Still open: the literalKeywordQuery all-tokens-in-one-chunk gate ("estoppel certificate sublease" class), topic literal-word gates, and the vector-threshold guards (NS-36) that block the multi-token vector-first class.
**Severity: High · Complexity: Low · Break-risk: Medium**
`rowMatchesQueryGuard` ([search-scoring.ts:457-484](apps/api/src/services/search-scoring.ts), applied at [search.ts:443](apps/api/src/services/search.ts) and again at :4003) **eliminates** rows: the `literalKeywordQuery` branch requires EVERY query token whole-word in ONE chunk ("estoppel certificate sublease" kills the leading authority whose chunk says "estoppel letter for the sublet unit" — no stemming, so "sublease"≠"subleasing"); topic branches are literal-word gates ("ant/ants" must appear literally). `phraseConceptGuardPasses` (:439-455) hard-drops chunks matching <2 concept groups **per chunk, before document aggregation** — a document covering "quiet enjoyment" in one chunk and "construction noise" in another is eliminated wholesale for the combined query — AND the same condition already carries a −0.28 score penalty (:2104-2107), so it's redundant elimination on top of demotion. This is the single most direct "wrong court cases" mechanism for multi-concept phrases.
**Direction:** convert eliminations to demotions (require ⌈n/2⌉ tokens; rely on the existing penalties); evaluate phrase-concept coverage across a document's candidate chunks, not per chunk.

### NS-18 · Document-block presentation pushes weak chunks of "wide" documents above better passages
**Severity: Medium-High · Complexity: Medium · Break-risk: Medium-High** (doc-block ordering is the product's presentation contract)
`orderDecisionFirst` sorts document groups by docScore (top chunk + 0.18/0.08 for #2/#3 + up to +0.14 for sheer chunk count + layer boosts) then flattens whole groups ([search-scoring.ts:3810-3816,3444-3455](apps/api/src/services/search-scoring.ts)) — every kept chunk of doc A precedes doc B's best chunk, so a 0.85-scored passage routinely sits below a wide document's ~0.3 support chunks. `diversify` then caps at 2 chunks/doc — but **1** for keyword/citation/rules/index queries — and the citation neighbor-ordinal guard (:4168-4177) can suppress the exact requested paragraph ("¶12") when its neighbor ¶11 scored higher.
**Direction:** rank docs primarily by max-chunk score (drop or shrink the count bonus); exempt exact-anchor matches from the neighbor guard.

### NS-19 · Decision-scope slice (8–14 docs) happens BEFORE the layer boosts that would reorder it
**Severity: Medium · Complexity: Low · Break-risk: Medium**
`topDecisionIds = …slice(0, decisionScopeDocumentLimit)` with limits of 8–14 for issue classes ([search.ts:1131-1134](apps/api/src/services/search.ts); [search-query-analysis.ts:2105-2130](apps/api/src/services/search-query-analysis.ts)) — but the big movers (decision-layer boosts of ±0.16 to ±0.42, [search-scoring.ts:3475-3596](apps/api/src/services/search-scoring.ts)) are computed only for docs already inside the scope. A doc ranked #13 on raw chunk scores that would win on its findings/conclusions layers is silently dropped.
**Direction:** slice 2–3× the limit into the scope; existing output caps already bound final size.

### NS-20 · Pagination changes the ranking universe; ties break on ingestion order
**Severity: Medium · Complexity: Low-Medium · Break-risk: Low-Medium**
Every candidate cap derives from `pageWindow = offset + 2×limit` ([search.ts:131](apps/api/src/services/search.ts)) — page 2 re-runs the pipeline with a larger window, changing lexical caps, vector topK, doc-scope size, and the diversify cut, so results can repeat or vanish across pages. Equal scores tie-break on `createdAt` (chunk ingestion timestamp — identical within a batch) then insertion order; re-ingesting a document reshuffles equal-score results ([search.ts:534-538,1292-1296](apps/api/src/services/search.ts)).
**Direction:** fix candidate-generation caps to constants independent of offset; add a content-stable final tiebreaker (documentId/chunkId).

### NS-21 · Vector topK is tied to pageWindow (~30 chunks corpus-wide) and skipped when FTS "has enough"
**Severity: Medium · Complexity: Low · Break-risk: Low**
`vectorSearchLimit = min(30-or-120, f(pageWindow))` → default requests see topK≈30 over 667k chunks ([search-query-analysis.ts:2097-2104](apps/api/src/services/search-query-analysis.ts)); and vector search is skipped entirely when phrase-FTS returns ≥min(max(limit,8),18) rows ([search.ts:375-386](apps/api/src/services/search.ts)) — paraphrase matches ("lift out of service" for "broken elevator") never get the chance when FTS found *any* literal evidence.
**Direction:** decouple topK from pageWindow (floor ~100); dedupe per-document before capping; downweight instead of skipping when FTS evidence exists.

---

## Sub-audit: vector/embedding path

### NS-22 · The embedding model is used without its query instruction — asymmetric model, symmetric usage
**Severity: High · Complexity: Low · Break-risk: Medium** (cosine distribution shifts; gates in NS-16 may need retouching — pair them)
`embed()` sends raw text for BOTH queries and passages ([embeddings.ts:31](apps/api/src/services/embeddings.ts); [search-fts.ts:1333](apps/api/src/services/search-fts.ts)). bge-base-en-v1.5 is an asymmetric s2p model: short queries are supposed to be prefixed with "Represent this sentence for searching relevant passages: " — without it, retrieval accuracy measurably drops. Query-side-only change; no corpus re-embed required.
**Direction:** add an `isQuery` flag to `embed()`; prefix query-side calls only; A/B against citation baselines.

### NS-23 · Chunk embeddings carry no document context; long paragraphs silently truncate at 512 tokens
**Severity: High · Complexity: Medium (requires re-embedding backfill) · Break-risk: Medium**
Chunk-side embeds are raw `chunk_text` only — title/citation/sectionLabel live in Vectorize *metadata*, never in the embedded text ([ingest.ts:159,167-173](apps/api/src/services/ingest.ts); [retrieval-vector-backfill.ts:54,75](apps/api/src/services/retrieval-vector-backfill.ts)). "The petition is granted and rent is reduced by $150" embeds with zero signal about the issue or ordinance involved. Separately, the chunker's `flush()` only fires between paragraphs — a single 4,000-char paragraph becomes one chunk ([ingest.ts:103,124-138](apps/api/src/services/ingest.ts)), and bge truncates silently at 512 tokens (~2,000 chars): the tail exists in FTS but is invisible to vector search.
**Direction:** embed `"{title} — {sectionLabel}\n{chunkText}"`; hard-split paragraphs at ~1,600 chars with overlap; flag any embed input >1,800 chars. The backfill machinery for re-embedding already exists.

### NS-24 · Lexical guards veto vector-only results — for guarded query classes the whole vector round-trip is wasted latency
**Severity: Medium-High · Complexity: Medium · Break-risk: Medium**
Vector-only candidates are hydrated, scored… then `rowMatchesQueryGuard` applies **lexical whole-word requirements** to them ([search.ts:443](apps/api/src/services/search.ts); [search-scoring.ts:457-484](apps/api/src/services/search-scoring.ts)): for ant/literal/short-query classes, a semantically-matching chunk phrased differently ("insect swarm in kitchen" for "ant infestation") is guaranteed-dropped — the embed + Vectorize call bought nothing. Vector rows are also restricted to `lexicalScopeDocumentIds` for lockout/habitability searches ([search.ts:403-405](apps/api/src/services/search.ts)), so the semantic channel cannot introduce new documents exactly where synonym breadth matters.
**Direction:** exempt rows above a (normalized) high vector bar from lexical guards — or skip vector entirely for guard-active classes and reclaim the latency.

### NS-25 · Query variants are keyword soup; the second embed slot is mostly wasted
**Severity: Medium · Complexity: Medium · Break-risk: Medium**
Two variants are embedded per request; for ~20 curated intents the "vector query" is a hand-written keyword bag ("dog dogs dog-free building no pets pet policy service animal…"), and `expandQueryForRetrieval` appends up to ~25 synonym phrases into ONE string ([search-scoring.ts:678-741,507-535](apps/api/src/services/search-scoring.ts)) — bge produces a mushy centroid for unordered bags, and max-merge admits that variant's noisy top-25 on equal footing.
**Direction:** embed the raw query (prefixed, NS-22) + at most one short natural-language reformulation; keep synonym bags for FTS/lexical only.

### NS-26 · No Vectorize metadata filtering — ineligible chunks burn the tiny topK
**Severity: Medium · Complexity: Medium (metadata backfill) · Break-risk: Low-Medium**
The Vectorize query passes only topK/namespace; corpus mode, `rejected_at`, `file_type`, jurisdiction, documentId scoping, and `active=1` are all applied **post-hoc** at hydration, silently dropping non-matching ids with no oversampling ([search-fts.ts:1341-1345,1435-1508](apps/api/src/services/search-fts.ts); [search.ts:400-405](apps/api/src/services/search.ts)). Worst case `filters.documentId`: all ~50 topK slots can go to other documents. Deactivated chunks linger in the index occupying slots (no delete path).
**Direction:** write fileType/trust-tier/active into vector metadata and push at least documentId/fileType into a Vectorize `filter`; or oversample topK when filters are active.

### NS-27 · Vector failures are invisible — and the error fallback drops the namespace — ✅ RESOLVED for rescue behavior (21ea794 bare tokens, edc4d81 multi-token via NS-36)
**Status:** the whole vector-first class now lexically rescues when the vector channel yields nothing — bare tokens AND multi-token forms (landlord harassment p5 1.00 at ~400ms). Remaining from the original finding: surfacing vector ERRORS in diagnostics and the namespace-drop on error fallback (needs the remote vector channel to verify).
**Severity: Low-Medium · Complexity: Low · Break-risk: Low**
ANY error on the namespaced Vectorize query triggers a retry with **no namespace** (cross-namespace matches can leak into scoring), then a bare catch returns `[]` while diagnostics still say `vectorQueryAttempted: true` — a dead index looks identical to "no semantic matches" ([search-fts.ts:1347-1358](apps/api/src/services/search-fts.ts)). Ingest-side, embed timeouts/upsert failures are swallowed (`catch {}`), leaving chunks permanently vector-less with no record ([ingest.ts:159-185](apps/api/src/services/ingest.ts)).
**Direction:** gate the namespace-less fallback on the specific local-proxy error; add a `vectorErrored` diagnostic; record failed chunk ids so the existing backfill can sweep them.

---

## Sub-audit: the 25–30s slow paths — stage-by-stage root cause

**The unifying finding (verified first-party):** the corpus already has the right index — the fully-populated, trigger-synced FTS5 table from migration 0008 — but the router only sends multi-token phrase queries to it. Single broad tokens, keyword families without a judge filter, and zero-hit fallbacks all fall through to an **unindexable full-corpus `instr()` scan** over both chunk tables (~667k+ rows), where the computed-rank ORDER BY means the LIMIT bounds nothing: every row is scanned and every match fully materialized (chunk text + JSON columns + a correlated trust-tier EXISTS probe per row) before the sort. In trusted-only thin-yield cases the scan runs **up to 3×** (trusted → provisional retry → whole-word rescue with 10-nested-REPLACE normalization per column per row). This is ~90–95% of each 25–30s request. The proof by contrast: "mold"+judge runs the bounded 200-doc universe path in 0.66s — the fast machinery exists, it is just gated on a judge filter.

### NS-28 · Route filterless keyword/literal recall through the existing FTS index — ◐ PARTIAL (via NS-29/NS-30)
**Status:** zero-hit and single-term classes now FTS-served (see NS-29/NS-30). Still on the scan: multi-term curated families (title/author column gap — see NS-30 status), and the phrase-eligible-but-AND-empty class (NL questions ~65s, "illegal lockout"-shaped queries) whose scan candidates are golden-pinned; those need NS-04/NS-07 query-analysis fixes (a viable AND query after selectivity-based token retention) rather than candidate adoption.
**Severity: High · Complexity: Medium · Break-risk: Medium** (substring→token-match semantics shift; the surface-variant generator already produces plural/hyphen forms, and the whole-word guard exists downstream)
The unscoped UNION-ALL `instr()` scan ([search-fts.ts:957-1117](apps/api/src/services/search-fts.ts); match/rank SQL in [search-lexical-sql.ts:16-49](apps/api/src/services/search-lexical-sql.ts)) is the destination for every slow class, while `search_chunks_fts` (1.13M rows, both populations, trigger-synced) sits unused for them. **This is the single highest-leverage performance fix in the codebase** (~90-95% of the slow classes).
**Direction:** unfiltered keyword/literal recall goes FTS-first (token OR variants, bm25-ranked, LIMIT); keep the LIKE scan only doc-scoped or as the FTS-unavailable fallback.

### NS-29 · Zero-hit queries re-prove emptiness with a 667k-row scan after FTS already returned 0 — ✅ RESOLVED (9f460fc)
**Status:** futility probe landed — before the substring scan, an OR-of-prefix-variants FTS probe (LIMIT 1, the scan's own recall shape) runs; zero matches skip the scan. Gibberish class measured 57.9s → 48ms with byte-identical (empty) output; latencyBudgetMs 5000 now enforced on zero_hit_gibberish. A first cut that ADOPTED the OR-FTS rows as candidates was reverted: it moved golden-pinned output (illegal_lockout) because bm25 order does not reproduce the scan's weighted ranking.
**Severity: High · Complexity: Low · Break-risk: Low-Medium**

### NS-30 · Single tokens are barred from FTS by the same ≥2-token gate that breaks phrase handling — ✅ RESOLVED for single-term vocabularies (d86459a, ae32a13); multi-term blocked
**Status:** single-execution-term keyword queries now recall through the FTS index with scan-parity ranking (the scan's match clause + weighted-instr rank + exact tiebreaks applied to FTS-recalled rows): "rent" 38-40s → 8-12s byte-identical, "habitibility" 103s → 20ms (futile-skip). **Multi-term curated families (mold => mold/molds/mildew) remain on the 40-80s scan**: rows matched only via title/author carry the scan's top weights (2.4/1.9) but those document-level columns are NOT in the FTS index, so parity is unreachable without an FTS index rebuild (add title/author columns — migration; mind the decoupled-migrations gotcha) or an eval-gated slate change with golden re-pinning. That follow-up is tracked under NS-31/Phase 4.
**Severity: High · Complexity: Low-Medium · Break-risk: Medium**

### NS-31 · The retry ladder can triple the full scan; the fast bounded-universe path is gated on a judge filter
**Severity: Medium-High · Complexity: Medium · Break-risk: Medium**
(a) `keywordProvisionalFallbackEligible` re-runs the entire unscoped scan against the provisional corpus whenever the trusted scan yields <~18 rows ([search.ts:330-369](apps/api/src/services/search.ts)); the whole-word rescue ([search.ts:447-479](apps/api/src/services/search.ts) → [search-fts.ts:1203-1308](apps/api/src/services/search-fts.ts)) is a third, even costlier variant. (b) The proven fast path — 200-doc bounded universe + 12-doc batched re-rank ([search.ts:160,187-247](apps/api/src/services/search.ts)) — requires `keywordFamilyRecallQuery && judge filter`; filterless family queries get no universe at all. That gate alone is the 38× "mold" delta.
**Direction:** sufficiency-check before each retry; generalize the bounded-universe path to filterless family queries with an FTS-pre-ranked universe.

### NS-32 · Correlated trust-tier EXISTS probes: an unindexable sort key + a per-row tax on every hydration
**Severity: Medium · Complexity: Medium · Break-risk: Medium** (schema change; mind the decoupled-migrations gotcha — needs the runtime safety-net pattern)
`fetchScopedDocumentIds` sorts ~13k docs by a correlated `EXISTS(retrieval_search_chunks…)` ([search-fts.ts:1403-1433](apps/api/src/services/search-fts.ts)), and the same probe rides as the `isTrustedTier` column in every hydration SELECT (e.g. :1017-1020, :1471-1474, :1589-1592) — no index can cover either.
**Direction:** materialize `documents.is_trusted` (maintained by activation writes) + composite index `(file_type, rejected_at, is_trusted, decision_date DESC, searchable_at DESC)`.

### NS-33 · Serial issue-family seed fetches + O(n×m) membership scans + double scoring
**Severity: Low-Medium · Complexity: Low · Break-risk: Low**
Up to 3 `fetchKeywordCandidateDocumentIds` seed fetches run serially per matched family ([search.ts:541-907](apps/api/src/services/search.ts)); `decisionScopeDocumentIds.includes()` runs inside row filters (O(rows×~150), [search.ts:1152-1154,1222-1226](apps/api/src/services/search.ts)); and `buildDecisionScopedCandidates` re-runs `scoreRow` on rows already scored ([search-scoring.ts:3983-4010](apps/api/src/services/search-scoring.ts)).
**Direction:** `Promise.all` the seed fetches (bounded ≤3); a `Set` for doc-id membership; memoize `scoreRow` per chunkId in context.

### NS-34 · Two documents sharing one citation string render as duplicate result rows (discovered during NS-03) — ✅ INVESTIGATED: mostly NOT duplicates; reclassified as two data-ops items
**Status (D1-verified):** the golden "twins" are original + REMAND decisions — legitimately distinct filings that SHOULD both appear (L080774 Decision + L080774/AT090249 Remand; T230207 Decision + AT240005 Remand). A presentation dedupe would hide real rulings — no code change. Two genuine data items remain, needing corpus ops (prod writes):
1. **True re-ingestion dup:** T150579 has two identical remand docs (same date, 40 chunks each, transposed titles): `doc_4a72bb4f-fbc…` ("T150579 AL150088 Remand Decision") and `doc_3d98c3ec-d98…` ("AL150088 T150579 Remand Decision") — retire one.
2. **Citation-extraction collision:** citation "316928" is shared by ELEVEN unrelated docs (L2K-era decisions with real citations in their titles — L2K1615, L2K1368, L2K2127… — plus two 2026 ordinance layout files); their citation fields need re-extraction from titles.
**Severity: Medium · Complexity: Medium · Break-risk: Medium** (data-level; fixing changes four golden pins)
Distinct from the fixed same-doc duplication: four KEYWORD goldens pin the same citation twice from different documentIds — `owner_move_in` (L080774 ×2), `capital_improvement` (T150579 ×2), `package_security` (T230207 ×2), `short_rent` (316928 ×2). These are citation-sharing document twins (re-ingestion or DECISION/APPEAL variants sharing a display citation). diversify keys per-doc caps on documentId, so both twins pass, and the user sees the same case twice.
**Direction:** either data cleanup (merge/retire twin docs) or a presentation-level citation-string dedupe in diversify/finalize (keep the higher-scored twin). Requires re-pinning those four goldens; verify the twins are actually identical documents first (they may be legitimately different filings sharing a citation label).

### NS-35 · One transient FTS error permanently degraded the whole isolate to the scan path — ✅ RESOLVED (edd8ebc)
**Severity: High (silent, isolate-wide) · Complexity: Low · Break-risk: Low** (discovered while verifying NS-04 — the root cause of golden rank flapping under load)
`ftsSearch`'s catch flipped the module-level `searchFtsAvailable` to false on ANY error — including transient D1 contention — permanently for the isolate's lifetime. Every subsequent query silently fell to the 25-50s substring scan with a *different candidate slate*, so results flapped between builds/isolates. In production this meant any D1 hiccup degraded that isolate until recycle.
**Status:** only structural errors (`no such table/module/column`) disable the flag now; transient failures return the `FTS_SEARCH_ERROR_RESULT` identity sentinel affecting that query alone, and the NS-29/NS-30 futility skips require a GENUINE zero-match (never an errored empty) before skipping the scan — a transient blip can no longer produce a wrong empty result.

### NS-36 · Vector-first guard stack collapses lexical-rescue rows and hides a 16-60s un-instrumented stage (discovered during NS-27) — ✅ RESOLVED (59fb2d4 instrumentation, edc4d81 guard fix)
**Status:** the span is instrumented (issue_seed_scope_prep, decision_scope_fallback_fetch — the crawl was the OR-parity fetch over an ultra-common token, not the seed fetches); the dead-vector kill bar landed (vector channel empty → only lexical<0.35 rows die); the rescue runs phrase-AND first. landlord_harassment: empty → p5 1.00/MRR 1.00 at ~400ms; golden re-pinned from [] to the D1-verified results.
**Severity: High (blocks the multi-token vector-first class) · Complexity: Medium · Break-risk: Medium-High** (guard semantics; ties into NS-17)
When the NS-27 rescue fed 40 valid lexical candidates for "landlord harassment" (vector channel dead), the strong-issue-evidence guards — which demand vectorScore alternatives that a dead vector channel structurally cannot produce — collapsed them to ONE result, behind a 16-60s span between `initial_scoring` and `decision_scope_fetch_start` that has NO stage instrumentation. Multi-token vector-first queries therefore stay deliberately empty (golden-pinned `[]`; `landlord_harassment` eval entry documents the failure).
**Direction:** (1) instrument the gap (rerank / issue-family seed fetches / decision-layer prep); (2) with NS-17, make vector-score guard thresholds conditional on the vector channel having actually produced signal (a dead channel cannot veto lexical evidence); (3) then re-enable the NS-27 rescue for multi-token forms and re-pin.

---

## Suggested attack order (value ÷ risk — FINAL)

**Phase 0 — measurement (start immediately, everything else calibrates against it)**
1. **NS-13**: judged eval set (~50–100 graded queries incl. misspellings/citations/NL/quoted phrases) + P@5/MRR harness. NS-16/22/24/28 explicitly need it.

**Phase 1 — the latency cliff (one coherent change-set to the recall router)**
2. **NS-29** (skip the futile scan after FTS-proven emptiness) — Low complexity, kills the 28s zero-hit class.
3. **NS-28 + NS-30** (FTS-first routing for filterless keyword/single-token recall) — kills the "rent"/"mold" 25s classes; also un-cliffs single-token phrase handling.
4. **NS-31** (retry sufficiency + generalized bounded universe) — removes the 2-3× scan multiplier.

**Phase 2 — phrase understanding (the user-visible quality core)**
5. **NS-03** (quoted phrases → exact_phrase), **NS-09** (regex anchoring), **NS-17** (phrase-guard eliminations → demotions) — all small.
6. **NS-04 + NS-07** (token cliffs + selectivity-based token retention), **NS-05** (phrase-FTS under filters).

**Phase 3 — recall breadth**
7. **NS-06** (lexicon families for eviction/money topics — data work, one family at a time).
8. **NS-01** (zero-hit rescue: prefix FTS + spell-map + vector fallback), **NS-22** (bge query prefix), **NS-27** (vector error visibility).
9. **NS-15** (recall-cap flooding), **NS-19** (pre-boost slice), **NS-21** (topK floor), **NS-12** (staged-docs triage).

**Phase 4 — deep ranking work (eval-gated)**
10. **NS-16** (fusion normalization/RRF), **NS-23** (context-enriched re-embed), **NS-24** (guard exemptions for high-vector rows), **NS-26** (Vectorize metadata filtering), **NS-08** (citation detection), **NS-10/NS-11** (vector gating, universe selection), **NS-32** (is_trusted materialization).

**Phase 5 — presentation & stability (product decisions)**
11. **NS-18** (doc-block ordering), **NS-20** (pagination-stable caps + content tiebreakers), **NS-25** (embed variant hygiene), **NS-33** (micro-perf cleanups).
