# Beedle AI Companion - Reviewed Issue Backlog

**Reviewed:** 2026-06-26  
**Scope:** Current local repo plus production spot checks against `beedle-ai.pages.dev` and `beedle-api.clifton23.workers.dev`.  
**Status:** Working backlog, not an implementation plan.

This file intentionally separates **confirmed product/release issues** from the larger raw audit. Auth is acknowledged but not prioritized here because we have already agreed it is not the focus of this pass.

## Triage Principles

- Prioritize issues that affect search trust, source access, deployment safety, and corpus integrity.
- Prefer fewer, clearer issues over a giant list of overlapping symptoms.
- Keep auth/security notes visible, but do not let them obscure the current product-quality work.
- Treat low-value cleanup as parking-lot work unless it blocks confidence or speed.

## Confirmed P0 / Do Next

### REL-01 - API typecheck currently fails, and deploy CI does not run typecheck/tests

**Severity:** High  
**Status:** Fixed locally by `REL-01a` and `REL-01b`; API typecheck now passes and the deploy workflow has a minimal pre-deploy gate. Remote CI verification is pending until push.
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
**Status:** Addressed locally; production migrations now live in a manual workflow. Remote verification is pending until push.
**Evidence:** Baseline `.github/workflows/deploy-api.yml` ran `pnpm wrangler d1 migrations apply beedle --remote` before every Worker deploy.

This is now correctly targeting remote production, which fixed the previous migration gap. The remaining problem is release safety: production data migrations run with no manual approval, no backup/export step, and no separate migration workflow.

**Why it matters:** The FTS migration incident showed why large D1 data changes need a controlled path. A bad migration can affect production before we notice.

**Direction:** Move production migrations behind either:

- a protected GitHub Environment with required approval, or
- a separate manual `workflow_dispatch` migration workflow.

For large backfills, use explicit batched scripts rather than one-shot migrations.

### SRC-01 - Production source links return `404` for known search results

**Severity:** High  
**Status:** Addressed locally with a DB-text fallback when R2 objects are missing. Production verification is pending until push.
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
**Status:** Measurable local guard added for representative phrase timing. Runtime optimization remains open.
**Evidence:** `Ant infestation in the kitchen` now returns the same five citations locally and in production:

`T210489`, `T250099`, `T221447`, `S001-92T`, `T210403`

But observed runtime was still roughly:

- local: about 33 seconds
- production: about 20 seconds

**Why it matters:** Relevance is much better, but a 20-30 second search is too slow for normal user workflows.

**Direction:** Profile the phrase path for this query. Likely suspects:

- phrase/lexical search still scanning too broadly
- FTS candidate volume too high
- per-row scoring doing too much repeated work
- final decision-layer evidence fetch/rerank too expensive

Set an explicit target, e.g. common phrase searches under 3 seconds.

## Confirmed P1 / High-Value Follow-Up

### REF-01 - Citation/reference normalizers over-strip prefixes

**Severity:** High  
**Evidence:** `apps/api/src/services/legal-references.ts` uses prefix strips such as `replace(/^sec/, "")`, `replace(/^rule/, "")`, and `replace(/^ic/, "")`.

Examples from inspection:

- `sec. 10.10` can normalize incorrectly.
- `rules 8.1` can be partially stripped as `rule`.
- `ICE-12` can be over-stripped by the `ic` rule.

**Why it matters:** Citation/reference normalization is central to a legal search product. False missing/invalid references create reviewer noise and weaken trust.

**Direction:** Replace raw prefix stripping with word-boundary-aware normalization and add targeted unit tests for `sec.`, `section`, `rule`, `rules`, `IC`, `ICE`, and roman-prefix cases.

### DATA-01 - Destructive corpus writes are not consistently atomic

**Severity:** High  
**Includes old items:** `SEC-05`, `BUG-06`, part of `PERF-06`.

**Evidence:** Several flows perform multi-step writes without a transaction or `DB.batch`, including legal-reference rebuilds, ingestion/reprocess, and retrieval activation rollback paths.

**Why it matters:** A mid-run failure can leave the corpus half-rebuilt, partially reprocessed, or impossible to roll back cleanly.

**Direction:** Group destructive write sequences into atomic batches where D1 supports it. For long operations, write replacement artifacts first, verify them, then swap state.

### DATA-02 - Vector activation can mark chunks active even if vector writes fail

**Severity:** High  
**Evidence:** Retrieval activation catches vector embedding/upsert failures and can still leave database rows active/queryable.

**Why it matters:** The database can claim a chunk is searchable while Vectorize does not actually contain the vector. That creates silent recall gaps.

**Direction:** Track vector write status explicitly and only mark vector-backed chunks active once upsert succeeds. Surface failures in activation reports.

### SEARCH-02 - `search.ts` is too large and too hand-tuned

**Severity:** High  
**Status:** First local refactor complete: phrase/highlight concept variants moved into shared data and consumed by API search plus web highlighting. Broader ranking simplification remains open.
**Evidence:** `apps/api/src/services/search.ts` is roughly 10k lines, with many topic-specific predicates, seed phrases, constants, and reranking branches.

**Why it matters:** Search quality now depends on a growing web of hardcoded special cases. It is difficult to reason about, hard to tune safely, and expensive to test.

**Direction:** Move topic/concept definitions into a data-driven lexicon/config layer. Keep the ranking engine generic and covered by golden-query tests.

### SEARCH-03 - Production search and retrieval debug use different query-type paths

**Severity:** Medium-High  
**Evidence:** Production `search()` always calls `runSearchInternal(..., "keyword", false)`, while `/admin/retrieval/debug` passes through `parsed.queryType`.

**Why it matters:** Debug results can exercise ranking branches that normal users never hit. That can give us false confidence when tuning.

**Direction:** Decide whether production should infer/pass query type or whether debug should explicitly label non-production modes. The important fix is making the distinction visible and tested.

### PERF-01 - Search scoring recomputes query-derived work inside hot loops

**Severity:** High  
**Status:** Partially addressed locally: `scoreRow` now receives a per-search derived query context for repeated terms, flags, and judge references. Row-text memoization and deeper helper propagation remain open.
**Evidence:** Search scoring repeatedly normalizes text and recomputes query-derived topic flags/terms across candidate rows.

**Why it matters:** This likely contributes directly to slow phrase searches.

**Direction:** Build a per-search query context once, memoize normalized row text, and pass precomputed flags through scoring helpers.

### FACET-01 - Primary legal filters use `LIKE` against JSON text

**Severity:** High  
**Evidence:** Index-code, rules-section, and ordinance-section filters are stored as JSON text blobs and searched with `LIKE`.

**Why it matters:** These are core product facets, but the current storage shape forces scans instead of indexed lookups.

**Direction:** Normalize into indexed join tables:

- `document_index_codes(document_id, code)`
- `document_rules_sections(document_id, section)`
- `document_ordinance_sections(document_id, section)`

### ADMIN-01 - Admin ingestion list filters/sorts after SQL `LIMIT`

**Severity:** Medium-High  
**Evidence:** The admin listing fetches a limited SQL page, then applies some filters and sorts in JavaScript.

**Why it matters:** Reviewer/admin queues can show the wrong top-N and misleading counts.

**Direction:** Push important filters/sorts into SQL or materialized columns before pagination.

## Confirmed P2 / Schedule Deliberately

### INGEST-01 - Upload parsing has no practical size/decompression guard

**Severity:** Medium  
**Evidence:** Multipart upload reads the whole file into memory and base64-encodes it. DOCX parsing uses `unzipSync` with no decompressed-size cap.

**Direction:** Add max upload size, max decompressed size, and early rejection.

### LLM-01 - LLM prompt boundaries and fallback transparency need hardening

**Severity:** Medium  
**Evidence:** Retrieved corpus text and user facts are included in model context without strong data fencing. Drafting can fall back silently when LLM calls fail.

**Direction:** Fence untrusted content as data, not instructions. Surface fallback/error state to the UI.

### LLM-02 - Assistant-chat AI calls lack timeouts

**Severity:** Medium  
**Evidence:** Draft conclusions uses a timeout; assistant chat paths are inconsistent.

**Direction:** Add `AbortController` timeouts to all outbound AI/LLM fetches.

### WEB-01 - Search UI can show stale results from an older request

**Severity:** Medium  
**Evidence:** Search submission has no request epoch or abort controller.

**Direction:** Add request sequencing and ignore stale responses.

### WEB-02 - Source/detail API client helpers are not consistently schema-validated

**Severity:** Low-Medium  
**Evidence:** Some helpers cast JSON responses instead of parsing with zod.

**Direction:** Validate all backend responses that drive UI rendering.

### UI-01 - Dashboard and placeholder pages contain fake/non-functional product signals

**Severity:** Low-Medium  
**Evidence:** Dashboard contains hardcoded activity/model status; Add Decision is a visual placeholder.

**Direction:** Either wire real data or label/remove these surfaces until functional.

### REPO-01 - API package scripts and experiment files are too noisy

**Severity:** Medium  
**Evidence:** Current counts:

- `326` API npm scripts
- `244` `.mjs` files under `apps/api/scripts`
- `apps/api/reports` is about `728M`

**Direction:** Keep durable commands in `package.json`; archive old experiment/report scripts; add a reports cleanup policy.

### REPO-02 - Large static catalog data is compiled as TypeScript source

**Severity:** Low-Medium  
**Evidence:** `packages/shared/src/index-codes.ts` is several thousand lines of data.

**Direction:** Move stable catalog data to JSON or a seeded database table with typed loading.

## Deferred / Out Of Scope For This Pass

### AUTH-01 - No in-code authentication/authorization

**Status:** Confirmed but intentionally deferred.  
**Evidence:** API routing has no auth middleware; admin and mutation endpoints are registered publicly at the Worker layer.

We are not treating this as the active focus because auth is explicitly out of scope for this pass. It should come back before broader rollout.

### CORS-01 - CORS defaults to wildcard if `CORS_ALLOWED_ORIGINS` is unset

**Status:** Confirmed but lower priority while auth is deferred.

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
- Web typecheck currently passes.
- API typecheck currently fails; do not list typecheck as a non-issue.

## Suggested Sequence

1. Fix `REL-01` so the API typechecks and CI has a minimal gate.
2. Fix `SRC-01` so users can open the decisions they find.
3. Profile and reduce `SEARCH-01` latency.
4. Add release safety around `REL-02`.
5. Fix reference normalization (`REF-01`) and destructive write safety (`DATA-01`, `DATA-02`).
6. Start the search architecture simplification (`SEARCH-02`, `PERF-01`, `FACET-01`).
