# Service boundaries and refactor contract

L01 separates policy, search execution and reviewer persistence while preserving the verified Medium behavior. This is a code organization change: no schema migration, ranking adjustment, API field change, new approval rule or authentication implementation is included. Existing public service exports remain available through `search.ts`, `search-scoring.ts`, `search-query-analysis.ts` and `admin-ingestion.ts`.

## Search

| Boundary | Responsibility |
| --- | --- |
| `search.ts` | Parse the versioned shared request, establish readiness/work budget, bind immutable sessions and serialize the correct response schema. |
| `search-pipeline.ts` | Run retrieval stages, preserve literal/structured intent, account for work and assemble results. This orchestration delegates the policy and I/O below. |
| `search-filter-policy.ts` | Build publication/filter SQL and ordered parameters. Canonical judge/reference helpers and `publication-policy.ts` remain the shared authorities. |
| `search-query-analysis.ts` | Interpret query families, build request-derived context and choose adaptive recall settings. |
| `search-query-plan.ts` | Pure query expansion, lexical execution terms and vector-channel selection. |
| `search-fts.ts`, `search-indexed-lexical.ts`, `search-scope-repository.ts` | Execute bounded candidate/coverage reads and provider work using explicit `Env`/request scope. Schema readiness, pre-top-K filters and budgets remain enforced by the existing adapters. |
| `search-evidence-policy.ts` | Shared evidence/context/metadata predicates, lexical scoring and request-owned caches used by ranking and presentation. |
| `search-ranking.ts` | Score a typed row against the request context; retain all calibrated weights, raw-cosine guards, reasons and work accounting. |
| `search-snippets.ts` | Build source passages and bounded literal-aware snippets without accessing bindings. |
| `search-candidates.ts` | Order/diversify scored candidates and select authority/supporting-fact layers. |
| `search-scoring.ts` | Compatibility exports for existing callers; no duplicated policy implementation. |

Policy files receive typed values and return typed/inferred results. They have no `Env`, binding access or network calls. Request-owned caches remain on `SearchContext` or the existing request-keyed WeakMap; moving a function does not introduce a second cache or a new lifetime. Candidate ranking and excerpt selection share the same evidence predicates. Changing a policy means editing its implementation and behavioral checks, without reproducing it in SQL/read models.

The orchestration still contains the established recall/rescue sequence. Simplifying corpus-specific rules or altering stage order would change search semantics and requires separate relevance evidence. Module length alone is not a performance measurement; no latency improvement is claimed by this extraction.

## Reviewer

| Boundary | Responsibility |
| --- | --- |
| `reviewer-contracts.ts` | List input type shared by queue/readers and compatibility exports. |
| `reviewer-policy.ts` | Pure readiness, issue triage, blocked-family and conservative manual-review decisions. Both detail and materialization call these functions. |
| `reviewer-read-model.ts` | Bounded SQL hydration and row/detail assembly, with the original 25-row materialization limit, byte limits, reference counts and failure diagnostics. |
| `admin-ingestion.ts` | Revision-checked mutation/reprocessing commands and compatibility exports. |
| `reviewer-mutations.ts`, `approval-policy.ts`, `document-generations.ts` | Existing receipt, actual approval and generation publication authorities. |

A displayed reviewer-readiness/manual-candidate signal does not grant approval. The actual approval policy and immutable reference validation remain separate authorities. Family grouping used to explain unresolved references is not a replacement for identifier normalization: canonical legal-reference suffixes/Parts remain governed by the existing reference normalizer.

## Verification and future changes

Before extraction, captured 13 synthetic query/filter cases from Medium baseline `15f5ea5`: expansion, scope/parameter order, adaptive recall bounds, every score/reason, source passage and combined snippet. The parity fixture is a refactor regression pin, not an independent legal/relevance judgment. An AST comparison verified all **168 original top-level function bodies remained byte-identical** during relocation.

The behavioral boundary test rejects binding/network access in policy files and dependency cycles between the new boundaries. Existing behavior tests remain the authority for publication, budgets, cursor stability, reviewer policy and state transitions. Source guards read an explicit boundary module list so a compatibility facade no longer hides the implementation being checked.

Run API behavior/source/queryability checks, workspace/integration types, native D1/R2/Queue/recovery fixtures and the actual Pages browser flows before accepting a boundary change. The verified Medium baseline remains documented in [the review ledger](medium-fix-verification-2026-10-27.md). Use the normal [release gate](release-readiness-and-rollout.md); rollback this refactor to the previous code revision with matching release configuration. It does not require data repair. Authentication and provider/corpus quality certification remain outside this change.
