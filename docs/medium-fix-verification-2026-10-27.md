# Medium remediation verification — 2026-10-27

Scope: M01–M26 on `codex/audit-remediation`. Preserve one local commit per finding and the 17 completed High commits. Authentication/H01 is deferred. The user authorized Low work after this Medium verification; Low changes do not belong to this verification baseline.

This ledger records the corrected boundaries and their regression coverage. A green local fixture does not replace representative corpus testing, legal review, independent relevance judgments, remote provider verification or deployment approval.

The later [Medium and Low regression review](medium-low-regression-review-2026-10-27.md) revisits every finding after Low implementation, folds new corrections into their owning commits and records the latest verification totals. Results below describe the original Medium completion stage.

| Finding | Boundary reviewed and regression evidence |
| --- | --- |
| M01 | Expensive public/admin routes use cost policy, bounded IP controls and shared fail-closed provider budgets. `cost-control-behavior` and native malformed-route cases. Quotas do not establish identity. |
| M02 | Provider deadlines include headers, body consumption, parsing and cancellation; oversized/malformed replies degrade safely. `provider-operation-behavior`, native provider failures. |
| M03 | Maintenance freezes bounded plans and advances finite items with durable leases, revision guards and cancellation. `maintenance-jobs-behavior`; recovery holds old pending plans. |
| M04 | Vector identity includes profile, source generation/hash and chunk text; uncertain visibility never publishes. `vector-generations`, `vector-jobs`, `vector-outbox` behavior and native Queue retry. |
| M05 | Cohort/filter constraints apply before semantic top-K and versioned readiness holds dirty metadata. `vector-filter-behavior`, M25 projection-version/runtime/preflight checks. |
| M06 | Indexed candidate paths retain bounded fallbacks and explicit request work limits. `search-indexed-recall` and `search-work-budget` behavior; queryability checks. No representative latency claim. |
| M07 | Stable integer identities survive update/delete and shadow phrase cutover. `search-rowid-behavior`, source guards and native populated FTS recovery. |
| M08 | Immutable bounded result sessions bind request/scope/ranking/epoch, enforce capacity/expiry and distinguish bounded totals. `search-pagination-behavior`; recovery clears sessions and reconciles reservations. |
| M09 | Reads check migration/build readiness without bootstrap DDL or sticky isolate failures. `search-readiness-behavior`; native fresh database starts unavailable until explicit maintenance. |
| M10 | Populated index recovery freezes bounds, stamps revisions and verifies coverage before readiness. `search-index-recovery-behavior`, native lost-posting and restored-bundle rebuilds. FTS archives are not backups. |
| M11 | Low-confidence predicates retain surrounding status, type and corpus scope. `reviewer-filter-behavior` and native scoped reviewer results. |
| M12 | Revision-checked metadata/rejection receipts retain null/omitted/false semantics, atomic failure, no-op and lost-ack replay. `reviewer-metadata-behavior`, native D1 rollback and restored receipt replay. Actors remain unknown until authentication. |
| M13 | Reviewer rows use bounded materialization, revision/epoch fences and keyset pagination with exact totals. `reviewer-queue` and `reviewer-pagination-client` behavior; native rebuild. Large-corpus sorting/drain capacity still needs measurement. |
| M14 | Untrusted excerpts stay outside privileged instructions; claims require allowlisted evidence and exact supported quotes. `assistant-evidence`, `draft-evidence`, `draft-review` behavior and native unknown-ID/oversized reply cases. |
| M15 | Client/server message, history and model context budgets agree and disclose truncation. `assistant-context-behavior` and web store tests. |
| M16 | Conversation scope and accepted filters constrain retrieval; named cases and complementary passages remain explicit. `assistant-retrieval-behavior`; no exhaustive answer is inferred from unseen evidence. |
| M17 | Live UI state survives persistence failures; record revisions, receipts, tombstones and conflict copies prevent stale overwrite. Web store/session tests and actual Pages browser races. Browser storage remains device-local. |
| M18 | Immutable reference editions preserve provenance and effective-time uncertainty; v3 generation artifacts carry bindings and retain history. `reference-editions`, `generation`, `draft-law` behavior; native recovery preserves generation/reference tables. Recorded provenance is not legal certification. |
| M19 | CI requires actual Worker/D1/R2/Queue and Pages/browser fixtures; holdout evaluator requires independent labels. `independent-holdout-behavior`, native/browser suites. No human labels or fresh provider quality results were fabricated. |
| M20 | Runtime/CLI share schema/config/revision/vector readiness contract; protected deployment remains separate. `release-readiness-behavior`, native maintenance/lexical release tests, actual adapter build/browser and offline Worker bundle. Hosted protection and remote rollout were not inspected. |
| M21 | Safe bounded request/job correlation measures outcomes and reads durable health without source/query leakage. `telemetry` and `operational-monitor` behavior, native scheduled heartbeat. SLO baselines remain unmeasured. |
| M22 | Deployment-scoped ingestion UUID receipts bind fingerprints and source revisions; atomic publication survives lost acknowledgments. `ingestion-idempotency-behavior`, native request polling and recovery receipt preservation. No account scoping or automatic case merging. |
| M23 | Unsupported taxonomy abstains; ambiguous/negated/weak evidence remains unknown until domain review. `taxonomy-inference-behavior` and native reviewed/unreviewed inference. Scores remain uncalibrated. |
| M24 | CSV protects formula-like text and preserves logical records; JSON remains lossless. `spreadsheet-csv-behavior`, native export and utility worksheet checks. Real spreadsheet-client acceptance remains external. |
| M25 | Strict bounded mutation DTOs, typed errors, one captured validation plan and real calendar dates retain revision/fingerprint contracts. `mutation-contracts-behavior`, native malformed routes/date repair, version-3 vector projection/preflight checks. |
| M26 | WAL-aware sealed snapshots, explicit resource identity, actual row/schema/object hashes and fenced receipts replace implicit remote migration/heuristic repair. `recovery-bundle-behavior` and disposable native restore rehearsal. Remote restore timing/provider behavior and semantic rebuild remain release gates. |

## Verification results

The final verification covers 304 API behavior checks, 84 source guards, 38 utility checks, 10 phrase/queryability checks, 47 web checks, 15 existing native runtime/release checks plus the new native recovery rehearsal, and eight Chrome flows against actual Pages output. Workspace and integration types, the framework security-floor guard, actual Pages build and offline production Worker bundle also pass.

The dedicated queryability gate exposed stale assertions left behind by M05's new filtered vector-call contract and M06's indexed lexical delegation. Updated those checks within their owning commits, retaining behavior coverage for filters/eligibility/budgets rather than pinning the removed broad-scan SQL. M26's native rehearsal rejected an attempted combined-count shortcut; verification retains the individually checked portable queries. Added target ownership fencing before R2 copying, bounded child-process cleanup and wide-row/binary transport checks before committing M26.

Remote restore atomicity/limits/timing, legal applicability, real corpus/provider relevance, independently labeled quality, spreadsheet-client acceptance and representative SLO baselines remain documented external gates. The registry audit remains unavailable under the earlier automatic approval rejection; it was not retried or bypassed. No production writes, paid inference, push or deployment occurred.
