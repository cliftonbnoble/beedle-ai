# Corpus operations

Use synthetic fixtures for routine tests. New real-corpus intake and changes require an explicit operator-selected target and review. Authentication/H01 remains separate; this guide adds no authorization boundary.

## Current supported contracts

| Work | Prerequisites / explicit target | Postcondition | Failure and rollback |
| --- | --- | --- | --- |
| Intake / retry | Selected API and original source; [source limits](source-intake.md), [idempotency](ingestion-idempotency.md) | Matching immutable source/receipt; quarantine or staged review; successful extraction does not auto-approve | Reconcile the same request receipt; never delete a completed receipt to force ingestion |
| Reviewer metadata / approval / rejection | Selected document, captured revision, mutation ID and explicit operator decision; [session safety](reviewer-session-safety.md), [metadata](reviewer-metadata-edits.md), [validation](mutation-validation.md) | Atomic confirmed mutation or controlled conflict; provenance/publication constraints preserved | Reload and recompute on conflict; preserve source, approval and reference history |
| Search index reconciliation | Exact target API/schema, all required migrations; [search recovery](search-index-recovery.md) and [readiness](search-readiness.md) | Bounded step checkpoints, reconciled postings/facets and readiness true | Resume the same build; never fabricate ready markers or replay old unbounded migrations |
| Reference / vector backfill | Explicit API, finite limit and saved job ID; [maintenance](maintenance-jobs.md), [vector filtering](vector-filtering.md), [generation lifecycle](vector-generations.md) | Frozen selection, recorded skips/completion and verified provider visibility | Inspect/resume same job, preserve acknowledged work; cancellation is not rollback |
| Activation / reprocessing | Immutable reviewed plan and source revisions; [activation](activation-generations.md), [document generations](document-generations.md) | New generation publishes only after complete verification; prior generation retained | Use exact recorded rollback target; do not reconstruct provenance or reapprove legacy rows |
| Backup / recovery | Reviewed descriptors and matching D1/R2 capture; [recovery runbook](recovery-runbook.md) | Restore into a fresh isolated target and verify actual schema/rows/source bytes | Keep source bundle/receipt; reconcile uncertain acknowledgement before any retry |

Read-only checks against an explicitly selected local API (8787 is the README development target):

```sh
API_BASE_URL=http://127.0.0.1:8787
curl --fail --silent --show-error "$API_BASE_URL/health"
curl --fail --silent --show-error "$API_BASE_URL/admin/search-readiness"
curl --fail --silent --show-error "$API_BASE_URL/admin/release-readiness"
```

Health is liveness. Readiness can intentionally return 503 on an empty/unprepared development corpus. Inspect its component state, then use the associated bounded maintenance procedure. The release contract is the authoritative current schema/policy version; do not copy version numbers from a historical phase guide. These reads do not repair, import or approve anything.

For a snapshot audit, close/quiesce its writer or capture with the WAL-aware recovery tool first, then explicitly supply its **absolute existing path** as `D1_DB_PATH`. The script helper rejects missing paths, directories and non-SQLite files before invoking sqlite3. It never chooses the first Wrangler database, creates a guessed file or infers a cloud resource. Symlinks resolve to an existing regular SQLite file. A path check does not establish consistency or permission to edit live state.

## Legacy utilities

Historical raw SQL/loop/repair commands are retained for investigation and compatibility, not the supported production mutation path. Some predate generation, revision, receipt and legal-edition contracts. Do not use their apply flags against live D1/local serving state, and do not assume a `report:` prefix means cheap or network-free. If an investigation needs one, use a separate reviewed offline snapshot, explicit `D1_DB_PATH`, explicit API target where applicable, a before/after diff and retained source backup. Follow normal versioned API/generation workflows for corrections; a successful legacy script is not evidence of publication/retrieval correctness.

Generated reports stay under ignored `apps/api/reports`. Keep only redacted durable conclusions/fixtures in Git. No user source text should be uploaded to an external evaluation/provider without the corresponding approved workflow and budget. [Independent holdout](evaluation/independent-holdout.template.json) and [quality gates](integration-and-quality-gates.md) distinguish deterministic fixtures from real relevance acceptance.
