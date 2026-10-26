# Document publication and trust

H10 introduces migration `0013_publication_policy.sql`. This is a release-affecting migration: existing rows start in `legacy` and leave public retrieval until verified. Do not deploy this migration by itself against the current corpus. Rehearse the complete High remediation release, including H12 reprocessing, against an isolated corpus copy first.

## States and routes

Ingestion begins in `preparing`; extraction failures requiring review use `needs_review`. Only successful source storage and complete text-artifact persistence permit `ready`, with a completion timestamp and expected chunk count. A ready document is public only when `searchable_at` exists and `published_revision` equals `document_revision`. Rejected documents are never public. Human trust additionally requires confirmation, approval, and `approved_revision` equal to the current revision. Vector activation never grants publication or approval.

Search defaults to trusted documents. Published provisional documents require an explicit `trusted_plus_provisional` request with `approvedOnly: false`; the search UI provides a visible opt-in. Fallbacks retain the requested scope and final results recheck current publication and trust. Assistant retrieval requires trust. `/source/:id` and `/decisions/:id/chunks` require publication, returning 404 for unpublished records. Returned source links always pass through the source route, including records with historical direct storage URLs.

Admin inspection uses `/admin/ingestion/:id/source` and `/admin/retrieval/documents/:id/chunks`, including staged and legacy documents. **These are inspection routes, not an authentication boundary. H01 remains deferred.** This change does not revoke previously distributed public bucket URLs or configure storage access policies.

## Inventory, rehearsal, and release

Before migration, save a database backup, original-source inventory, active retrieval manifests, and this read-only inventory:

```sql
SELECT file_type, (approved_at IS NOT NULL) AS historically_approved,
       (searchable_at IS NOT NULL) AS historically_searchable,
       (rejected_at IS NOT NULL) AS rejected, COUNT(*) AS documents
FROM documents GROUP BY 1, 2, 3, 4;
SELECT d.id, d.source_r2_key, COUNT(c.id) AS stored_chunks
FROM documents d LEFT JOIN document_chunks c ON c.document_id = d.id
GROUP BY d.id ORDER BY d.id;
```

Verify every referenced source actually exists in R2; a nonempty key is not proof of an object. On the isolated copy, apply all release migrations, reprocess legacy records through H12's verified revision path, review changed extraction and citations, and approve only revisions passing H11's policy. Never backfill readiness or approval merely from historical flags, activation, or nonzero chunk counts. Hold failed and ambiguous sources for manual remediation. Compare published/trusted counts, representative search results, source downloads, and assistant evidence against the inventory and record exclusions before release.

The behavioral lifecycle matrix exercises approved but unactivated, activated but unapproved, provisional, staged, incomplete, quarantined, legacy, rejected, and stale-revision documents across the real SQL and route/service implementations. Synthetic tests do not establish real-corpus extraction quality or production migration readiness.

For rollout failure, stop writes and restore the coordinated database/source/manifest backup and previous application version. Do not run an old application against the new readiness model or partially revert columns: older code can expose rows this policy holds back. Rehearse restoration on the isolated copy. No production inventory, migration, or restoration has been executed as part of this local change.
