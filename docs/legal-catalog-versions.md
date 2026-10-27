# Retained legal catalog publication (H16)

Migration `0019_legal_catalog_versions.sql` captures the existing index-code, ordinance, rules, crosswalk and source tables into a durable `legacy` catalog. Existing document validation is marked `unverified`: the migration does not claim old links/issues were checked against that captured catalog. New replacement catalogs stage immutable JSON rows, verify per-table counts and SHA-256 manifests, then replace all five live projections and advance the active version/epoch in one guarded D1 transaction. Readers see a complete projection. Competing rebuilds cannot overwrite a newer publication.

The current rebuild endpoint remains `POST /admin/references/rebuild`. Its existing payload is preserved, and its result adds `publication.version`, `publication.epoch` and the previous version when changed. A replacement must contain nonempty index, ordinance and rules sets and source descriptions; supplied expected-section coverage must be met. Existing exact-citation normalization/deduplication remains in effect, with collision diagnostics returned for review. Imported payload hashes describe the submitted dataset, not the authenticity, applicability or effective edition of original law. M18 remains separate.

Staging writes at most 50 statements per batch. Each table is limited to 10,000 rows and 8 MiB of serialized manifest content; the HTTP rebuild payload retains the existing 1 MiB JSON envelope. The durable ledger retains incomplete/failed stages for diagnosis. No stage failure, timeout or failed projection transaction requires restoring an in-memory copy. The active catalog and prior versions remain intact. If the final acknowledgment is lost, retrying an identical currently active payload is a no-op and does not invalidate approval again.

## Inspection and rollback

`GET /admin/references/versions` returns the current version/epoch and the newest 100 version records with state, payload hash, manifests, creation/publication time and failure category. The inspection, citation verification and rules inventory APIs also report catalog version/epoch. Multi-query readers return a conflict if publication changes during the read; document-validation lookup maps read one immutable catalog snapshot.

To restore a previously published version:

```http
POST /admin/references/restore
Content-Type: application/json

{"versionId":"VERSION_FROM_INSPECTION","expectedEpoch":3}
```

The request must use the current inspected epoch. The service verifies retained content, restores the complete projection atomically, records a publication event and increases the epoch. It never deletes newer catalog content. A concurrent publication returns 409. The `legacy` snapshot can also be restored; its observed stored-row manifest is sealed before replacement, without inventing original-file provenance. General pruning/maintenance remains outside this High finding.

Public provision links resolve immutable row IDs from previously published versions. Existing drafts keep access to their exact captured provision, while new research uses the current catalog. Staging rows are not exposed through that endpoint. Revision mismatches still return 409.

## Document validation and approval

Links/issues record the catalog that produced them. A catalog switch immediately makes approval/trusted retrieval ineligible when the document's validation version differs from the current catalog; it does not rewrite the entire document corpus or silently claim validation is current. List/detail/approval share the `reference_catalog_changed` blocker, and approval/activation recheck the catalog inside their write guard. Historical approval timestamps remain visible until explicit refresh, but cannot confer current trust.

`POST /admin/references/backfill` deliberately refreshes a selected page using one catalog snapshot. Each document replacement commits with its inspected document revision and current-catalog guard, retains the previous links/issues in `document_reference_validation_history`, and clears obsolete confirmation/approval. A concurrent edit or catalog switch rejects that document's transaction. Earlier successfully refreshed documents remain recorded; this is not an all-corpus transaction. Review the resulting issues, confirm metadata and approve individually. Metadata correction also records validation history and refreshes against the current catalog.

Reprocess/restore generations carry the validation catalog in their snapshots. A generation staged under an older catalog can be retained/restored, but stays ineligible for trust until refreshed. Pre-H16 generation manifest format 1 remains verifiable/restorable; its missing catalog provenance is explicitly `unverified`. New format 2 includes catalog columns in reference manifests.

## Release procedure and limits

Stop old catalog writers/rebuilds and pause affected consumers before migration; an old application cannot honor these version guards. Export D1 and the current reference import artifacts first. Rehearse migration and restore with an isolated copy. The migration's snapshot must be taken while catalog writes are quiescent. Direct `INSERT`, `UPDATE` and `DELETE` against live `legal_*` projections now fail outside the guarded publication transaction. Legacy scripts that mutate those tables directly must export/review a complete replacement payload and use the rebuild endpoint; do not drop the guards to continue using them.

Inventory `unverified`/stale document validation and choose a bounded refresh/review cohort. Do not bulk stamp version IDs, copy approval timestamps or auto-approve to hide the migration hold. Exact critical references must remain independently reviewed. Source paths in imported metadata are trace descriptions, not authorization or independently verified legal editions; authentication remains H01.

Validation includes actual SQLite faults inside every staging/publication batch, concurrent publishers, lost acknowledgments and identical replay, guarded restoration, old provision links, catalog changes during validation/read/approval, transactional reference refresh failure, and pre-H16 generation compatibility. An isolated workerd/D1/R2 HTTP rehearsal passed catalog replacement, stale-approval blocking, explicit backfill, stale rollback rejection and retained restoration. No production migration or corpus-wide refresh was performed.
