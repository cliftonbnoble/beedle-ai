# Immutable reference editions and historical context (M18)

A citation's presence in a catalog does not establish which text governed a historical event. Migration `0031_reference_editions.sql` separates that question from the active normalization catalog. It retains existing text and labels missing jurisdiction, effective interval, original-file hashes and reviewer provenance **unknown**. It does not invent a historical edition or upgrade an existing approval into a governing-law certification.

## Import and correction contract

`POST /admin/references/rebuild` accepts an optional strict `edition` descriptor. The existing normalization compiler can attach one using `LEGAL_REFERENCE_EDITION_JSON=/path/to/descriptor.json`; the file is bounded to 64 KiB. Without one, imports remain explicitly unreviewed for historical applicability.

A `reviewed_interval` requires a jurisdiction, a real calendar start date, an optional exclusive end date, three source records (`index_codes`, `rules`, `ordinance`) containing URI, SHA-256 and retrieval timestamp, and declared reviewer/time. Review metadata and source-file hashes are **importer declarations**, not an authenticated actor or independently verified original-file provenance. The server computes the normalized import-payload digest, immutable descriptor digest, retained table manifests and exact full provision-body digests itself. Source text must be reviewed against original artifacts before these declarations are used in a release.

Descriptors, correction edges and catalog text are immutable. Publication uses H16's complete-manifest and epoch guards; a failed/unpublished staged edition cannot participate in retrieval. Restoring a normalization catalog does not rewrite edition metadata or historical validation. Version listings and the reference admin view expose edition provenance.

An optional `supersedes: [earlierEditionId]` explicitly withdraws an earlier applicability assertion. It requires a reviewed interval and published targets in the same jurisdiction; duplicate targets, mismatched jurisdictions and unpublished targets fail. Edges become effective only when the correcting catalog is published. They preserve the original text/descriptor and also suppress identical-payload reimports, so retrying an old import cannot silently revive a withdrawn assertion. Source links still return the original body with a supersession notice. Use this for corrections, not ordinary amendments. To correct an old open interval, publish a corrected historical edition with its bounded interval and `supersedes`, plus the distinct later edition. Do not mutate the original interval in place. Undoing an incorrect correction requires another explicit reviewed edition, rather than silently changing history.

## Selection and unknowns

Draft requests accept `law_context` containing `jurisdiction` and `as_of_date` together, optionally an exact `edition_id`. An edition ID alone can inspect that exact candidate, with applicability still unknown. Superseded editions cannot supply fresh law evidence. Lookup uses exact jurisdiction identifiers with trimmed ASCII case folding, not inferred aliases or cross-jurisdiction matches.

Automatic selection requires one unique reviewed interval containing the requested date (`from <= date < to`, with null end open). Identical payload/descriptor imports are equivalent. Overlapping distinct editions are ambiguous; missing, unreviewed, mismatched or superseded editions produce no law for an explicit dated request. There is no fallback to current, parent or another jurisdiction's text. An explicit edition can resolve overlap only after its own jurisdiction/date are rechecked. Registry epoch fences detect changes during selection and before the final generated response, including A → B → A publication.

For compatibility, a request with no historical context can inspect current-catalog provision candidates. Their applicability remains unknown and review is required. The legacy case-assistant path labels unversioned law-document excerpts unknown; an explicit historical `law_context` uses the bounded edition-aware research path. That path also scopes similar-decision retrieval to the supplied jurisdiction.

A metadata match is not legal entailment or a determination of the legally relevant event date. Compiled intervals are operator assertions that require domain review. Rules may have transition provisions or other applicability conditions that dates alone cannot establish. No confidence score or approval here certifies governing law.

## Document validation and generations

Document-reference links retain the actual catalog used, edition/provision ID, full body SHA-256 and applicability status. With a unique known edition, rules/ordinance citation presence is checked in that historical edition, including citations absent from today's catalog. Index-code normalization remains tied to the active catalog. Unknown historical context retains a separate, explicitly unverified normalization-presence check; it does not assert historical validity. Extraction warnings use the same context-aware presence logic.

The document's `reference_catalog_version` identifies the active validation/normalization snapshot used by the pipeline, while an individual law link can identify a different historical catalog. Its recorded edition context includes the registry epoch and date basis. A decision date is only a **decision-date proxy**, not a proven legally applicable date. Reviewer date edits revalidate the resulting date in the same guarded mutation. Prior validation history and M12 receipts retain the earlier context and body bindings. A changed context revokes confirmation/approval through revision guards; detail views flag stale contexts for revalidation and provide immutable provision links.

New document-generation manifests use version 3, including the edition link fields and context. Reprocessing preserves reviewed dates and captures the next edition context. Restore keeps the exact retained binding instead of recomputing it from today's text. Versions 1 and 2 remain verifiable with their original column sets; absent edition data stays unknown. Restored documents still require review and reapproval.

## Drafts and replay

Law retrieval reads immutable catalog rows, retains full Parts/subsections, and bounds requests to six distinct provisions. Body text is excerpted at 8,000 characters; the body digest identifies the complete retained provision, while the existing excerpt digest identifies what the model received. Source URLs bind catalog, row, revision and full body hash. Ambiguous legacy URLs and mismatched revisions/hashes fail safely.

Saved draft authorities, provenance and every review export include the captured context, descriptor, body hash and edition ID. The UI displays the captured context independently of current form inputs, exposes source declarations, and keeps all claim support unverified. Opening an old link after replacement or correction returns the original text and its current supersession notice; it never silently substitutes a newer body.

## Release gates and rollback risk

This is a **large, high-risk data-model change**. Apply migration 31 before the new API. Rehearse on an isolated populated backup, retain matching D1/R2 artifacts, inspect legacy unknowns, and obtain domain review of actual jurisdictions, source hashes, intervals and corrections. Register evidenced historical editions, then use the existing bounded reference-backfill workflow; catalog publication can hold older approvals under H16 until deliberate revalidation. M13 reviewer-index readiness must also be restored after corpus changes. No real metadata was inferred or production migration/deployment performed here.

The active normalization projection remains one catalog. Historical law lookup is independent across retained editions; source-publication approval is not a legal applicability certification. Account/actor authorization remains H01's separate work. Unknown applicability remains visible and requires human review; this change does not fabricate missing provenance to clear a gate.

Rollback is additive at the schema level, but old generation writers do not understand v3 bindings. Export/rehearse the matching snapshot and disable reference mutation, reprocessing and generation restore before running an older API. Do not drop edition tables, correction edges or retained catalog rows to roll back. Prefer rolling forward; older code cannot restore a new v3 manifest safely. Revalidate edition contexts after any period of older writers. M20 gates deployment compatibility and M26 records the full recovery manifest.

## Verification

224 API behavioral tests, 84 API source checks, 46 web tests, workspace types and the production Worker dry-run pass. Seventeen edition regressions cover date/jurisdiction collisions and boundaries, overlap/explicit selection, unknown provenance, removed/parent provisions, immutable history, supersession and failed publication, identical reimports, registry races, date edits/receipts, historical citation presence, saved exports and v1/v2/v3 restore behavior. A populated migration fixture preserves old text with unknown metadata.

Disposable workerd/native D1/R2 checks exercised migration, historical/current selection, unknown-jurisdiction abstention, invalid dates, correction/replay, dated ingestion, reviewer date changes, reprocessing and restore. Chrome verified historical/current source selection, captured-context stability and expanded provenance/hash labels using synthetic editions/providers. No paid calls, real legal judgments, user tabs or production resources were involved. Independently judged real-corpus relevance and supported-provider behavior remain M19 release gates. The previously recorded utility script-inventory failure remains for M19 CI remediation.
