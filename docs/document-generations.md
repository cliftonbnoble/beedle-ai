# Reprocessing and retained generations

H12 adds migration `0015_document_generations.sql`. Reprocessing builds a complete replacement in staging tables. It retains a separately keyed source copy in R2 and records its SHA-256, extractor diagnostics/version, metadata snapshot, and ordered artifact counts/digests. Public retrieval does not read the staging tables.

After source and artifact verification, one guarded D1 transaction retains the previous projection, replaces sections/paragraphs/chunks/reference validation, updates FTS through its existing triggers, changes `active_text_generation`, deactivates previous retrieval excerpts, and persists a generation-specific vector job. A failed transaction leaves the previous visible document intact. Concurrent publishers compare the starting document revision; only one wins. Search discards evidence from a revision that changed during retrieval; multi-query document readers return 409 when their reads span a revision change.

The new generation starts ready but unpublished and unconfirmed. Reprocess/restore never inherit approval or reverse rejection. Review the new extraction and confirm metadata before H11 approval. Deliberate provisional publication remains a separate operator action under H10. Reprocessed law uploads also remain unpublished; the decision-approval endpoint cannot approve a law upload. Governing-law catalogs have their own publication workflow under H16.

## Reviewer values

Metadata edits record value, timestamp, starting revision, and `reviewer` provenance in `reviewer_overrides_json`. Reprocessing applies those values over the new extractor output, including explicit empty arrays. Existing nonempty values without a reliable extractor baseline, or values that differ from that baseline, are preserved as `legacy_preserved`. That label means the original actor is unknown, not that a reviewer approved it. The admin detail and form show this provenance. Null PATCH behavior remains M12 work; nulls that currently mean retain are not recorded as clearing a value.

The original extractor result remains in `metadata_json.extractedMetadata`. Reprocessing always rebuilds text, even when old paragraphs and chunks exist. Direct edits to visible text tables hold the document as legacy and revoke approval; maintenance scripts must use the revision workflow to establish completeness again.

## Inspection and rollback

- `GET /admin/ingestion/documents/:id/generations` returns the latest 100 generation records, digests, counts, status, and predecessor IDs.
- `POST /admin/ingestion/documents/:id/reprocess` stages and publishes a replacement projection, held for review.
- `POST /admin/ingestion/documents/:id/generations/:generationId/restore` with `{"expectedRevision": 7}` restores a retained complete generation belonging to that document. It verifies the retained source/artifact digests, clones the generation, snapshots the current projection, and performs the same guarded transaction. Approval and confirmation remain cleared. A stale revision returns 409. A retained incomplete legacy snapshot cannot be published through restore.

These remain admin inspection/mutation routes; H01 authentication is deferred. The restore route uses the existing admin write quota. No deployment or production operation was performed during this repair.

## Limits and recovery

The inline workflow accepts at most 15 MiB of source bytes (with the stricter PDF extraction limits), 1–1000 chunks, 5000 paragraphs, and 10000 rows/8 MiB of serialized data per artifact table. Larger documents require a separately designed staged maintenance job; M03 is outside this goal. Provider limits can still reject an operation; partial staging never changes the active projection. This is not a promise that every historical document fits within Worker CPU/memory limits.

Failed/abandoned stages and source copies are retained for diagnosis, as are previous generations. Automatic pruning is deliberately absent. Do not delete an R2 key while any active/retained generation references it. A lost acknowledgment may mean publication committed: inspect the document's `activeTextGeneration` and generation status before retrying. Failure cleanup never deletes a potentially committed source or marks a published generation failed.

Queue delivery occurs after the D1 transaction; a delivery failure leaves a durable queued row for the sweeper. Jobs/messages carry the text generation and old completions/retries cannot change a replacement generation's job. H14/H15 complete lease and initial-ingestion outbox guarantees. Obsolete provider vectors are ignored when their chunk IDs no longer belong to current/active database artifacts; provider garbage collection remains M04, outside this goal.

## Release rehearsal

Use the backup/inventory plan in [publication-policy.md](publication-policy.md). Start with one isolated copy of a representative document, compare extracted text/citations and preserved overrides, approve after review, and verify search/reader/source output. Reprocess it again, exercise stale-write rejection, restore a retained generation, and repeat review/approval before expanding the cohort. Keep the old source, database backup, and activation manifests. Never mass-mark legacy rows ready or approved.

Local validation includes injected failures inside every staging/publication batch, concurrent reprocess/edit races, digest corruption, extraction/storage failure, lost commit acknowledgment, reader consistency, vector generation fencing, and a real isolated Worker/D1/R2 HTTP run with 24 chunks. Real-corpus migration quality, cloud embedding delivery, and large-document runtime budgets still require deployment rehearsal.
