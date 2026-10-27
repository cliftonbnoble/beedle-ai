# Versioned reviewer edits (M12)

Metadata and rejection POSTs require `expectedRevision` (the positive integer returned by the reviewed detail) and a fresh UUID `mutationId`. This is a coordinated API contract change. The UI, maintained batch writers and live fixtures use it. Unversioned clients receive 400; stale edits receive 409 and must reload/review before constructing a new correction. Approval retains its existing expected-revision contract.

```json
{
  "expectedRevision": 42,
  "mutationId": "018fcbe8-f7c1-4fa2-98c0-0b72e99a67ef",
  "author_name": null,
  "confirm_required_metadata": false
}
```

Omitting a metadata field retains its value. `null` clears case number, decision date or author. `[]` clears a reference group. `false` explicitly revokes required-metadata confirmation and its timestamp. Explicit clears are recorded as reviewer overrides and survive reprocessing. A request must contain at least one field/confirmation decision; unknown keys are rejected.

Confirmation uses all three resulting reference groups, including unchanged persisted arrays. A request to confirm an incomplete record fails before writing. Confirming presence does not approve legal validity: reference validation refreshes against the current catalog, and unresolved/critical references still block approval under the existing policy. A material edit, override or validation change revokes previous approval/confirmation through the existing triggers. Confirmation, when explicitly requested, is applied after those triggers in the same transaction. A true unchanged metadata submission can retain confirmation. Rejection clears confirmation, approval and searchability atomically.

The client's expected revision guards the complete transaction: resulting metadata, reviewer overrides, reference replacements, catalog guard, confirmation and retry receipt. The existing 90-statement review limit applies to the receipt too; oversized reference updates fail before writes. Use staged maintenance for such documents. Revisions are opaque monotonic evidence versions; triggers can advance them multiple times in one transaction. Do not assume `old + 1`.

Migration `0029_reviewer_mutations.sql` stores one receipt/change record per document/mutation UUID, including action, request digest, expected/applied revisions, timestamp, and before/after metadata/review state. It contains no source document text. `actor_id` is explicitly NULL: this workflow does not invent a reviewer identity. Authenticated actor attribution, authorization and identity-based retention remain H01. Database backup/access/retention policy must cover this review history; deleting a document cascades its receipts.

Retries must preserve the original UUID, expected revision and payload. Identical concurrent deliveries converge on one receipt; a different payload/action using the same UUID receives 409. After an ambiguous acknowledgment the service reads the committed receipt before deciding whether the transaction failed. Replaying a receipt never reapplies its edits, including after a later reviewer change. The response's `mutationReceipt.appliedRevision` describes the historical operation; metadata detail is a freshly fenced current read and may have a later `documentRevision`. If that read races another edit it can return 409 even though the receipt is committed. Retry with the original operation or reload; never infer rollback from a missing response. The UI disables further writes after an uncertain/error result until the selected document is reloaded.

Offline inference/remediation writers carry the revision captured in their candidate snapshot. They refuse legacy reports without a revision; regenerate/review those reports. They must not fetch a newer revision merely to authorize an older computed correction. HTTP-based operators derive their mutation from the inspected detail and retain its revision. Persist the exact mutation payload/UUID in any operator workflow that retries across process restarts.

## Rollout and rollback

Back up and rehearse migration 0029 in preview, then release the API and maintained clients together. No existing document is rewritten by the migration. Old metadata clients fail closed, so avoid mixed client/API versions. Real D1/workerd checks used a disposable local database and synthetic providers only; no production writes or paid provider calls were made.

Prefer a forward correction. Keep the receipt table/history when rolling back binaries and do not restore a writer that ignores expected revisions or treats explicit null as retain. A failed review batch rolls back metadata, reference rows, guards and receipt together. Do not independently restore receipt history without the matching document snapshot: doing so can misrepresent whether a mutation was applied. M26 owns consistent recovery of all database state.

Verification: nullable clears and override preservation; retained-array confirmation; false/omission/no-op behavior; missing groups and unsafe references; stale forms and commit races; duplicate deliveries and UUID collisions; transaction failure and lost acknowledgment; HTTP validation/rejection; real local workerd/D1 concurrent 200/409, replay and injected rollback. Full API behavioral/source and web checks, workspace typechecks and Worker dry run accompany this commit. The general utility suite has one independently reproduced pre-existing inventory failure at baseline `fbb27ad`: `vector-generation-shadow.mjs` has no inventory alias/support classification. The other 37 utility checks pass; M12 does not change that script or the inventory policy.
