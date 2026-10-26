# Revision-bound approval

H11 uses `reviewer-policy-1` for both reviewer readiness and approval. It retains the existing reviewer thresholds (including the bounded clean/confirmed reference tiers), adds ingestion readiness, and checks critical citations in raw metadata even when historical reference links are absent. A warning about taxonomy remains a caution. Every actual blocker prevents approval; there is no override endpoint.

Both `POST /decisions/:id/approve` and `POST /admin/ingestion/documents/:id/approve` now require JSON:

```json
{ "expectedRevision": 7 }
```

Use `documentRevision` from the detail or list response that was reviewed. Missing/invalid revision returns 400, stale/intervening changes return 409, and current policy blockers return 422 with readiness details. A success persists the approved revision and policy version. Do not retry a conflict automatically using a freshly fetched number; review the changed evidence first. The web client and the three approval operator scripts carry the inspected/confirmed revision. Historical external clients and live fixtures using `{}` must migrate to this contract.

Migration `0014_approval_revision.sql` revokes approval and confirmation when document evidence or reference validation changes. Confirmation changes also advance the revision and revoke approval. A revision is a monotonic concurrency token, not a count of user edits: one operation can advance it several times. Publication becomes stale when evidence changes and must be re-established deliberately. Rejection clears publication and cannot race into an approved state.

Metadata replacement, reference validation, and explicit confirmation share one guarded D1 transaction. The guard trigger aborts stale writes; its temporary row is inserted and deleted inside that transaction. Operations above 90 mutation statements fail before writes and require a staged revision workflow. General nullable metadata PATCH semantics remain tracked separately under M12; this repair does not claim to implement that Medium finding.

Approval is still an operator action. Authentication, actor identity, and a separately authorized/audited exception mechanism remain deferred under H01; no exception bypass was added. Existing automation flags do not establish that a human performed a review.

Release with the H10/H12 corpus rehearsal and coordinated backup plan in [publication-policy.md](publication-policy.md). Keep the migration and application together. Rehearse blocked/stale approval, edit/reject races, transaction rollback, and reapproval on the isolated corpus before production. No production schema or data was changed during local remediation.
