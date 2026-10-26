# Retrieval activation generations (H13)

Apply migration `0016_activation_generations.sql` with this application version, after the H10–H12 migrations. The API accepts only schema version 2. Authentication remains H01 and is not supplied by an activation digest.

Preparation reads currently published, approved decisions under `reviewer-policy-1`, their exact preview text and anchors, current document/text/retrieval revisions, and the original R2 source. SHA-256 binds that source, the complete canonical plan, and its server-derived rollback targets. New intake captures the source hash when extracting; reprocessing carries it into the retained generation. Missing historical extraction hashes require reprocessing and renewed review. A reprocessed legacy source has newly observed provenance, not proof of its historical origin. Restoring a legacy snapshot without an extraction hash still requires reprocessing before activation.

The request limit is 5 documents, 128 chunks, 20,000 characters per chunk, and 900,000 UTF-8 bytes per complete plan. Use explicit chunk selections through the prepare endpoint when a document is larger. Each batch replaces the active retrieval selection for every listed document, so review omissions as well as additions. Batch-specific vector IDs are 63 ASCII bytes; Vectorize allows 64 bytes. [Vectorize limits](https://developers.cloudflare.com/vectorize/platform/limits/).

## Prepare, review, publish

Run from `apps/api`. `API_BASE_URL` defaults to `http://127.0.0.1:8787`. Choose an explicitly isolated endpoint for rehearsal. These commands do not change approval, confirmation, or document publication status.

```sh
node scripts/retrieval-activation-v2.mjs prepare --batch review-2026-10-a --documents DOC_ID --file /tmp/activation-plan.json
node scripts/retrieval-activation-v2.mjs write --file /tmp/activation-plan.json --dry-run
node scripts/retrieval-activation-v2.mjs write --file /tmp/activation-plan.json
node scripts/retrieval-activation-v2.mjs rollback --file /tmp/activation-plan.json --dry-run
node scripts/retrieval-activation-v2.mjs rollback --file /tmp/activation-plan.json
```

Preparation saves a new file exclusively and never overwrites a report. Review the source SHA-256/revision, selected text/anchors, vector namespace/mode and exact previous batch/chunk IDs. `--database-only` at preparation deliberately publishes lexical retrieval without a vector readiness requirement; use it for isolated local validation or an explicit lexical-only release.

Legacy embedding reports can be converted deliberately:

```sh
node scripts/retrieval-activation-v2.mjs convert --batch reviewed-conversion-a --legacy OLD_EMBEDDING_PAYLOAD.json --file /tmp/converted-plan.json
```

Conversion checks exact source text, chunk classification and anchors against fresh server preparation. Stale/missing evidence fails. Old caller trust labels/checksums and deletion manifests confer no authority. Review the new plan before a separate write. Historical write/debug/rollback scripts now stop immediately with these instructions, including before their old metadata mutations. Old reports and utilities remain available for inspection.

## Publication and recovery

A batch ID is immutable. An identical retry resumes its staged plan or reports its historical published/rolled-back state. Different content under that ID conflicts; old parent/child records are never replaced. Historical replay never reactivates superseded chunks.

A 15-minute owner lease fences staging status and the final transaction. A concurrent owner receives 409. After process termination, retry the identical file after lease expiry. Already acknowledged vector submissions are retained and checked again; an interrupted unacknowledged submission may be upserted again under the same batch-specific IDs. Embedding concurrency is four, vector groups are sixteen, and individual provider calls time out after 15 seconds. Lease expiry prevents a late invocation from publishing.

Vectorize upsert acknowledgments are asynchronous. The service checks stored vectors with `getByIds`, including document, plan and chunk digests plus namespace, before allowing publication. A `retryable` result with pending vectors leaves the old selection visible. Retry the identical file until visibility is verified; no provider delay is treated as success. [Vectorize client API](https://developers.cloudflare.com/vectorize/reference/client-api/).

The final D1 transaction checks ownership and every document's policy, source identity and current revisions, then inserts the complete projection and switches document pointers. Search rechecks retrieval revisions before returning results. Editing a document invalidates derived activation and approval. Source digest checks occur before provider work and immediately before publication; the application never overwrites source keys. External storage changes must be investigated, not acknowledged by modifying the plan.

Rollback takes only the exact stored plan/rollback digests. It requires the batch still be current for every document, verifies retained evidence, and atomically restores the previous selection while increasing retrieval revisions. A missing/corrupt retained projection fails closed. It retains all prior rows, vectors and manifests; there is no destructive rollback cleanup. For multi-document batches, roll back later dependent activations first. A document edit invalidates the old rollback path; reprocess/review instead of restoring stale evidence.

## Release rehearsal and existing data

Before release, export the database and retained R2 sources. Inventory active legacy `retrieval_search_chunks` and their batches; migration 0016 does not invent source hashes or adopt old trust declarations. Inconsistent legacy active rows intentionally block preparation. Reprocess affected documents through H12, inspect text/references and preserved reviewer values, confirm/approve, then prepare fresh activations. This deactivates stale retrieval through revision invalidation without deleting the historical records. Do not bulk approve or clear retained rows to bypass this gate.

Rehearse migration, real-corpus plan size, deliberate compatibility conversion, concurrent writes, delayed vector visibility and rollback in isolated D1/R2/Vectorize resources. Local automated checks cover actual SQLite transactions plus local workerd/D1/R2 HTTP publication, replay and restoration. Provider visibility is fault-injected in behavioral tests; no production Vectorize or corpus migration was performed. Garbage collection and general maintenance scheduling remain separate Medium findings.
