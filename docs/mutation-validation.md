# Mutation input, validation plans and calendar dates (M25)

All JSON routes use the bounded body reader. Empty/malformed JSON, failed input streams and invalid UTF-8 return 400. Unsupported media types, charsets and compressed bodies return 415; JSON requires `application/json` (case-insensitive, optional UTF-8 charset). Declared and streamed limits return 413, including when stream cancellation fails. The normal envelope limit is 1 MiB; ingestion retains its separately bounded base64/multipart envelopes and 15 MiB source limit. Known JSON nulls/scalars/arrays must pass their route DTO; they cannot become a default mutation.

Bulk searchability no longer catches body-reader failures and executes a default operation. Vector probes always use the JSON reader. Reprocessing requires the explicit JSON command `{}` and rejects extra fields. Reference/vector backfill creation requires an explicit finite `limit`; a missing body or `{}` cannot create a default 500-row job. Existing step/cancel/retry commands intentionally accept their declared empty-object DTO. Taxonomy validation preserves 400/413/415 instead of flattening every failure to 400 or exposing raw errors. Unexpected errors return a generic 500; controlled missing targets return 404, stale revisions/plans/state return 409, and unavailable services remain 503.

## Bounds and compatibility

| Contract | Limit / behavior |
| --- | --- |
| Reviewer reference arrays | At most 12 values per group, 200 characters each; at most 36 total |
| Reviewer scalar metadata | Case number/author 200 characters; rejection reason 2,000; explicit null/false and omission semantics retained |
| Validation service references | At most 50 values per group, 200 characters each; 150 total |
| Catalog lookup snapshot | At most 30,000 projected active reference rows and 4 MiB of projected string bytes, checked before materializing maps |
| Catalog replacement | At most 10,000 rows per reference set/crosswalk, 200,000 characters per provision body, bounded labels/source traces/links; the 1 MiB HTTP envelope also applies |
| Backfill selection | Explicit integer limit 1–5,000; immutable finite job protocol remains M03 |
| Bulk searchability | Explicit integer limit 1–1,000; typed booleans/mode, dry-run defaults true |
| Vector probe | Chunk ID at most 200 characters, explicit query at most 2,000, topK at most 25 |
| Upload metadata | Title at most 1,000; jurisdiction/citation at most 200; existing filename/MIME and source limits retained |

Worst-case accepted reviewer reference replacements fit the existing atomic transaction ceiling, including unknown-reference issue rows, revision guard, receipt and explicit confirmation decision. Larger automatic extraction/reference work remains bounded through its staging workflow; a legacy record requiring more than the atomic review budget needs an explicit staged correction rather than partial writes.

Limits/types are checked before catalog/provider/storage work. Invalid JSON/DTOs create no documents, receipts, jobs, embeddings or publication changes. M01 request quotas and operational cost accounting still apply to rejected HTTP attempts; “no domain writes” does not mean an abusive request is free of rate/cost controls. Ingestion multipart fields must be declared, unique strings with explicit true/false, one File and matching endpoint file type. Conflicting header/body retry keys fail. File bytes are copied only after metadata validation. JSON flags no longer turn arbitrary strings into true.

Valid bounded strings retain their original whitespace so existing M12/M22 request fingerprints do not silently change. Existing completed ingestion receipts remain immutable and pollable. Payloads previously tolerated with impossible dates, extra fields, ambiguous multipart fields or values beyond the new bounds can now fail validation even on resend; use the existing receipt's status endpoint to recover a committed ingestion result. Client requests need explicit limits and valid DTOs. JSON command callers already sending `{}` retain that explicit command behavior.

## One revision-scoped reference plan

`prepareReferenceValidation` captures one immutable catalog version/registry epoch, bounded normalized maps and applicable edition/provision sets. Ingestion and reprocessing reuse it for initial validation and final link/issue statement generation. Index inference may select other codes from the already captured index map; changing the jurisdiction/date or requesting provision keys outside the captured plan requires a fresh plan. There is no isolate-global catalog cache.

The lookup makes a capacity aggregate before loading projected strings. Provision sets use bounded `json_each` lookups and preserve ambiguous/no-match behavior. A prepared 150-reference case has a fixed query count; statement building performs no additional catalog/edition queries. Metadata-only callers prepare once, and finite maintenance batches can reuse their immutable lookup maps. Transactional revision/catalog guards continue to reject changed state before replacing references; they do not mix current and historical editions. Malformed extracted reference strings remain explicit unknown issues rather than turning into valid legal bindings.

## Calendar dates and retained evidence

Input dates must be real Gregorian `YYYY-MM-DD` dates, including leap-year rules; year 0000 is rejected. Reversed search date ranges fail validation. Invalid automatically extracted dates become unknown with an explicit warning and retained `unverifiedDecisionDate` metadata/source evidence. Reprocessing an existing invalid date also leaves it unknown and retains the previous generation/provenance. It does not invent a corrected date.

Migration 0033 preserves the raw `decision_date` and adds indexed virtual `valid_decision_date`. SQLite normalizes the candidate with `+0 days` and accepts it only when its canonical value matches. SQL date filters and vector metadata use the validated projection, so invalid legacy values cannot pretend to fall within a calendar range. Search/preview output reads invalid dates as null. Reviewer detail/queue show unknown plus a retained invalid-date indication; the UI explains that the original source needs review. Saving a real date uses the existing revision/receipt protocol. Existing approvals are not silently rewritten or presented as governing-law certification.

Date filter projection version is now 3. Migration 0033 clears old vector-filter checkpoints and advances derived source revisions; new checkpoints carry an explicit `projection_version`. Runtime readiness and the M20 preflight reject older checkpoints even if source revisions match. Unchanged embeddings can be reused while metadata is resubmitted/read-back verified. Remote provider compatibility and full coverage remain M04/M05/M19 release checks.

## Rollout and rollback

**Medium implementation, material rollout risk:** apply migration 0033 before the new reader and follow M20's schema 33–33 contract. Rehearse index creation/dirty-mark timing on a representative populated preview; no production timing was measured here. Rebuild the new reviewer materialization policy using bounded steps. The generated column and new index add derived storage/work without rewriting original dates.

Quiesce older ingestion, publication, cron and Queue writers before cutover. An old binary can still write the old vector projection; a new checkpoint field is not permission to mix incompatible writers. For an existing semantic deployment, plan the transition explicitly: serve the new candidate in **lexical-only** mode with both vector readiness flags false, drain old work, finish/verify metadata synchronization under the new code, then use a separate M20-gated release to enable semantic mode. Isolated preview preparation supports rehearsal. Do not bypass preflight or set a ready marker manually to break a deployment/backfill dependency.

Prefer a forward repair. Preserve receipts, raw dates, reference history, generated schema and matching D1/R2 snapshots; disable publication/mutation before any older writer rollback. The older reader may interpret invalid dates and filter checkpoints differently and has a different reviewer policy. Do not drop the new columns/history or restore unsafe request fallbacks. Authentication and Low findings remain excluded; no remote migration, provider call, deployment or push was performed.
