# Privacy-aware operational telemetry (M21)

The implementation adds application correlation and aggregate measurements. It does not deploy log settings, install a production alert destination or claim production SLOs have been met. Authentication, actor identity and access policy remain H01.

## Measurements and correlation

Each HTTP invocation generates a fresh UUID at ingress, before environment validation, quotas, body parsing, readiness and retrieval. It returns `x-request-id` and `Server-Timing: app;dur=...`; approved CORS origins can read these headers. The application never accepts a client-supplied correlation value or uses raw URLs/queries as log labels. A route is its declared template (for example `/source/:documentId`) or `unmatched`.

`operation_complete` contains the generated request ID, environment, committed release revision when available, status, server duration, query class, numeric corpus revision, fixed outcome categories, selected stage durations, bounded numeric provider-status counts, work-budget counters and aggregate binding measurements. It does not contain query text, source text, document titles, citations, filenames, storage keys, IPs, cookies, authorization headers, prompt bodies, provider payloads or arbitrary exception strings. Debug searches no longer print their question or detailed result context. Ingestion records warning **counts**. Client error bodies retain their existing controlled validation contract, but unexpected service errors are not dumped to logs.

The binding wrappers preserve original receivers, prepared statement bindings, atomic batches, sessions and returned R2 streams. They measure D1 statement calls, returned rows, reported `rows_read`, missing metadata, R2 operation latency, AI calls, vector operations/submissions and logical bounded LLM operations. SQL and bind values are never retained. `db_rows_read_reported` is a **partial** sum: D1 `first`/`raw` do not expose all metadata, and `db_meta_missing` explicitly records that gap. Embedding texts requested and texts actually submitted are separate; these counters and reserved budgets are not a provider billing receipt.

Provider categories distinguish unconfigured generation, no evidence, timeout, cancellation, 429, rejection, malformed/oversized/truncated output and network failure. Evidence-check rejection remains distinct from provider-operation failure; neither implies verified claim support. The assistant displays a fixed status label and retains its category across saved-chat reloads. Unknown upstream strings are never rendered as status text. Intentional lexical-only operation reports `semantic_disabled` without pretending the vector channel broke or calling providers. Application budget exhaustion continues to return its deliberate 429/defer behavior.

The server duration runs from handler ingress to the response being ready. It includes cold **handler/readiness work**, but not platform activation before JavaScript ingress, network transport, client JSON/rendering or completion of a streamed download. Binding durations are aggregate work and can overlap; do not add them together as a critical path. R2 `get` measures opening the object, not transferring every byte. Active uninterruptible provider calls may outlive a timeout; the snapshot reports active calls rather than claiming they stopped.

Background tasks have separate generated IDs and their parent HTTP/scheduled ID. They unwrap parent binding wrappers so work is not counted twice. Queue deliveries have distinct invocation IDs, while producer/retry/completion events share a SHA-256-derived opaque job ID from document/generation identity. Attempt outcomes are not authoritative job state: acknowledgments and concurrent consumers can race; the persisted health snapshot is authoritative. Dispatch failures retain the opaque join key, including an uncertain acknowledgment. This joins an ingestion/outbox dispatch to its Queue retries without logging the source identifiers. It is pseudonymous correlation, not account isolation or cryptographic anonymity. These are application operation summaries, not a claim that a distributed OpenTelemetry backend or provider-side trace propagation is configured.

## Sampling and platform policy

The checked-in preview/production configurations explicitly enable custom Workers Logs, disable invocation logs and native traces, and keep platform sampling at 1. Application success sampling defaults to 0.1 (`TELEMETRY_SUCCESS_SAMPLE_RATE`, either 0 or 0.000001–1); each record states its actual rate. Server errors, degraded generation/channel operations and background/job/health events remain unsampled. Quota-limited HTTP requests use the success sampling rate to avoid unbounded denial-log volume. Invalid configured sample rates are rejected by the release wrapper. The local config disables persistence and routine success logs; test fixtures explicitly retain all samples.

Cloudflare documents [custom/invocation logging and head sampling](https://developers.cloudflare.com/workers/observability/logs/workers-logs/). Disabling invocation logs avoids the default request/URL log. Platform-owned envelopes, access logs, exception metadata, retention, account limits and external sinks still need an operator privacy review. Export/redact only the required aggregate records; do not forward complete request envelopes. Additional platform sampling, loss or retention expiry invalidates the assumption that application sampling weights are the only weights. The missing-heartbeat check helps expose absent delivery; it does not prove lossless logging.

One bounded completion summary replaces many debug messages. Configured Queue batches contain one message; job transitions are logged individually for durable correlation. Stage/operation/reason labels come from fixed sets. Logging/sink/diagnostics failure does not change the application result. Observe real volume/cost before increasing success sampling; do not enable document/query logging to diagnose performance.

## Durable health and actionable guardrails

`GET /admin/operations/health` is an aggregate read-only administrative probe. Scheduled maintenance emits `job_health` after its bounded tasks finish. Each job table is aggregated once; it reports due age, expired/invalid leases, terminal failures, incomplete completed vector jobs, legacy holds and pending reviewer/FTS/facet/recovery state. Legacy rows without verified outboxes are explicitly held, not reported as successful vector coverage. Invalid lease timestamps remain visible as expired; malformed due times cannot silently establish health. These probes do not repair data or prove the contents of remote Vectorize. Use M04/M05 reconciliation and M10 coverage/recovery when appropriate.

[alerts.json](../apps/api/observability/alerts.json) defines provisional guardrails and concrete operator actions. They cover incomplete completed vectors, terminal/stalled jobs, dispatch delay, persistent reviewer/facet lag, missing cron/log heartbeat, server-error rate and generation fallback rate. Repeated-sample rules require distinct observations separated in time; duplicate delivery cannot manufacture a persistent stall. Low-volume rate samples cannot trigger rate alarms. A non-healthy aggregate is operational evidence for inspection, not permission to delete data, clear an active lease or replay an unguarded writer.

Use the checked-in offline evaluator with a private JSONL export of custom records or Wrangler-tail envelopes. Normalize multiline JSON to JSONL first; never paste raw case data into a report or public CI. For an already authorized private export:

```sh
pnpm --filter @beedle/api report:operational-telemetry \
  --input /private/operations/events.jsonl --out /private/operations/report.json \
  --environment preview --from 2026-10-26T11:00:00Z --to 2026-10-26T12:00:00Z \
  --release FULL_40_CHARACTER_COMMIT_SHA
```

The reader caps input at 64 MiB, 64 KiB per line and 100,000 aggregate records, rejects invalid samples, separates environments/releases, deduplicates observations and writes a new mode-0600 report. Only safe projected metrics enter the output. Exit 2 means actionable provisional alerts, 1 means invalid/incomplete input, and 0 means no configured guardrail fired in that window. A zero exit status does not establish an SLO or legal/retrieval quality. Configure notification delivery in the approved observability system separately; no messages or recurring assistant automation were created here.

## Baseline and SLO acceptance

No representative production latency/availability baseline was available or collected. `latencySlo` and the zero-result-regression comparison deliberately have null targets. Local synthetic tests verify measurement and alarms; they are not a production workload baseline.

Before proposing SLOs, collect a complete private preview/production window at the exact release, corpus revision, capability and representative query/filter mix. Account for request shape, cold/steady state, provider limits, errors and deliberate corpus/approval holds. Use at least an hour and 1,000 actual sampled search requests per candidate baseline, and enough observations for each important class. `--representative` is an explicit operator declaration; it is not independently verified. The report exposes observed corpus/capability cohorts and requires one known cohort plus a fresh heartbeat before marking sufficient search samples eligible **for a proposal**, while still requiring owner acceptance and leaving targets unset.

The evaluator reports weighted server-error/quota rates, successful p50/p95 latency by query class, zero-result rate, generation fallback rate and provider **operation** failure rate separately. It uses inverse application sample weights; missing metadata/lost exports and biased query mixes limit conclusions. Provider-operation failures include body/shape/deadline checks, while evidence-rejected answers can occur after a successful provider operation. Empty searches can be correct; compare zero-result changes only against an accepted matching baseline or use M20's positive/negative/filtered canary. Independent recall/ranking, support and abstention quality remain M19.

After owner review, version the accepted target/window/exclusions and alert destination with that measured evidence, then rehearse a slow readiness request, provider 429/malformed output, a stalled job, failed heartbeat and missing vectors in isolated preview. Do not claim a production SLO from synthetic timings or quietly pick a convenient latency threshold.

## Rollout and rollback

There is no schema/data migration in M21. Follow M20's protected release/canary gates; verify log access, retention and volume in isolated preview before enabling persistence. Retain the generated correlation/status headers and safe redaction policy in a forward repair. If instrumentation causes a platform incompatibility, revert the binding instrumentation to the last compatible release while retaining durable jobs/data and safe error redaction; do not restore raw query/provider logging. Browser fallback metadata is optional and older chats remain readable.

Real platform collection, alert routing, representative baseline, target acceptance and legal/domain relevance remain external release prerequisites. No production resource, provider, deployment or push was used during this implementation.
