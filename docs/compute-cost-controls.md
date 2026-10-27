# Route and provider cost controls (M01)

Every API registration declares a cost class. The router wraps the matched handler with enforcement, including aliases and GET exports/previews. There is no independently maintained security path list that can disagree with the router's normalization. `routeInventory` exposes the declarations to deterministic tests; its diagnostic matcher is not the enforcement boundary. OPTIONS preflight and explicitly free health/taxonomy reads incur no quota. An absent/failing rate binding refuses protected work with 503; exhaustion returns 429 with Retry-After. Cost classes are not authentication or authorization.

Existing search, ingestion, LLM and administrative-write bindings remain. Omitted draft-debug/template/legacy approval routes are now covered. `REVIEW_READ_RATE_LIMIT` supplies 180 requests per minute per connecting IP for review/list/detail/source/preview reads, and `EXPORT_RATE_LIMIT` supplies 20 per minute for export operations. These separate read/export buckets avoid spending the much smaller mutation quota while navigating the review page or exporting several formats. Local, preview and production have distinct namespace IDs. Blank template generation skips research entirely even when findings/law text are supplied; other template modes retain their existing research behavior.

The IP is the Cloudflare connecting IP. Verified-user/office quotas require H01 and remain deferred. No client-supplied user/account header is accepted as an authenticated subject. Shared-office fairness cannot be fully established before that identity work. Cloudflare's rate-limit binding is a per-location, approximate burst control, not a global financial ledger: see [the platform behavior](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/).

## Shared application budgets

Migration `0020_compute_budgets.sql` adds one counter per deployment environment/provider work class. A single conditional D1 UPSERT reserves both the UTC-minute and UTC-day windows using D1's clock. Competing requests and isolates share the same cap. This does not rely on process memory or an IP. Default reviewed deployment limits are:

| Work class | Unit | Per minute | Per UTC day |
| --- | --- | ---: | ---: |
| LLM | Outbound completion attempt | 20 | 500 |
| Embedding | Text sent for embedding | 400 | 50,000 |
| Vector write | Vector submitted in an upsert | 1,600 | 100,000 |

`COMPUTE_BUDGETS_JSON` declares all three limits separately in each environment. Deployed environments require explicit valid configuration. Provider work fails closed when policy or the counter schema is unavailable. Actual attempt units are reserved before provider calls, including search embeddings, HTTP LLM generation, activation, background vector jobs and backfill. Failed, cancelled or uncertain-acknowledgment attempts are not refunded; accounting is deliberately conservative. Batch units must fit the configured window limits. Limits should be calibrated to measured traffic, model/token limits and the operator's provider budget before production rollout.

These are application work-unit ceilings, not a dollar-accurate account billing cap. Different models, input lengths and applications in the same Cloudflare/provider account can have different costs. M02 bounds each LLM operation/output; provider billing controls remain an additional operational safeguard. No paid provider was called during verification.

A shared-budget refusal returns 429 with the relevant minute/day reset in Retry-After. Vector jobs persist a deferred retry until that reset without consuming a provider-failure attempt. Activation keeps its retryable immutable plan; partial provider work remains conservatively charged. `GET /admin/cost-budgets` exposes configured limits and raw current counter buckets/usage for inspection, without a reset endpoint or uploaded content. Historical counter values identify their UTC buckets; they are not usage in a later window.

## Verification and rollout

Behavior tests execute every registered costly handler's rejection path, all formerly omitted routes, missing bindings, preflight/HEAD and router-normalization cases. They also cover zero-work blank templates, concurrent reservations, minute/day reset, environment separation, invalid policy/missing schema, zero outbound calls on refusal, and durable job deferral. An isolated actual workerd/D1 rehearsal used one-request burst limits and a synthetic loopback provider: omitted routes returned 429 and the shared budget refused a second LLM attempt from a different IP before it reached that provider.

Apply migration 0020 to the isolated preview first, configure both added rate bindings and the budget JSON, then exercise ordinary review/export/ingestion flows and inspect counter headroom. Apply the additive migration before the API revision reaches any production traffic. Do not reset counters to hide exhaustion; review traffic and policy deliberately. No production migration, resource creation or deployment was performed here. Rollback can retain the harmless counter table, but removing the code reopens the route/budget gaps.
