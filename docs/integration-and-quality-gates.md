> M20 update: CI now builds and tests the actual Pages adapter output; see [Release readiness and rollout](release-readiness-and-rollout.md). Local `test:e2e` still defaults to Next development mode; set `BEEDLE_E2E_ADAPTER=pages` after a Pages build to test deployment output.

# Behavioral and independent quality gates (M19)

The PR workflow runs two required suites with no credentials, live corpus or paid inference. Configure branch protection to require `gate` and `runtime-and-browser`; repository code cannot change hosted branch protection. Neither new suite imports the optional `live-test-helpers.mjs`, and neither skips when a server, browser, migration or binding is missing.

## Reproduce locally

Use Node 22+ and `pnpm install --frozen-lockfile`, then:

```sh
pnpm --filter @beedle/api test:integration:types
pnpm --filter @beedle/api test:integration
pnpm --filter @beedle/web exec playwright install --with-deps chromium
pnpm --filter @beedle/web test:e2e
```

An installed Chrome can be used locally with `BEEDLE_TEST_CHROME_CHANNEL=chrome pnpm --filter @beedle/web test:e2e`. CI installs the Chromium revision associated with the locked Playwright package. Browser tests reserve loopback ports 8819 and 5819; stop conflicting development servers first. No existing server is reused. Traces, screenshots and the HTML report go to ignored `output/playwright/`, retained in CI on failure for seven days.

The native suite starts Wrangler's actual local Worker runtime with all real D1 migrations, fresh disposable D1/R2 storage, native rate-limit bindings, and a real local Queue producer/consumer. It checks readiness, published search, grouped reviewer filters, source-write failure, D1 atomic rollback/lost acknowledgment, provider validation/body bounds, populated FTS recovery, durable vector retry/redispatch and duplicate delivery. The provider is a bounded loopback HTTP fixture; fake AI/Vectorize implement deterministic responses with cross-isolate vector storage in local R2. External fetches are rejected. This proves application/runtime behavior, not production Vectorize consistency or provider quality.

`tests/integration/worker.ts` is a separate test entrypoint; production Wrangler continues to bundle `src/index.ts`. Its fault controls are never mounted by the production app. CLI setup clears Cloudflare credentials, always uses `--local`, never deploys, and removes temporary storage/processes after the suite. Setup/requests/polls are bounded. Browser routes delay real API responses to expose reviewer races and pending chat state; native IndexedDB faults expose quota and two-tab transcript conflicts. Search and chat still execute the actual API and providers configured by this fixture.

Existing optional live golden/relevance suites retain their regression purpose. They do **not** replace these mandatory suites or the independent evaluation below. The old 19-query relevance labels include golden-derived and corpus-constructed judgments; they are not an unseen holdout.

## Independent human evaluation — separate release prerequisite

No newly independently judged corpus has been fabricated or relabeled. The checked-in [template](evaluation/independent-holdout.template.json) intentionally fails validation until humans complete it. Actual independent domain judgments and representative provider/preview execution remain an **unsatisfied release gate**. Synthetic metric tests verify the evaluator only.

1. A domain curator chooses unseen positive, negative, filtered, historical and adversarial questions against a frozen, isolated corpus. Keep this set outside implementation/tuning access. Record a SHA-256 of its corpus manifest, relevance grades by stable **document ID** (0 irrelevant, 1 relevant, 2 strongly relevant), answer/abstention expectations, scope and a written rubric. Never derive labels from current rankings or old goldens. A separate reviewer checks the labels. Seal and archive the JSON and its SHA before running the candidate code. Attestations are declarations, not authenticated signatures (H01 deferred).
2. Capture the candidate against the matching isolated preview, explicitly accepting inference cost. This is an opt-in local operation, not a PR job, and performs no administrative writes:

```sh
pnpm --filter @beedle/api eval:independent-holdout --mode collect \
  --holdout /private/review/holdout.json --api https://ISOLATED-API.example \
  --target-kind isolated-preview --corpus-sha256 CORPUS_SHA256 \
  --code-revision FULL_40_CHARACTER_GIT_SHA --allow-provider-calls \
  --out /private/review/candidate-run.json
```

The caller must verify isolation and the corpus snapshot; target naming and a declared digest do not prove server identity. No credentials or production target are inferred. Collection requires valid successful search/assistant contracts, caps responses at 2 MiB, has per-request deadlines, refuses redirects and fails incomplete runs. Collection retains quoted evidence and requires matching search/assistant corpus revisions throughout the run. Keep preview corpus frozen across collection; verify its manifest again before accepting results. Current assistant historical filtering constrains decision dates; it does not establish governing-law applicability. Evaluate legal edition selection separately with the M18 domain procedure.

3. The independent reviewer labels newly retrieved documents **blind to rank/system expectations** and checks each generated claim against original sources from that snapshot. Save judgments with this shape (one row for every case, one support judgment for every captured claim):

```json
{"version":1,"runSha256":"SHA256_OF_CANONICAL_CAPTURE","reviewer":"SAME_INDEPENDENT_REVIEWER","reviewedAt":"UTC_ISO_TIMESTAMP","blindRelevanceReview":true,"claimSupportCheckedAgainstOriginalSources":true,"rows":[{"id":"positive-01","grades":{"NEW_DOCUMENT_ID":0},"claims":[{"id":"CAPTURED_CLAIM_ID","supported":false,"reason":"Explain the support gap"}],"scopeSatisfied":true,"abstentionAppropriate":true,"answerSupported":false,"reason":"Explain overall assessment"}]}
```

The collection command prints its canonical artifact SHA. Supplemental grades cannot rewrite sealed labels. A changed candidate run requires new matching judgments. Missing rows, retrieved-document labels, claim judgments, coverage classes, incompatible identities/digests or timestamps fail closed.

```sh
pnpm --filter @beedle/api eval:independent-holdout --mode score \
  --holdout /private/review/holdout.json --run /private/review/candidate-run.json \
  --judgments /private/review/judgments.json --out /private/review/report.json
```

Report recall@5, precision@5, MRR@5 and nDCG@5 separately from human claim/answer support, abstention and scope compliance, overall and per category. Recall is explicitly relative to the **judged pool**, unless the domain reviewer attests that pool is exhaustive; it cannot prove corpus-wide recall. No-claim support rates are null, not 100%. Domain owners must set acceptance thresholds using measured baselines and the legal risk; the evaluator deliberately returns `requires-domain-review`, never automatic production approval. Compare unchanged holdout/corpus across candidates; retire exposed holdouts from future unseen claims.

Files can contain sensitive case text. Inputs are limited to 16 MiB, new outputs use exclusive creation and mode 0600, and logs print hashes/status only. Keep private artifacts outside Git and public CI. The aggregate report excludes queries, answers and source facts. Reviewer reasons and per-case IDs may themselves be sensitive.

## Release and rollback

No schema or product behavior changes are required by this finding. A failing mandatory suite blocks release; do not convert it into a skip. The M19 script inventory alias also fixes the pre-existing `report:vector-generation-shadow` inventory mismatch. If test tooling must be rolled back, retain equivalent behavioral coverage before releasing application changes. Actual Pages output, schema/preview release readiness and rollback gates belong to M20; authentication and Low findings remain outside this work.
