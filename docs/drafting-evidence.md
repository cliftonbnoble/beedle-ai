# Draft evidence and review contract

New drafts carry `assessment_version: retrieval-only-2` and require human review.
Similarity, section labels, exact quotations, and repeated excerpts do not verify
that a legal proposition follows from a source. Claim records retain source IDs,
excerpt offsets, candidate text, and review flags. New drafts cannot receive
strong/mixed/weak semantic support or elevated confidence. Historical payloads
remain `legacy-1`, preserving their original labels for auditability.

## Separate inputs and sources

- `findings_text` is supplied record context, not independently established fact.
- `law_text` is supplied legal context, not verified authority.
- `rules_sections` and `ordinance_sections` request exact catalog provisions.
  Explicit citations in `law_text` also request those provisions. A missing exact
  subsection is reported as missing; its parent is never substituted.
- Similar decisions are retrieved separately and labeled `similar_decision`.
  Their facts are never included in the rule/ordinance list.
- Catalog excerpts are `provision_candidate` records, carrying their update
  revision and a link to the complete provision. The link refuses a changed
  revision instead of silently displaying different evidence.
- Up to six distinct provisions fit a draft review. Larger requests fail with
  a clear message so the caller can split them. Provision excerpts are bounded
  at 8,000 characters with an explicit truncation warning; the linked text is full.
- Nonempty `uploaded_doc_ids` fail validation while document access control is
  deferred. An empty array remains compatible. Paste reviewed findings as context;
  do not fetch a private document merely because a caller knows its ID.
- Index codes, issue tags, and requested provisions contribute to similar-case
  retrieval. Style preferences reach the drafting prompt as user data.

The current catalog has no reliable jurisdiction/effective-edition registry.
Drafts explicitly report `jurisdiction_and_effective_edition_unverified`; a
reviewer must verify amendments, governing jurisdiction, effective dates, and
applicability against the original law before using a provision. The broader M18
catalog redesign is out of scope for the High-only remediation goal. Do not add
implicit claims that these candidates are current law.

No validated holding/disposition extractor is available. Direction is always
`unclear`; outcome keywords are not counted or passed to the model as a
recommendation. The fallback is a review scaffold, not an inferred legal outcome.

Shared response, API, and web changes should deploy together. No historical draft
or production catalog is rewritten by these changes. Preserve captured excerpts,
source revisions, and assessment versions in review artifacts.
