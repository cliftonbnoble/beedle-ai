# Assistant follow-ups and scope (M16)

The assistant accepts one strict `filters` object. It supports the search jurisdiction, index codes/operator, rules/ordinance section, party-title substring, judge names, decision series, date range and up to eight selected document IDs. Legacy root `indexCodes`/`judgeNames` remain accepted; conflicting duplicate scope declarations return 400 before database/provider work. Singular and plural document constraints intersect. The assistant always searches published, currently approved decisions; unsupported file type, provisional corpus, chunk-type or approval overrides are rejected rather than ignored. Code selections default to exact `and` membership. This is the current public publication policy, not an identity authorization boundary (H01 remains deferred).

Responses expose `effectiveScope`, the actual bounded query, a resolution category, and focused IDs not represented in the result set. Source links capture the response's filters, rather than the currently edited controls. The reader applies those filters to its contextual search. Its existing full-source preview policy is unchanged. Changing controls during a request affects the next request; the arriving response records its own captured scope without resetting those controls.

## Reference resolution

The server assigns case numbers to the ordered retrieved decision list. `referenceContext` records that order, the original issue and the active cases used by the preceding answer. The UI echoes the most recent assistant context, never skipping a later failed answer to reuse older context. This structure is untrusted conversation data, not a signature or access credential. Every resolved/focused ID is checked again with the current trusted publication predicate and all current filters before retrieval or generation.

- Ordinals such as “the second case” address the preceding reference list. After automatic narrowing, the original numbering remains stable; a subsequent pronoun can address the narrowed active case.
- Short pronouns and explicit case phrases require an unambiguous antecedent or explicit focus. Multiple numbered cases, unavailable numbers, conflicts with focus, and filter changes without a freshly selected case request clarification. No broad fallback follows a failed focus check.
- Explicit focus can be chosen through the retrieved-decision checkboxes and cleared in the filter bar. Full judge names are separated with semicolons, allowing commas inside names.
- A standalone issue starts a new reference list. Previous plain answer text is never parsed as a reliable document identity.
- “More cases” reuses the earlier issue with the current filters. Results are bounded and may overlap earlier results; the answer discloses this. It does not promise exhaustive or unseen cases.

Resolution is deterministic and deliberately conservative, not a general natural-language coreference system. Unusual/long implicit pronouns or mixed new-topic/follow-up questions may need a restated issue or explicit selection. The original full question remains in the unprivileged model data. The applied query and scope are inspectable so reviewers can detect intent changes. Relevance, entailment and historical legal applicability are not established by resolving a case ID.

## Retrieval bounds

One search carries the complete filter intersection. Selected document IDs constrain SQL candidates and provider metadata before semantic top-K. Conflicting scalar/set IDs yield no provider call. The M05 namespace, cohort, partition/byte budgets and M08 cursor identity apply to the new filter as well.

After selecting at most eight decisions, one indexed lexical pass searches complementary passages within those same decision IDs and filters. It retains at most two additional passages per decision, preferring current-question terms and section diversity, excluding already supplied chunks. The indexed channel's 1,600 candidate ceiling, 96 returned-row limit and the request-wide SQL/rows/bytes/scoring/deadline budget remain enforced. This is bounded additional evidence, not complete document reading. A selected case absent from the bounded decision result set is reported explicitly and cannot receive a generated claim without a retrieved evidence span. The M14 quote checks and final corpus epoch fence still apply.

## Verification and release

Behavioral tests cover stable ordinal/pronoun resolution, topic changes, plural/ambiguous references, changed/conflicting filters, strict request rejection before work, all accepted filters against actual SQLite SQL, complementary passage recall, sticky budget exhaustion, unavailable/unapproved cases and captured model scope. Provider tests place 120 higher-scoring outsiders ahead of selected cases and verify metadata filtering before cutoff; cursor tests reject changed selected IDs.

Disposable workerd/D1 checks confirmed two-case retrieval, case 2 followed by a pronoun, judge filtering, and clarification on unavailable/changed scope, with synthetic providers and no paid calls. Chrome verified numbered decisions, case 2 remaining numbered 2 after automatic narrowing, focus selection, judge controls and captured reader links. Full API behavior/source and web suites, workspace types and the production Worker dry-run bundle pass. The previously recorded utility script-inventory failure remains a separate M19 CI prerequisite.

No migration or production operation is required. Release the shared DTO, API and web together; older clients can send legacy root filters but cannot resolve ordinals without reference context. Rollback this commit as a unit. Saved new fields are optional and ignored by the previous client; deleting the stored conversation is unnecessary. Validate difficult real follow-ups and retrieval quality in isolated preview and the independently judged M19 holdout before claiming production relevance gains. Authentication and Low findings are out of scope.
