# Decision reader loading

The reader requests the document by ID through the existing published-decision endpoint. Query/filter context is optional and cannot prevent the document from rendering. ID-only bookmarks make no search request. Unpublished/missing documents remain blocked by the server; a client retry cannot bypass that policy. Authentication/H01 is unchanged.

## Independent state and cancellation

A new document ID mounts a fresh pane, so old title/source/body/error state cannot flash for a new identity. Its AbortController covers fetch and response-body consumption. The API helper validates the response schema and requires the document and every chunk to match the requested ID.

Only after that preview succeeds does the optional document-scoped search start. It has its own loading/error/retry state and controller. Query/filter changes immediately ignore old context using a serialized full identity (no delimiter collisions), then abort the old operation. Both loaders also check their aborted signal before committing state, covering a producer that settles after cancellation. Search failure, invalid URL filters or an empty result leave the document readable. Retrying search does not refetch the document. Query changes also retain the current preview.

Client cancellation stops the browser request; it cannot recall provider work that the API has already started. Existing backend work/cost/deadline bounds remain necessary.

## Context reuse and anchors

The mounted reader reuses already loaded matching context when presentation-only URL fields change. Existing search links open a separate `noopener` tab and carry query/filter text, not a validated reusable result object. This change introduces no persistent search/snippet cache or URL cursor capability. An ID-only bookmark needs only the preview; a query-bearing bookmark performs one bounded optional lookup per context identity.

The preview route captures and rechecks the corpus revision around its document reads and returns that revision with `private, no-store`. Optional matches must carry the same revision. Missing or changed revision evidence leaves the captured document readable with a reload prompt and no passage indicators; reloading rechecks publication and can return not found if the decision has been withdrawn. This closes the race between independent document and search requests. It does not make an already displayed page a live subscription: publication changes after both requests settle are checked on the next load. Cross-tab result reuse is still not implemented.

Header metadata comes from the document, never from a selected judge filter or an older search result. Search-term highlighting is labeled separately from matched-passage indicators. Match mapping includes primary/authority/supporting passages and uses the current document's paragraph anchors when source and reconstructed-reader chunk IDs differ. Unrelated documents/prefixes/out-of-range anchors cannot create matches. These are retrieval indicators, not verification of legal relevance.

Section/paragraph DOM IDs and matched-passage links retain navigable source anchors. Initial hash navigation is restored after asynchronous document rendering; invalid fragments cannot break the reader. Back-to-search preserves the existing applied query/filter URL.

## Verification and rollback

Pure checks cover cross-profile source-anchor mapping, authority/supporting passages, wrong document/prefix, out-of-range and unsafe indices. API checks reject a corpus change during a preview read. Actual Pages browser tests cover ID-only/direct reload, an unpublished ID, search outage/retry, delayed search, changing/invalid/removed query, retained preview/context, late response cancellation, navigation away from a delayed document, mismatched document/chunk responses, missing preview/search revision tokens, and a real reviewer edit between preview and search followed by publication revalidation. Abort signals are observed at the actual browser fetch boundary; an intercepted route's eventual network event is not treated as proof of cancellation.

Reader browser fixtures ingest and approve their own synthetic documents through the real review API. They do not depend on the older reviewer-race fixtures remaining published after metadata edits. No production source, provider inference, data migration or deployment is involved.

Rollback is code-only. Reverting L07 restores query-dependent document loading and coupling to search failure. Supported browser/routing behavior still needs the normal isolated deployed-preview acceptance before release. The history-update case follows [Next.js's supported native History API integration](https://nextjs.org/docs/app/getting-started/linking-and-navigating).
