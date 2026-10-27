# Assistant live state and browser persistence (M17)

The chat page and sidebar subscribe to one `AssistantLiveStore` in the current window. A save is an output operation, never a reload of the current interaction. Pending placeholders and typing flags remain live until their request/presentation completes. Saved records omit those flags; a saved pending turn records an interrupted-request marker, so reopening a chat shows the retained question and an explanatory notice instead of a permanent spinner. Starting another question finishes the preceding typing presentation. Responses locate their own pending message ID, including after a conflict copy moves to a new thread ID. Deleted requests cannot recreate a chat through a late completion.

## Durable records and concurrent tabs

The native IndexedDB database `beedle-assistant-v2` contains version-2 per-thread records, monotonic revisions, deletion tombstones, capacity accounting and small transaction receipts. An atomic read/write transaction reads the latest thread and three-way merges against the writer's captured base. Independent filter changes merge. Competing transcript changes or different edits to the same field retain the local branch as a new chat and preserve the remote branch. The UI explains the recovery and follows the local pending request into its copy. Another tab deleting a chat while this tab has unsaved/in-flight work similarly preserves a local copy. Explicit local deletion wins over an obsolete failed write; tombstones prevent stale legacy data from reappearing.

BroadcastChannel messages and small storage-event notifications carry only thread ID/revision/writer, without transcript text. The receiver rereads that durable record and merges it with its own live work. Visibility changes reconcile missed notifications. Notifications from the originating window never normalize its live state. Storage events are a fallback, not the data store or the authority for revision ordering.

A save uses an operation ID. If the transaction commits but its acknowledgment is lost, retry reconciles that operation before refreshing remote state. Receipts retain the most recent 2,000 operations, with bounded cleanup of at most 25 per write. They hold IDs/revisions, not another full transcript. This gives a bounded retry window; a very old uncertain write whose receipt has been pruned may require a conflict copy rather than claiming indefinite exactly-once delivery.

## Limits and failures

Only changed threads are serialized. Writes debounce for 300 ms with a 2-second maximum delay and process at most 25 dirty records per pass; typing completion with no durable change produces no extra write. Deletions run first so they can free capacity after a quota failure. Logical JSON limits are 8 MiB per thread, 32 MiB across active threads, 500 active threads and 10,000 retained record IDs including tombstones. Browser-specific physical quotas may be lower. No old conversation is silently evicted to meet a limit.

Failures to open/read/write storage become a visible unsaved warning. Live questions, answers and filter state remain in memory. “Retry saving” reconciles revisions and uncertain writes; “Export chats” downloads the current live collection as JSON even when saving is unavailable. “Export stored data” preserves the readable original legacy data and raw database records for recovery, reporting an unavailable component rather than claiming a complete backup. Exports contain chat content and are explicitly user initiated.

Unsaved changes can be lost if the process/browser closes before the transaction commits; the UI distinguishes saved, saving and unsaved states. Page-hide/visibility flushing is best effort. Export before closing when an error persists. IndexedDB itself is browser storage, not a remote backup.

## Legacy migration and retention

The old `beedle-assistant-threads-v1` array is read once at startup and migrated per thread into the versioned store. Existing records/tombstones take precedence over that old snapshot. Core thread/message shapes, bounded filters and shared assistant evidence/reference schemas are validated. M14 provenance, M15 context and M16 case references/focus survive migration. Malformed records or optional fields produce a visible warning; invalid evidence is withheld from rendering. The original v1 bytes are retained for recovery and are never overwritten by a normalized copy. A fresh `?new=1` creates one chat after hydration rather than an initial chat plus another requested chat.

Deleting a v2 chat removes its transcript from active durable records; conflict copies are separate chats. The original legacy backup is retained, and the delete dialog says so. To remove every local copy, clear this site's browser data after exporting anything needed. User-created export files are independent copies.

**Identity remains deferred under H01.** The database and retained legacy key belong to the browser origin, not an account. This fix introduces no authentication or sign-out behavior. Before introducing multiple accounts on one origin, implement and test an account-specific namespace and an explicit sign-out/retention/migration policy. Do not treat the current origin-wide store as isolated account storage.

## Verification, release and rollback

Thirteen executable store regressions cover live pending/typing state, reload snapshots, malformed/versioned legacy data and provenance, quota/open failure, lost acknowledgments, concurrent independent fields, competing transcripts, asynchronous response completion, deletion/refresh races, deletion during a conflicting save recovery after capacity failures and field-order changes during normalization. All 46 web tests and workspace types pass.

Chrome with native IndexedDB verified one fresh chat, saved pending indicators, injected quota failure with the answer retained, retry, two-tab conflict relocation and correct pending-response completion, reload mid-request without a spinner, and legacy migration with references/focus retained and the original backup untouched. Test contexts used local synthetic providers; no paid provider, user tab or production resource was used. M19 adds required repeatable browser/Worker CI coverage; private-browser implementations and real supported browsers still need preview checks.

Ship the client changes together. Keep the legacy backup until migration has been reviewed and needed recovery data exported. Rollback restores the earlier client, which reads the retained v1 snapshot and cannot see newer IndexedDB chats; export newer chats before rollback. Do not drop the v2 database as part of rollback. Reopening a pending request cannot resume a terminated server request automatically; the user must restate it.
