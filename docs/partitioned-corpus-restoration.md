# Owned partitioned corpus restoration

The optional `restored-corpus-32-v1` reader supports a prospective fresh build
from verified originals. It does not repair or replay an old recovery epoch.
Keep the original database, originals, fences and immutable recovery evidence.
The final application release still requires the [release gates](release-readiness-and-rollout.md).

The operator modules in `apps/api/scripts/lib/owned-restoration-*.mjs` separate
new-resource creation, immutable ownership claims, schema bootstrap and catalog
copying. Every mutation reserves an exclusive durable intent first. A lost
acknowledgment is reconciled by reading the exact physical resource, owner and
outcome; it never authorizes another create, insert or migration command.
Partial or differing outcomes remain held.

Owned command execution subscribes to process completion before awaiting PID
receipt bookkeeping. A fast exit during an `fsync` cannot strand the controller.
An unknown CLI exit can advance only after an independent exact owner, schema,
ledger and complete typed-value reconciliation; no acknowledgment is invented.

For each of the 32 new targets:

1. Verify the new UUID/name against the resource plan and forbidden original
   resources. Prove that no Worker or Pages deployment binds the target. Read
   back the immutable resource-owner claim independently.
2. Apply the frozen canonical migrations 1–18 once through pinned Wrangler.
   Require the acknowledged command, exact ordered ledger and expected native
   schema before proceeding. The ledger is written by the migration executor.
3. Seed the five preserved legacy catalog tables while they are still empty.
   The compiler uses typed UTF-8 hex literals, validates every statement against
   D1's 100,000-byte limit, and conditions inserts on the exact resource claim.
   Read back types and hex bytes, counts and the complete source catalog digest.
4. Apply canonical migrations 19–33 once, after positive catalog proof. These
   migrations capture the preserved catalog in its immutable legacy snapshot
   and retain unknown edition provenance. Verify the complete ledger/schema,
   unchanged catalog, captured row count and empty case corpus independently.
5. Build the reviewed source-copy/reprocessing/index/proof protocol separately.
   An empty schema or catalog copy is not a restored corpus or release pass.

Each phase requires fresh source-freeze, original-recovery-anchor, ownership,
production-pause, target-isolation and local-reserve guards. Only the documented
exact `_cf_KV` storage-table definition is excluded from application ownership
inspection; unknown provider-prefixed objects are held. The operator never
queries or changes provider storage.

Cloudflare documents [SQL imports and reserved storage](https://developers.cloudflare.com/d1/best-practices/import-export-data/)
and [per-statement/query/import limits](https://developers.cloudflare.com/d1/platform/limits/).
SQL file imports contain no transaction wrapper or virtual-table replacement.
The unsupported virtual-table export path is not used.

Initial provisional publication must remain visibly unreviewed. Source-byte
identity and successful extraction do not establish human approval, confirmed
metadata or authoritative legal applicability. Full acknowledged per-target
index/database checks, all-source readback, measured remote retained/reprocessing
peak capacity, representative real-data behavior and performance, compatible
rollback and fresh protected release evidence remain required before cutover.
