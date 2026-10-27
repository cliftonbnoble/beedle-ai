// Fail before a historical script can mutate metadata or send an obsolete request.
throw new Error('Legacy activation writers are retired by H13. Preserve existing reports. Use scripts/retrieval-activation-v2.mjs prepare or convert, review the new plan, then write or rollback. See docs/activation-generations.md.');
