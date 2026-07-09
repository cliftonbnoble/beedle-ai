-- `0008_search_fts.sql` already creates the FTS table and its maintenance triggers. The deployed
-- corpus is populated from the previous request-time bootstrap, so a corpus-wide dedupe/backfill
-- would only spend D1 CPU and can exceed its migration limit. Runtime bootstrap is removed in code;
-- from this migration forward the triggers maintain all new or changed chunks.
--
-- Keep a fast, recorded migration boundary so upgraded databases acknowledge that the old runtime
-- bootstrap must no longer be relied upon. A fresh database runs 0008 first and needs no backfill.
SELECT 1;
