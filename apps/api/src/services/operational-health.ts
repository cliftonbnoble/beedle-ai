import type { Env } from '../lib/types';
// Aggregate each durable job table once. No payloads, document identifiers,
// source paths, provider error bodies or legal facts enter this projection.
export async function inspectOperationalHealth(env: Env) {
    const row = await env.DB.prepare(`WITH vector AS (
 SELECT COUNT(*) AS vector_total,
 COALESCE(SUM(state='queued' AND outbox_ready_at IS NOT NULL),0) AS vector_queued,
 COALESCE(SUM(state='processing'),0) AS vector_processing,
 COALESCE(SUM(state='failed' AND outbox_ready_at IS NOT NULL),0) AS vector_failed,
 COALESCE(SUM(outbox_ready_at IS NULL AND state IN ('queued','failed')),0) AS vector_legacy_held,
 COALESCE(SUM(state='processing' AND (unixepoch(lease_expires_at) IS NULL OR unixepoch(lease_expires_at)<=unixepoch())),0) AS vector_expired_leases,
 COALESCE(MAX(CASE WHEN state='queued' AND outbox_ready_at IS NOT NULL AND (next_attempt_at IS NULL OR unixepoch(next_attempt_at) IS NULL OR unixepoch(next_attempt_at)<=unixepoch()) THEN MAX(0,unixepoch()-COALESCE(unixepoch(next_attempt_at),unixepoch(outbox_ready_at),unixepoch(created_at),0)) ELSE 0 END),0) AS vector_oldest_due_seconds,
 COALESCE(SUM(state='completed' AND outbox_ready_at IS NOT NULL AND (vector_count IS NOT expected_chunk_count OR next_chunk_offset IS NOT expected_chunk_count)),0) AS vector_completed_incomplete
 FROM document_vector_jobs), maintenance AS (
 SELECT COALESCE(SUM(state IN ('queued','retryable')),0) AS maintenance_queued,
 COALESCE(SUM(state='running'),0) AS maintenance_running,COALESCE(SUM(state='failed'),0) AS maintenance_failed,
 COALESCE(SUM(state='running' AND (unixepoch(lease_expires_at) IS NULL OR unixepoch(lease_expires_at)<=unixepoch())),0) AS maintenance_expired_leases,
 COALESCE(MAX(CASE WHEN state IN ('queued','retryable') AND (next_attempt_at IS NULL OR unixepoch(next_attempt_at) IS NULL OR unixepoch(next_attempt_at)<=unixepoch()) THEN MAX(0,unixepoch()-COALESCE(unixepoch(next_attempt_at),unixepoch(created_at),0)) ELSE 0 END),0) AS maintenance_oldest_due_seconds
 FROM maintenance_jobs WHERE dry_run=0)
 SELECT vector.*,maintenance.*,
 (SELECT COUNT(*) FROM reviewer_queue_sources WHERE dirty=1) AS reviewer_pending,
 (SELECT CASE WHEN phase='ready' THEN 0 ELSE 1 END FROM reviewer_queue_control WHERE singleton=1) AS reviewer_build_pending,
 (SELECT CASE WHEN phase='ready' THEN 0 ELSE 1 END FROM search_index_build WHERE singleton=1) AS substring_build_pending,
 (SELECT CASE WHEN phase='ready' THEN 0 ELSE 1 END FROM search_phrase_build WHERE singleton=1) AS phrase_build_pending,
 (SELECT CASE WHEN phase='ready' THEN 0 ELSE 1 END FROM search_facet_build WHERE singleton=1) AS facet_build_pending,
 (SELECT CASE WHEN status='running' THEN 1 ELSE 0 END FROM search_index_rebuild WHERE singleton=1) AS search_recovery_running
 FROM vector,maintenance`).first<Record<string, number>>();
    if (!row || Object.values(row).some(value => !Number.isFinite(value)))
        throw new Error('Operational health state unavailable');
    const issues: string[] = [];
    for (const name of ['vector_failed', 'vector_expired_leases', 'vector_completed_incomplete', 'maintenance_failed', 'maintenance_expired_leases'])
        if (row[name]! > 0)
            issues.push(name);
    if (row.vector_oldest_due_seconds! > 900)
        issues.push('vector_dispatch_lag');
    if (row.maintenance_oldest_due_seconds! > 900)
        issues.push('maintenance_dispatch_lag');
    return { healthy: issues.length === 0, observedAt: new Date().toISOString(), metrics: row, issues };
}
