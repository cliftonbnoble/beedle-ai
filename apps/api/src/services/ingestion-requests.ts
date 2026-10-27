import { sha256Bytes } from '../lib/digests';
import { RequestConflictError, RequestValidationError } from '../lib/http';
import type { Env } from '../lib/types';
import { ingestionKeySchema, type IngestDocumentInput } from '@beedle/shared';
type Input = Omit<IngestDocumentInput, 'sourceFile'> & {
    sourceFile: {
        filename: string;
        mimeType: string;
    };
};
export interface IngestionReceipt {
    request_id: string;
    operation: string;
    fingerprint: string;
    source_sha256: string;
    document_id: string;
    source_key: string;
    record_id: string;
    source_revision: number;
    predecessor_document_id: string | null;
    predecessor_revision: number | null;
    state: 'processing' | 'retryable' | 'complete' | 'conflict';
    owner_token: string | null;
    lease_expires_at: string | null;
    result_json: string | null;
}
export class IngestionInProgress extends Error {
    constructor(readonly receipt: IngestionReceipt) { super('Ingestion is in progress'); }
}
const digest = (value: unknown) => sha256Bytes(new TextEncoder().encode(JSON.stringify(value)));
export async function ingestionRequestId(operation: string, key: string) { return digest(['ingestion/deployment/1', operation, ingestionKeySchema.parse(key)]); }
export function ingestionStatus(receipt: IngestionReceipt) {
    return { requestId: receipt.request_id, state: receipt.state, documentId: receipt.document_id,
        recordId: receipt.record_id, sourceRevision: receipt.source_revision, statusUrl: `/ingest/requests/${receipt.request_id}`,
        ...(receipt.result_json ? { result: JSON.parse(receipt.result_json) } : {}) };
}
export async function readIngestionStatus(env: Env, requestId: string) {
    if (!/^[a-f0-9]{64}$/.test(requestId))
        throw new RequestValidationError('Invalid ingestion request ID');
    const receipt = await env.DB.prepare('SELECT * FROM ingestion_requests WHERE request_id=?').bind(requestId).first<IngestionReceipt>();
    return receipt ? ingestionStatus(receipt) : null;
}
export interface IngestionAttempt {
    receipt: IngestionReceipt;
    env: Env;
    completion(result: {
        chunkCount: number;
        extractionStatus: string;
    }): D1PreparedStatement[];
}
export async function beginIngestion(env: Env, input: Input, sourceSha256: string): Promise<IngestionAttempt> {
    const requestId = await ingestionRequestId(input.fileType, input.idempotencyKey!);
    const fingerprint = await digest([1, input.fileType, input.jurisdiction, input.title, input.citation, input.decisionDate ?? null,
        input.sourceFile.filename, input.sourceFile.mimeType, input.performVectorUpsert, sourceSha256, input.revisionOf ?? null]);
    let receipt = await env.DB.prepare('SELECT * FROM ingestion_requests WHERE request_id=?').bind(requestId).first<IngestionReceipt>();
    if (!receipt) {
        let recordId: string | undefined, revision = 1;
        if (input.revisionOf) {
            const parent = await env.DB.prepare(`SELECT d.document_revision,d.file_type,d.jurisdiction,r.record_id,r.source_revision,
    (SELECT MAX(source_revision) FROM document_source_revisions WHERE record_id=r.record_id) AS latest
    FROM documents d JOIN document_source_revisions r ON r.document_id=d.id WHERE d.id=? AND d.ingestion_state IN ('ready','needs_review')`)
                .bind(input.revisionOf.documentId).first<{
                document_revision: number;
                file_type: string;
                jurisdiction: string;
                record_id: string;
                source_revision: number;
                latest: number;
            }>();
            if (!parent || parent.document_revision !== input.revisionOf.expectedRevision || parent.latest !== parent.source_revision)
                throw new RequestConflictError('Revision predecessor changed or is unavailable; reload it before submitting a new request');
            if (parent.file_type !== input.fileType || parent.jurisdiction !== input.jurisdiction)
                throw new RequestValidationError('A source revision must retain its record type and jurisdiction');
            recordId = parent.record_id;
            revision = parent.source_revision + 1;
        }
        const documentId = `doc_${crypto.randomUUID()}`, now = new Date().toISOString();
        await env.DB.prepare(`INSERT INTO ingestion_requests(request_id,operation,fingerprint,source_sha256,document_id,source_key,record_id,source_revision,
   predecessor_document_id,predecessor_revision,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'retryable',?,?) ON CONFLICT(request_id) DO NOTHING`)
            .bind(requestId, input.fileType, fingerprint, sourceSha256, documentId, `ingestion/${requestId}/source`, recordId ?? documentId, revision, input.revisionOf?.documentId ?? null, input.revisionOf?.expectedRevision ?? null, now, now).run();
        receipt = await env.DB.prepare('SELECT * FROM ingestion_requests WHERE request_id=?').bind(requestId).first<IngestionReceipt>();
    }
    if (!receipt)
        throw new Error('Ingestion reservation unavailable');
    if (receipt.fingerprint !== fingerprint)
        throw new RequestConflictError('Idempotency key was already used for different source bytes or metadata');
    if (receipt.state === 'complete')
        return { receipt, env, completion: () => [] };
    if (receipt.state === 'conflict')
        throw new RequestConflictError('Ingestion predecessor changed; use a new key after reviewing the current source revision');
    const token = crypto.randomUUID(), now = new Date().toISOString(), expires = new Date(Date.now() + 180000).toISOString();
    const claimed = await env.DB.prepare(`UPDATE ingestion_requests SET owner_token=?,lease_expires_at=?,state='processing',updated_at=?
 WHERE request_id=? AND (state='retryable' OR (state='processing' AND COALESCE(julianday(lease_expires_at),0)<=julianday(?))) RETURNING *`)
        .bind(token, expires, now, requestId, now).first<IngestionReceipt>();
    if (!claimed)
        throw new IngestionInProgress(receipt);
    receipt = claimed;
    const guard = () => env.DB.prepare(`INSERT INTO ingestion_write_guard(singleton) SELECT CASE WHEN EXISTS(
 SELECT 1 FROM ingestion_requests WHERE request_id=? AND owner_token=? AND state='processing'
 AND julianday(lease_expires_at)>julianday('now')) THEN 1 ELSE 0 END ON CONFLICT(singleton) DO NOTHING`).bind(requestId, token);
    const rawStatements = new WeakMap<D1PreparedStatement, D1PreparedStatement>();
    function wrap(target: D1PreparedStatement, mutating: boolean): D1PreparedStatement {
        const proxy = new Proxy(target, { get(object, key) {
                if (key === 'bind')
                    return (...args: unknown[]) => wrap(object.bind(...args), mutating);
                // Fence every mutation, including RETURNING. Reads preserve native metadata.
                if (mutating && key === 'raw')
                    return () => { throw new Error('Raw mutations are not supported in an ingestion attempt'); };
                if (mutating && ['run', 'first', 'all'].includes(String(key)))
                    return async (...args: unknown[]) => {
                        const results = await env.DB.batch([guard(), object]);
                        const result = results[1]!;
                        if (key === 'first') {
                            const row = result.results[0] ?? null;
                            return args[0] && row ? (row as Record<string, unknown>)[String(args[0])] ?? null : row;
                        }
                        if (key === 'run' || key === 'all')
                            return result;
                        throw new Error('Raw statements are not supported in an ingestion attempt');
                    };
                const value = Reflect.get(object, key);
                return typeof value === 'function' ? value.bind(object) : value;
            } });
        rawStatements.set(proxy, target);
        return proxy;
    }
    const DB = new Proxy(env.DB, { get(object, key) {
            if (key === 'exec' || key === 'withSession')
                return () => { throw new Error('Unfenced database execution is not supported in an ingestion attempt'); };
            if (key === 'prepare')
                return (sql: string) => wrap(object.prepare(sql), !/^[\s]*(SELECT|PRAGMA|EXPLAIN)\b/i.test(sql));
            if (key === 'batch')
                return async (statements: D1PreparedStatement[]) => (await object.batch([guard(), ...statements.map(row => rawStatements.get(row) || row)])).slice(1);
            const value = Reflect.get(object, key);
            return typeof value === 'function' ? value.bind(object) : value;
        } });
    // Reserved source keys are immutable for this fingerprint. Never let delayed
    // compensation delete bytes needed by a newer owner; recovery handles orphans.
    const SOURCE_BUCKET = new Proxy(env.SOURCE_BUCKET, { get(object, key) { if (key === 'delete')
            return async () => { }; const value = Reflect.get(object, key); return typeof value === 'function' ? value.bind(object) : value; } });
    const attempt = { ...env, DB, SOURCE_BUCKET };
    const partial = await DB.prepare('SELECT ingestion_state,document_revision FROM documents WHERE id=?').bind(receipt.document_id)
        .first<{
        ingestion_state: string;
        document_revision: number;
    }>();
    if (partial) {
        if (partial.ingestion_state !== 'preparing' || partial.document_revision !== 1) {
            await env.DB.prepare("UPDATE ingestion_requests SET state='conflict',owner_token=NULL,lease_expires_at=NULL WHERE request_id=? AND owner_token=?").bind(requestId, token).run();
            throw new RequestConflictError('Unfinished ingestion was edited; preserve it for manual reconciliation');
        }
        await DB.prepare("DELETE FROM documents WHERE id=? AND ingestion_state='preparing' AND document_revision=1").bind(receipt.document_id).run();
    }
    if (receipt.predecessor_document_id) {
        const parent = await env.DB.prepare(`SELECT d.document_revision,r.source_revision,(SELECT MAX(source_revision) FROM document_source_revisions WHERE record_id=r.record_id) AS latest FROM documents d JOIN document_source_revisions r ON r.document_id=d.id WHERE d.id=?`).bind(receipt.predecessor_document_id).first<{
            document_revision: number;
            source_revision: number;
            latest: number;
        }>();
        if (!parent || parent.document_revision !== receipt.predecessor_revision || parent.source_revision !== receipt.source_revision - 1 || parent.latest !== parent.source_revision) {
            await env.DB.prepare("UPDATE ingestion_requests SET state='conflict',owner_token=NULL,lease_expires_at=NULL WHERE request_id=? AND owner_token=?").bind(requestId, token).run();
            throw new RequestConflictError('Revision predecessor changed; review it and submit a new request key');
        }
    }
    const current = receipt;
    return { receipt, env: attempt, completion(result: {
            chunkCount: number;
            extractionStatus: string;
        }) {
            return [
                env.DB.prepare(`INSERT INTO ingestion_write_guard(singleton) SELECT CASE WHEN EXISTS(SELECT 1 FROM documents d WHERE id=? AND source_r2_key=? AND document_revision=1
   AND ((?='needs_review' AND ingestion_state='needs_review') OR (?='extracted' AND ingestion_state='preparing' AND (SELECT COUNT(*) FROM document_chunks WHERE document_id=d.id)=? AND ?>0)))
   THEN 1 ELSE 0 END ON CONFLICT(singleton) DO NOTHING`).bind(current.document_id, current.source_key, result.extractionStatus, result.extractionStatus, result.chunkCount, result.chunkCount),
                ...(current.predecessor_document_id ? [env.DB.prepare(`INSERT INTO ingestion_write_guard(singleton) SELECT CASE WHEN EXISTS(
   SELECT 1 FROM documents d JOIN document_source_revisions r ON r.document_id=d.id WHERE d.id=? AND d.document_revision=?
   AND r.record_id=? AND r.source_revision=? AND NOT EXISTS(SELECT 1 FROM document_source_revisions n WHERE n.record_id=r.record_id AND n.source_revision>r.source_revision))
   THEN 1 ELSE 0 END ON CONFLICT(singleton) DO NOTHING`).bind(current.predecessor_document_id, current.predecessor_revision, current.record_id, current.source_revision - 1)] : []),
                env.DB.prepare('INSERT INTO source_records(record_id) VALUES(?) ON CONFLICT DO NOTHING').bind(current.record_id),
                env.DB.prepare(`INSERT INTO document_source_revisions(document_id,record_id,source_revision,predecessor_document_id,source_sha256,identity_origin) VALUES(?,?,?,?,?,'verified_upload')`)
                    .bind(current.document_id, current.record_id, current.source_revision, current.predecessor_document_id, current.source_sha256),
                env.DB.prepare(`UPDATE ingestion_requests SET state='complete',result_json=?,owner_token=NULL,lease_expires_at=NULL,updated_at=? WHERE request_id=? AND owner_token=?`)
                    .bind(JSON.stringify(result), new Date().toISOString(), current.request_id, token)
            ];
        } };
}
export async function releaseIngestion(env: Env, receipt: IngestionReceipt): Promise<boolean> {
    let conflict = false;
    if (receipt.predecessor_document_id) {
        const current = await env.DB.prepare(`SELECT document_revision FROM documents WHERE id=? AND NOT EXISTS(
   SELECT 1 FROM document_source_revisions WHERE record_id=? AND source_revision>=? AND document_id!=?)`)
            .bind(receipt.predecessor_document_id, receipt.record_id, receipt.source_revision, receipt.document_id).first<{
            document_revision: number;
        }>();
        conflict = !current || current.document_revision !== receipt.predecessor_revision;
    }
    await env.DB.prepare(`UPDATE ingestion_requests SET state=?,owner_token=NULL,lease_expires_at=NULL,updated_at=? WHERE request_id=? AND owner_token=? AND state='processing'`)
        .bind(conflict ? 'conflict' : 'retryable', new Date().toISOString(), receipt.request_id, receipt.owner_token).run();
    return conflict;
}
