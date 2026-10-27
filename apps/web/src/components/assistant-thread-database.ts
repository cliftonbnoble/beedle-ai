import { assistantDataEqual, assistantPersistenceLimits, planThreadCommit, readStoredThread, readThreadRecord, threadBytes, type ThreadRecord, type ThreadOperation, type ThreadCommit } from './assistant-thread-records';
export type ThreadRepository = {
    readAll(): Promise<unknown[]>;
    read(id: string): Promise<unknown>;
    commit(operation: ThreadOperation): Promise<ThreadCommit>;
};
export const assistantDatabaseName = 'beedle-assistant-v2';
const receiptLimit = 2000;
export class AssistantThreadDatabase implements ThreadRepository {
    private connection?: Promise<IDBDatabase>;
    private open(): Promise<IDBDatabase> {
        if (!this.connection)
            this.connection = new Promise<IDBDatabase>((resolve, reject) => {
                if (typeof indexedDB === 'undefined') {
                    reject(new Error('Browser storage is unavailable.'));
                    return;
                }
                let blocked = false;
                const request = indexedDB.open(assistantDatabaseName, 1);
                request.onupgradeneeded = () => {
                    request.result.createObjectStore('threads', { keyPath: 'id' });
                    request.result.createObjectStore('receipts', { keyPath: 'id', autoIncrement: false }).createIndex('time', 'time');
                    request.result.createObjectStore('capacity');
                };
                request.onsuccess = () => { const db = request.result; if (blocked) {
                    db.close();
                    return;
                } db.onversionchange = () => { db.close(); this.connection = undefined; }; resolve(db); };
                request.onerror = () => reject(request.error || new Error('Unable to open browser storage.'));
                request.onblocked = () => { blocked = true; reject(new Error('Close older Beedle tabs to finish opening chat storage.')); };
            }).catch(error => { this.connection = undefined; throw error; });
        return this.connection;
    }
    async readAll(): Promise<unknown[]> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction('threads', 'readonly');
            const request = tx.objectStore('threads').getAll();
            tx.oncomplete = () => resolve(request.result);
            tx.onabort = tx.onerror = () => reject(tx.error || new Error('Unable to read saved chats.'));
        });
    }
    async read(id: string): Promise<unknown> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction('threads', 'readonly');
            const request = tx.objectStore('threads').get(id);
            tx.oncomplete = () => resolve(request.result);
            tx.onabort = tx.onerror = () => reject(tx.error || new Error('Unable to read saved chat.'));
        });
    }
    async commit(operation: ThreadOperation): Promise<ThreadCommit> {
        if (threadBytes(operation.thread) > assistantPersistenceLimits.threadBytes)
            throw new Error('This chat exceeds the 8 MiB saved-chat limit. Export it before starting another chat.');
        if (operation.thread) {
            const checked = readStoredThread(operation.thread);
            if (!checked.thread || checked.issues)
                throw new Error('The current chat has unsupported saved fields. Export it before recovery.');
        }
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(['threads', 'receipts', 'capacity'], 'readwrite');
            const threads = tx.objectStore('threads'), receipts = tx.objectStore('receipts'), capacity = tx.objectStore('capacity');
            let result: ThreadCommit;
            let reason: Error | undefined;
            const fail = (message: string) => { reason = new Error(message); tx.abort(); };
            const receipt = receipts.get(operation.id);
            receipt.onsuccess = () => {
                if (receipt.result) {
                    const saved = threads.get(receipt.result.targetId);
                    saved.onsuccess = () => { if (!saved.result) {
                        fail('A saved transaction receipt has no chat record. Export stored data for recovery.');
                        return;
                    } result = { record: saved.result, ...(receipt.result.forkedFrom ? { forkedFrom: receipt.result.forkedFrom } : {}) }; };
                    return;
                }
                const existing = threads.get(operation.threadId);
                existing.onsuccess = () => {
                    // Records written by this adapter have a strict versioned shape. Refuse
                    // tampered records rather than overwriting their recoverable bytes.
                    const current = existing.result as ThreadRecord | undefined;
                    if (current && (readThreadRecord(current).issues || typeof current.bytes !== 'number' || current.bytes !== threadBytes(current.thread))) {
                        fail('Saved chat data needs recovery. Export stored data before retrying.');
                        return;
                    }
                    result = planThreadCommit(current || null, operation);
                    const target = threads.get(result.record.id);
                    target.onsuccess = () => {
                        const before = target.result as ThreadRecord | undefined;
                        if (result.forkedFrom && before) {
                            if (!assistantDataEqual(before.thread,result.record.thread)) {
                                fail('A conflict copy already exists with different content. Export stored data before retrying.');
                                return;
                            }
                            result = { ...result, record: before };
                        }
                        const usage = capacity.get('usage');
                        usage.onsuccess = () => {
                            const prior = usage.result || { bytes: 0, count: 0, records: 0 };
                            const records = (prior.records || 0) + (before ? 0 : 1);
                            const bytes = prior.bytes - (before?.bytes || 0) + result.record.bytes;
                            const count = prior.count - (before?.thread ? 1 : 0) + (result.record.thread ? 1 : 0);
                            if (bytes > assistantPersistenceLimits.totalBytes || count > assistantPersistenceLimits.threads || records > 10000) {
                                fail('Saved chats reached the 32 MiB / 500 chat limit. Export or delete chats, then retry saving. Recovery may be needed after 10,000 retained chat records.');
                                return;
                            }
                            threads.put(result.record);
                            capacity.put({ bytes, count, records }, 'usage');
                            receipts.put({ id: operation.id, time: Date.now(), targetId: result.record.id, appliedRevision: result.record.revision, forkedFrom: result.forkedFrom });
                            // Keep recent transaction receipts for lost-ack retries, with bounded cleanup.
                            const total = receipts.count();
                            total.onsuccess = () => {
                                let remaining = Math.min(25, Math.max(0, total.result - receiptLimit));
                                if (!remaining)
                                    return;
                                const cursor = receipts.index('time').openCursor();
                                cursor.onsuccess = () => { if (cursor.result && remaining-- > 0) {
                                    cursor.result.delete();
                                    cursor.result.continue();
                                } };
                            };
                        };
                    };
                };
            };
            tx.oncomplete = () => resolve(result!);
            tx.onabort = tx.onerror = () => reject(reason || tx.error || new Error('Unable to save chat.'));
        });
    }
}
