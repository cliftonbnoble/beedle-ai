import { createId, deriveThreadTitle, type ChatMessage, type ConversationThread } from './assistant-thread-store';
import { assistantDataEqual, assistantPersistenceLimits, durableThread, readStoredThread, readThreadRecord, planThreadCommit, type ThreadCommit, type ThreadOperation, type ThreadRecord } from './assistant-thread-records';
import type { ThreadRepository } from './assistant-thread-database';
export type AssistantSnapshot = {
    threads: ConversationThread[];
    hydrated: boolean;
    status: 'loading' | 'saved' | 'saving' | 'unsaved' | 'unavailable';
    error: string | null;
    warning: string | null;
    relocations: Array<{
        from: string;
        to: string;
        id: string;
    }>;
};
export const emptyAssistantSnapshot: AssistantSnapshot = { threads: [], hydrated: false, status: 'loading', error: null, warning: null, relocations: [] };
const same=assistantDataEqual;
const overlayLive = (durable: ConversationThread, live: ConversationThread): ConversationThread => ({ ...durable,
    interrupted: live.messages.some(message => message.pending) ? false : durable.interrupted,
    messages: [...durable.messages.map(message => {
            const current = live.messages.find(item => item.id === message.id);
            return current?.streaming && current.content === message.content ? { ...message, streaming: true } : message;
        }), ...live.messages.filter(message => message.pending)] });
export class AssistantLiveStore {
    private live = new Map<string, ConversationThread>();
    private bases = new Map<string, ThreadRecord>();
    private dirty = new Set<string>();
    private operations = new Map<string, ThreadOperation>();
    private listeners = new Set<() => void>();
    private timer?: ReturnType<typeof setTimeout>;
    private firstDirtyAt = 0;
    private starting?: Promise<void>;
    private writing?: Promise<void>;
    private queuedRefresh = new Set<string>();
    private state: AssistantSnapshot = emptyAssistantSnapshot;
    constructor(private repository: ThreadRepository, private legacy: () => string | null = () => null, private announce: (record: ThreadRecord) => void = () => { }, private delayMs = 300) { }
    snapshot = () => this.state;
    subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; };
    private publish(update: Partial<AssistantSnapshot> = {}) {
        this.state = { ...this.state, ...update, threads: [...this.live.values()].sort((a, b) => b.updatedAt - a.updatedAt || a.id.localeCompare(b.id)) };
        for (const listener of this.listeners)
            listener();
    }
    private warn(message: string) { this.publish({ warning: message }); }
    private failed(error: unknown) {
        const detail = error instanceof Error ? error.message.slice(0, 220) : 'Browser storage rejected the operation.';
        this.publish({ status: 'unavailable', error: `Chats are kept in this tab but are not safely saved. ${detail}` });
    }
    private schedule() {
        if (!this.state.hydrated || this.state.status === 'unavailable')
            return;
        if (!this.firstDirtyAt)
            this.firstDirtyAt = Date.now();
        if (this.timer)
            clearTimeout(this.timer);
        this.timer = setTimeout(() => { this.timer = undefined; void this.flush(); }, Math.min(this.delayMs, Math.max(0, 2000 - (Date.now() - this.firstDirtyAt))));
    }
    start(): Promise<void> {
        if (!this.starting)
            this.starting = this.initialize();
        return this.starting;
    }
    private async initialize() {
        const legacyThreads: ConversationThread[] = [];
        let issues = 0;
        try {
            const raw = this.legacy();
            if (raw) {
                if (new TextEncoder().encode(raw).byteLength > assistantPersistenceLimits.legacyBytes)
                    throw new Error('Legacy chat storage is too large to migrate automatically. Export the original saved data.');
                const values: unknown = JSON.parse(raw);
                if (!Array.isArray(values) || values.length > assistantPersistenceLimits.threads)
                    throw new Error('Legacy chat storage needs recovery. Export the original saved data.');
                const seen = new Set<string>();
                for (const value of values) {
                    const parsed = readStoredThread(value);
                    issues += parsed.issues;
                    if (parsed.thread && !seen.has(parsed.thread.id)) {
                        seen.add(parsed.thread.id);
                        legacyThreads.push(parsed.thread);
                    }
                    else if (parsed.thread)
                        issues++;
                }
            }
        }
        catch (error) {
            issues++;
            this.warn(error instanceof Error ? error.message : 'Legacy chats could not be read.');
        }
        try {
            const records = await this.repository.readAll();
            for (const raw of records) {
                const parsed = readThreadRecord(raw);
                issues += parsed.issues;
                if (!parsed.record)
                    continue;
                this.bases.set(parsed.record.id, parsed.record);
                if (!this.live.has(parsed.record.id) && parsed.record.thread)
                    this.live.set(parsed.record.id, parsed.record.thread);
            }
            for (const thread of legacyThreads)
                if (!this.bases.has(thread.id) && !this.live.has(thread.id)) {
                    this.live.set(thread.id, thread);
                    this.dirty.add(thread.id);
                }
            this.publish({ hydrated: true, status: this.dirty.size ? 'unsaved' : 'saved' });
            if (this.dirty.size)
                this.schedule();
        }
        catch (error) {
            for (const thread of legacyThreads)
                if (!this.live.has(thread.id)) {
                    this.live.set(thread.id, thread);
                    this.dirty.add(thread.id);
                }
            this.publish({ hydrated: true });
            this.failed(error);
        }
        if (issues)
            this.warn('Some saved chat data could not be loaded or validated completely. The original storage has been retained. Export stored data before making recovery changes.');
    }
    add(thread: ConversationThread) {
        if (this.live.has(thread.id) || this.bases.has(thread.id))
            throw new Error('Chat identity already exists.');
        this.live.set(thread.id, thread);
        this.dirty.add(thread.id);
        this.publish({ status: this.state.status === 'unavailable' ? 'unavailable' : 'unsaved' });
        this.schedule();
        return thread;
    }
    update(id: string, updater: (thread: ConversationThread) => ConversationThread) {
        const before = this.live.get(id);
        if (!before)
            return;
        const next = updater(before);
        if (next.id !== id)
            throw new Error('A chat update cannot change its identity.');
        if (next === before)
            return;
        this.live.set(id, next);
        this.dirty.add(id);
        this.publish({ status: this.state.status === 'unavailable' ? 'unavailable' : 'unsaved' });
        this.schedule();
    }
    remove(id: string) { this.live.delete(id); this.operations.delete(id); this.dirty.add(id); this.publish({ status: this.state.status === 'unavailable' ? 'unavailable' : 'unsaved' }); this.schedule(); }
    complete(pendingId: string, message: ChatMessage): string | null {
        for (const thread of this.live.values()) {
            if (!thread.messages.some(item => item.id === pendingId && item.pending))
                continue;
            this.update(thread.id, current => {
                const messages = current.messages.map(item => item.id === pendingId ? message : item);
                return { ...current, messages, interrupted: false, title: deriveThreadTitle(messages), updatedAt: Date.now() };
            });
            return thread.id;
        }
        return null; // Deleted/cancelled requests cannot resurrect a thread.
    }
    private relocate(from: string, current: ConversationThread, to = createId('thread-conflict')) {
        this.live.set(to, { ...current, id: to });
        this.dirty.add(to);
        this.operations.delete(from);
        this.dirty.delete(from);
        this.publish({ relocations: [...this.state.relocations.slice(-49), { from, to, id: createId('relocation') }], warning: 'Concurrent changes were preserved in separate chats. This tab continues in the recovered copy; review both conversations.' });
        return to;
    }
    private acceptRemote(record: ThreadRecord, baseOverride?: ThreadRecord) {
        const base = baseOverride || this.bases.get(record.id);
        if (!baseOverride && base && base.revision >= record.revision)
            return;
        const current = this.live.get(record.id);
        if (!current && this.dirty.has(record.id)) {
            this.bases.set(record.id, record);
            return;
        }
        const inFlight = current?.messages.some(message => message.pending);
        if (current && (this.dirty.has(record.id) || inFlight || current.messages.some(message => message.streaming))) {
            const planned = planThreadCommit(record, { id: 'merge', threadId: record.id, forkId: createId('thread-conflict'), base: base || null, thread: durableThread(current) });
            if (planned.forkedFrom || (inFlight && !same(record.thread?.messages, base?.thread?.messages))) {
                this.relocate(record.id, current, planned.forkedFrom ? planned.record.id : undefined);
                if (record.thread)
                    this.live.set(record.id, record.thread);
                else
                    this.live.delete(record.id);
            }
            else if (planned.record.thread) {
                const merged = overlayLive(planned.record.thread, current);
                this.live.set(record.id, merged);
                if (same(durableThread(merged), record.thread))
                    this.dirty.delete(record.id);
                else
                    this.dirty.add(record.id);
            }
        }
        else if (record.thread)
            this.live.set(record.id, record.thread);
        else
            this.live.delete(record.id);
        this.bases.set(record.id, record);
    }
    async refresh(id?: string) {
        await this.start();
        if (this.writing) {
            this.queuedRefresh.add(id || '*');
            return;
        }
        try {
            const values = id ? [await this.repository.read(id)] : await this.repository.readAll();
            for (const raw of values) {
                if (raw === undefined)
                    continue;
                const { record, issues } = readThreadRecord(raw);
                if (issues)
                    this.warn('Some saved data could not be validated. Export stored data before recovery.');
                if (record)
                    this.acceptRemote(record);
            }
            this.publish();
            if (this.dirty.size)
                this.schedule();
        }
        catch (error) {
            this.failed(error);
        }
    }
    flush(): Promise<void> {
        if (this.writing)
            return this.writing;
        this.writing = this.writeDirty().finally(() => {
            this.writing = undefined;
            const ids = [...this.queuedRefresh];
            this.queuedRefresh.clear();
            if (ids.includes('*'))
                void this.refresh();
            else
                for (const id of ids)
                    void this.refresh(id);
            if (this.dirty.size && this.state.status !== 'unavailable')
                this.schedule();
        });
        return this.writing;
    }
    private async writeDirty() {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }
        this.firstDirtyAt = 0;
        if (!this.state.hydrated || this.state.status === 'unavailable')
            return;
        this.publish({ status: 'saving' });
        // Freeze this pass. New edits are handled by the next bounded pass.
        for (const id of [...this.dirty].sort((a, b) => Number(this.live.has(a)) - Number(this.live.has(b))).slice(0, 25)) {
            const current = this.live.get(id);
            const thread = current ? durableThread(current) : null;
            const base = this.bases.get(id) || null;
            if (!this.operations.has(id) && (thread ? same(thread, base?.thread) : base?.thread === null)) {
                this.dirty.delete(id);
                continue;
            }
            const operation = this.operations.get(id) || { id: createId('write'), threadId: id, forkId: createId('thread-conflict'), base, thread };
            this.operations.set(id, operation);
            try {
                const result = await this.repository.commit(operation);
                this.operations.delete(id);
                this.acceptCommit(operation, result);
                this.announce(result.record);
            }
            catch (error) {
                this.failed(error);
                return;
            }
        }
        this.publish({ status: this.dirty.size ? 'unsaved' : 'saved', error: null });
    }
    private acceptCommit(operation: ThreadOperation, result: ThreadCommit) {
        const current = this.live.get(operation.threadId);
        if (result.forkedFrom && !current) {
            this.bases.set(result.record.id, result.record);
            this.dirty.add(result.record.id);
            this.dirty.delete(operation.threadId);
            this.bases.delete(operation.threadId);
            this.queuedRefresh.add(operation.threadId);
        }
        else if (result.forkedFrom && current) {
            const moved = this.relocate(operation.threadId, current, result.record.id);
            this.bases.set(moved, result.record);
            const local = this.live.get(moved)!;
            if (same(durableThread(local), result.record.thread))
                this.dirty.delete(moved);
            // The original belongs to the other tab; refresh it after this write pass.
            this.live.delete(operation.threadId);
            this.bases.delete(operation.threadId);
            this.queuedRefresh.add(operation.threadId);
        }
        else if (!current) {
            if (!operation.thread) {
                this.bases.set(operation.threadId, result.record);
                this.dirty.delete(operation.threadId);
            }
            // Deletion during an in-flight save is still dirty until its tombstone is durable.
            else {
                this.bases.set(operation.threadId, result.record);
                this.dirty.add(operation.threadId);
            }
        }
        else if (same(durableThread(current), operation.thread)) {
            // Compare against the acknowledged payload, so our own saved user message
            // is never mistaken for another tab's transcript change.
            this.acceptRemote(result.record, { version: 2, id: result.record.id, revision: 0, thread: operation.thread, bytes: 0 });
            const latest = this.live.get(result.record.id);
            if (latest && same(durableThread(latest), result.record.thread))
                this.dirty.delete(result.record.id);
        }
        else {
            this.acceptRemote(result.record, { version: 2, id: result.record.id, revision: 0, thread: operation.thread, bytes: 0 });
        }
    }
    async retry() {
        this.publish({ status: 'unsaved', error: null });
        // Reconcile uncertain writes with their original operation IDs before a
        // refresh can mistake our own committed payload for a concurrent edit.
        if (this.operations.size) {
            await this.flush();
            if (this.state.status === 'unavailable')
                return;
        }
        // Re-read durable revisions before retrying; a failed open must not turn
        // existing chats into new writes with no concurrency base.
        await this.refresh();
        if (this.state.status !== 'unavailable')
            await this.flush();
    }
    exportLive() { return JSON.stringify({ version: 2, exportedAt: new Date().toISOString(), threads: this.state.threads }, null, 2); }
    async exportStored() {
        let legacy: string | null = null;
        let legacyError: string | undefined;
        try {
            legacy = this.legacy();
        }
        catch {
            legacyError = 'Legacy storage could not be read.';
        }
        return JSON.stringify({ version: 2, legacy, legacyError, records: await this.repository.readAll() }, null, 2);
    }
}
