import { assistantChatResponseSchema } from '@beedle/shared';
import type { ChatMessage, ConversationThread } from './assistant-thread-store';
export const assistantPersistenceLimits = { threadBytes: 8 * 1024 * 1024, totalBytes: 32 * 1024 * 1024, threads: 500, messageChars: 100000, legacyBytes: 32 * 1024 * 1024 } as const;
export type ThreadRecord = {
    version: 2;
    id: string;
    revision: number;
    thread: ConversationThread | null;
    bytes: number;
};
export type ThreadOperation = {
    id: string;
    threadId: string;
    forkId: string;
    base: ThreadRecord | null;
    thread: ConversationThread | null;
};
export type ThreadCommit = {
    record: ThreadRecord;
    forkedFrom?: string;
};
export const assistantDataEqual = (a: unknown, b: unknown) => {
    const canonical = (value: unknown) => JSON.stringify(value, (_key, item: unknown) => item && typeof item === 'object' && !Array.isArray(item)
        ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
    return canonical(a) === canonical(b);
};
const same=assistantDataEqual;
export const threadBytes = (thread: ConversationThread | null) => thread ? new TextEncoder().encode(JSON.stringify(thread)).byteLength : 0;
const validId = (value: unknown): value is string => typeof value === 'string' && value.length > 0 && value.length <= 200;
const object = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === 'object' && !Array.isArray(value));
const strings = (value: unknown, max: number) => Array.isArray(value) && value.length <= max && value.every(item => typeof item === 'string' && item.trim() && item.length <= 200);
const time = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
export function durableThread(thread: ConversationThread): ConversationThread {
    return { ...thread, interrupted: thread.messages.some(message => message.pending) || Boolean(thread.interrupted),
        messages: thread.messages.filter(message => !message.pending).map(({ pending: _pending, streaming: _streaming, ...message }) => message) };
}
// Validation never mutates a live thread. Invalid optional legacy evidence is
// withheld from rendering; the untouched v1 storage remains available for export.
export function readStoredThread(raw: unknown): {
    thread: ConversationThread | null;
    issues: number;
} {
    if (!object(raw) || !validId(raw.id) || !Array.isArray(raw.messages) || raw.messages.length > 10000 || !time(raw.createdAt) || !time(raw.updatedAt))
        return { thread: null, issues: 1 };
    if (threadBytes(raw as unknown as ConversationThread) > assistantPersistenceLimits.threadBytes)
        return { thread: null, issues: 1 };
    let issues = 0;
    const messages: ChatMessage[] = [];
    const ids = new Set<string>();
    for (const item of raw.messages) {
        if (!object(item) || !validId(item.id) || ids.has(item.id) || !['user', 'assistant'].includes(String(item.role)) || typeof item.content !== 'string' || item.content.length > assistantPersistenceLimits.messageChars) {
            issues++;
            continue;
        }
        ids.add(item.id);
        if (item.pending === true)
            continue;
        const message: ChatMessage = { id: item.id, role: item.role as ChatMessage['role'], content: item.content };
        for (const key of ['citations', 'claims', 'retrievedSources', 'assessmentVersion', 'evidenceStatus', 'evidenceCapturedAt', 'evidenceCorpusRevision', 'retrievalContext', 'effectiveScope', 'referenceContext', 'fallbackReason'] as const) {
            if (item[key] === undefined)
                continue;
            if (Array.isArray(item[key]) && item[key].length > 8) {
                issues++;
                continue;
            }
            const parsed = assistantChatResponseSchema.shape[key].safeParse(item[key]);
            if (parsed.success)
                Object.assign(message, { [key]: parsed.data });
            else
                issues++;
        }
        for (const key of ['model', 'scopeLabel', 'query'] as const)
            if (item[key] !== undefined) {
                if (typeof item[key] === 'string' && item[key].length <= 4000)
                    message[key] = item[key];
                else
                    issues++;
            }
        if (item.indexCodes !== undefined) {
            if (strings(item.indexCodes, 50))
                message.indexCodes = item.indexCodes as string[];
            else
                issues++;
        }
        messages.push(message);
    }
    const thread: ConversationThread = { id: raw.id, title: typeof raw.title === 'string' ? raw.title.slice(0, 200) : 'Saved chat',
        createdAt: raw.createdAt, updatedAt: raw.updatedAt, indexCodes: [], messages,
        interrupted: raw.interrupted === true || raw.messages.some(item => object(item) && item.pending === true) };
    for (const key of ['indexCodes', 'judgeNames', 'selectedDocumentIds'] as const)
        if (raw[key] !== undefined) {
            if (strings(raw[key], key === 'selectedDocumentIds' ? 8 : key === 'judgeNames' ? 12 : 50))
                thread[key] = raw[key] as string[];
            else
                issues++;
        }
    return { thread, issues };
}
export function readThreadRecord(raw: unknown): {
    record: ThreadRecord | null;
    issues: number;
} {
    if (!object(raw) || raw.version !== 2 || !validId(raw.id) || !Number.isSafeInteger(raw.revision) || Number(raw.revision) < 1)
        return { record: null, issues: 1 };
    if (raw.thread === null)
        return { record: { version: 2, id: raw.id, revision: Number(raw.revision), thread: null, bytes: 0 }, issues: 0 };
    const parsed = readStoredThread(raw.thread);
    if (!parsed.thread || parsed.thread.id !== raw.id)
        return { record: null, issues: parsed.issues + 1 };
    return { record: { version: 2, id: raw.id, revision: Number(raw.revision), thread: parsed.thread, bytes: threadBytes(parsed.thread) }, issues: parsed.issues };
}
// Atomic three-way merge. Separate field edits merge; competing transcript or
// same-field edits fork so two independent questions cannot acquire a false order.
export function planThreadCommit(current: ThreadRecord | null, operation: ThreadOperation): ThreadCommit {
    const local = operation.thread;
    const base = operation.base?.thread || null;
    const remote = current?.thread || null;
    const record = (id: string, thread: ConversationThread | null, revision: number): ThreadRecord => ({ version: 2, id, revision, thread: thread ? { ...thread, id } : null, bytes: threadBytes(thread ? { ...thread, id } : null) });
    if (!local)
        return { record: record(operation.threadId, null, (current?.revision || 0) + 1) };
    if (same(local, remote))
        return { record: current! };
    if ((!current && !operation.base) || current?.revision === operation.base?.revision)
        return { record: record(operation.threadId, local, (current?.revision || 0) + 1) };
    let conflict = !base || !remote;
    const merged = { ...remote } as ConversationThread;
    if (base && remote) {
        for (const key of ['messages', 'indexCodes', 'judgeNames', 'selectedDocumentIds', 'interrupted'] as const) {
            const localChanged = !same(local[key], base[key]), remoteChanged = !same(remote[key], base[key]);
            if (localChanged && remoteChanged && !same(local[key], remote[key]))
                conflict = true;
            Object.assign(merged, { [key]: localChanged ? local[key] : remote[key] });
        }
        merged.updatedAt = Math.max(local.updatedAt, remote.updatedAt);
        // Title follows the chosen transcript; title changes are not independent edits.
        merged.title = !same(local.messages, base.messages) ? local.title : remote.title;
    }
    if (conflict)
        return { record: record(operation.forkId, local, 1), forkedFrom: operation.threadId };
    return { record: record(operation.threadId, merged, (current?.revision || 0) + 1) };
}
