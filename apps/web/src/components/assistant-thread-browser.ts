'use client';
import { useSyncExternalStore } from 'react';
import { AssistantLiveStore, emptyAssistantSnapshot } from './assistant-live-store';
import { AssistantThreadDatabase } from './assistant-thread-database';
import { assistantStorageKey, createEmptyThread } from './assistant-thread-store';
let instance: AssistantLiveStore | undefined;
const notificationKey = 'beedle-assistant-notify-v2';
export function browserAssistantStore() {
    if (!instance) {
        const writer = crypto.randomUUID();
        let channel: BroadcastChannel | undefined;
        try {
            channel = new BroadcastChannel('beedle-assistant-v2');
        }
        catch { /* Storage events/visibility provide sync fallback. */ }
        const store = new AssistantLiveStore(new AssistantThreadDatabase(), () => window.localStorage.getItem(assistantStorageKey), record => {
            const notification = { version: 2, writer, id: record.id, revision: record.revision };
            try {
                channel?.postMessage(notification);
            }
            catch { /* Durable commit already succeeded. */ }
            try {
                window.localStorage.setItem(notificationKey, JSON.stringify(notification));
            }
            catch { /* Broadcast/visibility still work. */ }
        });
        const receive = (value: unknown) => {
            if (!value || typeof value !== 'object')
                return;
            const message = value as Record<string, unknown>;
            if (message.version === 2 && message.writer !== writer && typeof message.id === 'string' && message.id.length <= 200 && Number.isSafeInteger(message.revision))
                void store.refresh(message.id);
        };
        if (channel)
            channel.onmessage = event => receive(event.data);
        window.addEventListener('storage', event => { if (event.key === notificationKey && event.newValue) {
            try {
                receive(JSON.parse(event.newValue));
            }
            catch { /* Ignore malformed notifications. */ }
        } });
        document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible')
            void store.refresh();
        else
            void store.flush(); });
        window.addEventListener('pagehide', () => { void store.flush(); });
        instance = store;
        void store.start();
    }
    return instance;
}
const subscribe = (listener: () => void) => browserAssistantStore().subscribe(listener);
const getSnapshot = () => typeof window === 'undefined' ? emptyAssistantSnapshot : browserAssistantStore().snapshot();
export function useAssistantThreads() { return useSyncExternalStore(subscribe, getSnapshot, () => emptyAssistantSnapshot); }
export function newAssistantThread(indexCodes: string[] = []) { return browserAssistantStore().add(createEmptyThread(indexCodes)); }
export async function downloadAssistantChats(stored = false) {
    const store = browserAssistantStore();
    let data: string;
    if (stored) {
        try {
            data = await store.exportStored();
        }
        catch {
            let legacy: string | null = null;
            try {
                legacy = window.localStorage.getItem(assistantStorageKey);
            }
            catch { /* Export the readable portion. */ }
            data = JSON.stringify({ version: 2, legacy, error: 'The browser database could not be read. Live chats are available through Export chats.' }, null, 2);
        }
    }
    else
        data = store.exportLive();
    const url = URL.createObjectURL(new Blob([data], { type: 'application/json' }));
    const link = document.createElement('a');
    link.href = url;
    link.download = stored ? 'beedle-saved-chat-recovery.json' : 'beedle-chats.json';
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
}
