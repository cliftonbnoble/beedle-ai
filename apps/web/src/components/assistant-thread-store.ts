import { type AssistantChatResponse } from "@beedle/shared";

export type ChatRole = "user" | "assistant";

export type ChatMessage = {
  id: string;
  role: ChatRole;
  content: string;
  citations?: AssistantChatResponse["citations"];
  model?: string;
  pending?: boolean;
  streaming?: boolean;
  scopeLabel?: string;
  query?: string;
  indexCodes?: string[];
};

export type ConversationThread = {
  id: string;
  title: string;
  indexCodes: string[];
  createdAt: number;
  updatedAt: number;
  messages: ChatMessage[];
};

export const assistantStorageKey = "beedle-assistant-threads-v1";
export const assistantThreadsUpdatedEvent = "assistant-threads-updated";

export function createId(prefix: string) {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function createMessage(role: ChatRole, content: string, extras?: Partial<ChatMessage>): ChatMessage {
  return {
    id: createId(role),
    role,
    content,
    ...extras
  };
}

export function createEmptyThread(indexCodes: string[] = []): ConversationThread {
  return {
    id: createId("thread"),
    title: "New chat",
    indexCodes,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    messages: []
  };
}

export function deriveThreadTitle(messages: ChatMessage[]) {
  const firstUser = messages.find((message) => message.role === "user")?.content?.trim() || "New chat";
  const words = firstUser.split(/\s+/).filter(Boolean);
  if (words.length <= 5) return firstUser.length > 42 ? `${firstUser.slice(0, 39)}...` : firstUser;
  return `${words.slice(0, 5).join(" ")}...`;
}

export function getThreadPreview(thread: ConversationThread) {
  const lastMessage = [...thread.messages].reverse().find((message) => !message.pending);
  if (!lastMessage) return thread.indexCodes.length > 0 ? thread.indexCodes.join(", ") : "AI Assistant";
  const compact = lastMessage.content.replace(/\s+/g, " ").trim();
  return compact.length > 70 ? `${compact.slice(0, 67)}...` : compact;
}

export function formatThreadTime(timestamp: number) {
  try {
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit"
    }).format(new Date(timestamp));
  } catch {
    return "";
  }
}

function normalizeThreads(raw: unknown): ConversationThread[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .filter((item): item is ConversationThread => Boolean(item && typeof item === "object" && "id" in item))
    .map((thread) => ({
      ...thread,
      indexCodes: Array.isArray((thread as { indexCodes?: unknown }).indexCodes)
        ? (((thread as { indexCodes?: unknown[] }).indexCodes) || []).filter(
            (value): value is string => typeof value === "string" && value.trim().length > 0
          )
        : [],
      title:
        Array.isArray(thread.messages) && thread.messages.length > 0
          ? deriveThreadTitle(thread.messages as ChatMessage[])
          : typeof thread.title === "string"
            ? thread.title
            : "New chat",
      createdAt: typeof thread.createdAt === "number" ? thread.createdAt : Date.now(),
      updatedAt: typeof thread.updatedAt === "number" ? thread.updatedAt : Date.now(),
      messages: Array.isArray(thread.messages)
        ? thread.messages.filter((message): message is ChatMessage => Boolean(message && typeof message === "object" && "id" in message))
        : []
    }))
    .sort((a, b) => b.updatedAt - a.updatedAt);
}

export function loadAssistantThreads(): ConversationThread[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(assistantStorageKey);
    if (!raw) return [];
    return normalizeThreads(JSON.parse(raw));
  } catch {
    return [];
  }
}

export function saveAssistantThreads(threads: ConversationThread[]) {
  if (typeof window === "undefined") return;
  const normalized = normalizeThreads(threads);
  window.localStorage.setItem(assistantStorageKey, JSON.stringify(normalized));
  window.dispatchEvent(new CustomEvent(assistantThreadsUpdatedEvent));
}
