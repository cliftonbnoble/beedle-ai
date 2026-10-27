import { type AssistantChatResponse } from "@beedle/shared";

export type ChatRole = "user" | "assistant";

export type ChatMessage = {
  id: string;
  role: ChatRole;
  content: string;
  citations?: AssistantChatResponse["citations"];
  claims?: AssistantChatResponse["claims"];
  retrievedSources?: AssistantChatResponse["retrievedSources"];
  assessmentVersion?: AssistantChatResponse["assessmentVersion"];
  evidenceStatus?: AssistantChatResponse["evidenceStatus"];
  evidenceCapturedAt?: AssistantChatResponse["evidenceCapturedAt"];
  evidenceCorpusRevision?: AssistantChatResponse["evidenceCorpusRevision"];
  effectiveScope?: AssistantChatResponse["effectiveScope"];
  referenceContext?: AssistantChatResponse["referenceContext"];
  retrievalContext?: AssistantChatResponse["retrievalContext"];
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
  judgeNames?: string[];
  selectedDocumentIds?: string[];
  createdAt: number;
  updatedAt: number;
  messages: ChatMessage[];
  interrupted?: boolean;
};

export const assistantStorageKey = "beedle-assistant-threads-v1";

export function createId(prefix: string) {
  return `${prefix}-${crypto.randomUUID()}`;
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
