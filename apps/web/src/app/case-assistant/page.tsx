"use client";

import { Suspense, type FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { canonicalIndexCodeOptions } from "@beedle/shared";
import { ArrowUp } from "lucide-react";
import { StatusPill } from "@/components/status-pill";
import {
  assistantThreadsUpdatedEvent,
  createEmptyThread,
  createMessage,
  deriveThreadTitle,
  loadAssistantThreads,
  saveAssistantThreads,
  type ConversationThread
} from "@/components/assistant-thread-store";
import { runAssistantChat } from "@/lib/api";

function buildDecisionHref(documentId: string, query: string, indexCodes: string[]) {
  const params = new URLSearchParams();
  params.set("query", query);
  params.set("corpusMode", "trusted_plus_provisional");
  params.set("limit", "20");
  params.set("approvedOnly", "0");
  for (const code of indexCodes) params.append("indexCode", code);
  return `/search/decision/${encodeURIComponent(documentId)}?${params.toString()}`;
}

function dedupeIndexCodeOptions(options: typeof canonicalIndexCodeOptions) {
  const byCode = new Map<string, (typeof canonicalIndexCodeOptions)[number]>();
  for (const option of options) {
    if (!byCode.has(option.code)) byCode.set(option.code, option);
  }
  return Array.from(byCode.values());
}

function assistantScopeLabel(indexCodes: string[]) {
  if (indexCodes.length === 0) return "All decisions";
  if (indexCodes.length === 1) return `Index code ${indexCodes[0] || ""}`;
  return `${indexCodes.length} index codes`;
}

function toApiMessages(messages: ConversationThread["messages"]) {
  return messages
    .filter((message) => !message.pending)
    .map((message) => ({ role: message.role, content: message.content }));
}

function thinkingDots() {
  return (
    <span style={{ display: "inline-flex", gap: "0.28rem", alignItems: "center" }}>
      {[0, 1, 2].map((index) => (
        <span
          key={index}
          style={{
            width: "0.42rem",
            height: "0.42rem",
            borderRadius: "999px",
            background: "rgba(23, 77, 130, 0.45)",
            animation: `assistant-thinking 1.1s ${index * 0.12}s infinite ease-in-out`
          }}
        />
      ))}
    </span>
  );
}

export default function CaseAssistantPage() {
  return (
    <Suspense fallback={<main className="page-shell"><section className="card" style={{ padding: "1.25rem" }}>Loading assistant…</section></main>}>
      <CaseAssistantPageInner />
    </Suspense>
  );
}

function CaseAssistantPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchKey = searchParams.toString();
  const requestedThreadId = searchParams.get("thread") || "";
  const wantsNew = searchParams.get("new") === "1";

  const [threads, setThreads] = useState<ConversationThread[]>([]);
  const [activeThreadId, setActiveThreadId] = useState<string>("");
  const [indexCodes, setIndexCodes] = useState<string[]>([]);
  const [indexCodeFilterText, setIndexCodeFilterText] = useState("");
  const [isIndexCodeModalOpen, setIsIndexCodeModalOpen] = useState(false);
  const [draft, setDraft] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [streamingState, setStreamingState] = useState<{ messageId: string; visibleChars: number } | null>(null);
  const [composerOverflow, setComposerOverflow] = useState<"hidden" | "auto">("hidden");
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const composerRef = useRef<HTMLTextAreaElement | null>(null);
  const initializedRef = useRef(false);
  const handledNewKeyRef = useRef<string | null>(null);
  const syncingFromStorageRef = useRef(false);
  const dedupedIndexCodeOptions = useMemo(() => dedupeIndexCodeOptions(canonicalIndexCodeOptions), []);
  const filteredIndexCodeOptions = useMemo(() => {
    const filter = indexCodeFilterText.trim().toLowerCase();
    if (!filter) return dedupedIndexCodeOptions;
    return dedupedIndexCodeOptions.filter((option) =>
      [option.code, option.description, option.ordinance, option.rules].some((value) => value.toLowerCase().includes(filter))
    );
  }, [dedupedIndexCodeOptions, indexCodeFilterText]);
  const groupedIndexCodeOptions = useMemo(() => {
    const groups = new Map<string, Array<(typeof canonicalIndexCodeOptions)[number]>>();
    for (const option of filteredIndexCodeOptions) {
      const family = option.code.match(/^[A-Za-z]+/)?.[0] || "#";
      const current = groups.get(family) || [];
      current.push(option);
      groups.set(family, current);
    }
    return Array.from(groups.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, [filteredIndexCodeOptions]);

  useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;
    const loaded = loadAssistantThreads();
    if (loaded.length) {
      setThreads(loaded);
      return;
    }

    const initial = createEmptyThread([]);
    setThreads([initial]);
    saveAssistantThreads([initial]);
  }, []);

  useEffect(() => {
    function refreshThreadsFromStorage() {
      if (!initializedRef.current) return;
      const loaded = loadAssistantThreads();
      setThreads((current) => {
        if (JSON.stringify(current) === JSON.stringify(loaded)) return current;
        syncingFromStorageRef.current = true;
        return loaded;
      });
    }

    window.addEventListener("storage", refreshThreadsFromStorage);
    window.addEventListener(assistantThreadsUpdatedEvent, refreshThreadsFromStorage as EventListener);
    return () => {
      window.removeEventListener("storage", refreshThreadsFromStorage);
      window.removeEventListener(assistantThreadsUpdatedEvent, refreshThreadsFromStorage as EventListener);
    };
  }, []);

  useEffect(() => {
    if (!initializedRef.current || !threads.length) return;
    if (syncingFromStorageRef.current) {
      syncingFromStorageRef.current = false;
      return;
    }
    saveAssistantThreads(threads);
  }, [threads]);

  useEffect(() => {
    if (!threads.length) return;

    if (wantsNew) {
      if (handledNewKeyRef.current === searchKey) return;
      handledNewKeyRef.current = searchKey;
      const next = createEmptyThread([]);
      setThreads((current) => [next, ...current]);
      setActiveThreadId(next.id);
      setIndexCodes(next.indexCodes);
      setDraft("");
      setError(null);
      setStreamingState(null);
      router.replace(`/case-assistant?thread=${encodeURIComponent(next.id)}`, { scroll: false });
      return;
    }

    handledNewKeyRef.current = null;

    if (requestedThreadId) {
      const requested = threads.find((thread) => thread.id === requestedThreadId);
      if (requested) {
        if (activeThreadId !== requested.id) setActiveThreadId(requested.id);
        return;
      }
    }

    const fallbackThread = threads[0];
    if (fallbackThread && (!activeThreadId || !threads.some((thread) => thread.id === activeThreadId))) {
      setActiveThreadId(fallbackThread.id);
    }
  }, [activeThreadId, requestedThreadId, router, searchKey, threads, wantsNew]);

  const activeThread = useMemo(
    () => threads.find((thread) => thread.id === activeThreadId) || threads[0] || null,
    [activeThreadId, threads]
  );

  const messages = activeThread?.messages || [];
  const canSend = useMemo(() => draft.trim().length >= 3 && !loading, [draft, loading]);

  useEffect(() => {
    if (!composerRef.current) return;
    composerRef.current.style.height = "0px";
    const nextHeight = Math.min(composerRef.current.scrollHeight, 140);
    composerRef.current.style.height = `${nextHeight}px`;
    setComposerOverflow(composerRef.current.scrollHeight > 140 ? "auto" : "hidden");
  }, [draft]);

  useEffect(() => {
    if (!activeThread) return;
    const nextIndexCodes = activeThread.indexCodes || [];
    setIndexCodes((current) =>
      current.length === nextIndexCodes.length && current.every((value, index) => value === nextIndexCodes[index])
        ? current
        : nextIndexCodes
    );
    if (!requestedThreadId) {
      router.replace(`/case-assistant?thread=${encodeURIComponent(activeThread.id)}`, { scroll: false });
    }
  }, [activeThread, requestedThreadId, router]);

  useEffect(() => {
    if (!isIndexCodeModalOpen) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [isIndexCodeModalOpen]);

  useEffect(() => {
    if (!scrollerRef.current) return;
    scrollerRef.current.scrollTo({ top: scrollerRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, streamingState]);

  function updateThread(threadId: string, updater: (thread: ConversationThread) => ConversationThread) {
    setThreads((current) =>
      current
        .map((thread) => (thread.id === threadId ? updater(thread) : thread))
        .sort((a, b) => b.updatedAt - a.updatedAt)
    );
  }

  useEffect(() => {
    const lastMessage = activeThread?.messages[activeThread.messages.length - 1];
    if (!activeThread || !lastMessage || lastMessage.role !== "assistant" || lastMessage.pending || !lastMessage.streaming) {
      return;
    }

    if (!streamingState || streamingState.messageId !== lastMessage.id) {
      setStreamingState({ messageId: lastMessage.id, visibleChars: 0 });
      return;
    }

    if (streamingState.visibleChars >= lastMessage.content.length) {
      updateThread(activeThread.id, (thread) => ({
        ...thread,
        messages: thread.messages.map((message) =>
          message.id === lastMessage.id ? { ...message, streaming: false } : message
        )
      }));
      setStreamingState(null);
      return;
    }

    const step =
      lastMessage.content.length > 2200
        ? 28
        : lastMessage.content.length > 1200
          ? 18
          : lastMessage.content.length > 600
            ? 12
            : 7;

    const timeout = window.setTimeout(() => {
      setStreamingState((current) =>
        current && current.messageId === lastMessage.id
          ? {
              ...current,
              visibleChars: Math.min(lastMessage.content.length, current.visibleChars + step)
            }
          : current
      );
    }, 12);

    return () => window.clearTimeout(timeout);
  }, [activeThread, streamingState]);

  function addIndexCode(code: string) {
    setIndexCodes((current) => {
      const next = current.includes(code) ? current : [...current, code];
      if (activeThread) {
        updateThread(activeThread.id, (thread) => ({
          ...thread,
          indexCodes: next,
          updatedAt: Date.now()
        }));
      }
      return next;
    });
  }

  function removeIndexCode(code: string) {
    setIndexCodes((current) => {
      const next = current.filter((value) => value !== code);
      if (activeThread) {
        updateThread(activeThread.id, (thread) => ({
          ...thread,
          indexCodes: next,
          updatedAt: Date.now()
        }));
      }
      return next;
    });
  }

  function clearIndexCodes() {
    setIndexCodes([]);
    if (!activeThread) return;
    updateThread(activeThread.id, (thread) => ({
      ...thread,
      indexCodes: [],
      updatedAt: Date.now()
    }));
  }

  async function sendMessage(content: string) {
    const trimmed = content.trim();
    if (!trimmed || loading || !activeThread) return;

    const userMessage = createMessage("user", trimmed);
    const pendingMessage = createMessage("assistant", "Thinking through the corpus…", { pending: true });
    const nextMessages = [...activeThread.messages, userMessage, pendingMessage];

    updateThread(activeThread.id, (thread) => ({
      ...thread,
      title: deriveThreadTitle(nextMessages),
      indexCodes,
      updatedAt: Date.now(),
      messages: nextMessages
    }));

    setDraft("");
    setLoading(true);
    setError(null);

    try {
      const response = await runAssistantChat({
        messages: toApiMessages([...activeThread.messages, userMessage]),
        judgeNames: [],
        indexCodes,
        limit: 6
      });

      updateThread(activeThread.id, (thread) => {
        const withoutPending = thread.messages.filter((message) => !message.pending);
        const assistantMessage = createMessage("assistant", response.answer, {
          citations: response.citations,
          model: response.model,
          scopeLabel: response.scopeLabel,
          query: trimmed,
          indexCodes,
          streaming: true
        });
        const finalMessages = [...withoutPending, assistantMessage];
        return {
          ...thread,
          title: deriveThreadTitle(finalMessages),
          updatedAt: Date.now(),
          messages: finalMessages,
          indexCodes
        };
      });
    } catch (nextError) {
      const message = nextError instanceof Error ? nextError.message : "Assistant chat failed.";
      updateThread(activeThread.id, (thread) => {
        const withoutPending = thread.messages.filter((item) => !item.pending);
        const assistantMessage = createMessage(
          "assistant",
          `I couldn't complete that chat request yet. ${message}`,
          { scopeLabel: assistantScopeLabel(indexCodes), query: trimmed, indexCodes }
        );
        const finalMessages = [...withoutPending, assistantMessage];
        return {
          ...thread,
          title: deriveThreadTitle(finalMessages),
          updatedAt: Date.now(),
          messages: finalMessages,
          indexCodes
        };
      });
      setError(message);
    } finally {
      setLoading(false);
    }
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await sendMessage(draft);
  }

  return (
    <main
      className="page-shell case-assistant-layout"
      style={{
        marginTop: "-1.32rem",
        marginBottom: "-2.55rem",
        height: "calc(100dvh - 3.35rem)",
        minHeight: "560px",
        overflow: "hidden",
        display: "grid",
        gridTemplateRows: "auto minmax(0, 1fr)"
      }}
    >
      <section className="page-hero" style={{ marginBottom: "0.08rem", alignItems: "start" }}>
        <div>
          <h2 className="page-title" style={{ fontSize: "clamp(1.55rem, 2.7vw, 2.1rem)" }}>Chat</h2>
        </div>
        <StatusPill label="Grounded chat" />
      </section>

      <section
        className="card"
        style={{
          padding: 0,
          overflow: "hidden",
          minHeight: 0,
          height: "100%",
          display: "grid",
          gridTemplateRows: "auto minmax(0, 1fr) auto"
        }}
      >
        <div
          style={{
            padding: "0.42rem 0.56rem 0.38rem",
            borderBottom: "1px solid rgba(24, 38, 56, 0.08)",
            display: "flex",
            justifyContent: "flex-start",
            gap: "0.45rem",
            flexWrap: "wrap",
            alignItems: "center"
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.45rem",
              flexWrap: "wrap",
              minWidth: 0
            }}
          >
            <button type="button" className="drafting-inline-button" onClick={() => setIsIndexCodeModalOpen(true)}>
              + Add Index Code
            </button>
            {indexCodes.length > 0 ? (
              <button type="button" className="drafting-inline-button" onClick={clearIndexCodes}>
                Clear
              </button>
            ) : null}
            {indexCodes.length > 0 ? (
              <div
                className="drafting-index-field__chips"
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.38rem",
                  flexWrap: "wrap",
                  minWidth: 0
                }}
              >
                {indexCodes.map((code) => (
                  <span key={code} className="drafting-index-chip">
                    <span>{code}</span>
                    <button type="button" onClick={() => removeIndexCode(code)} aria-label={`Remove ${code}`}>
                      ×
                    </button>
                  </span>
                ))}
              </div>
            ) : null}
          </div>
        </div>

        <div
          ref={scrollerRef}
          style={{
            overflowY: "auto",
            minHeight: 0,
            padding: "0.22rem 0.52rem 0.02rem",
            display: "flex",
            flexDirection: "column",
            alignItems: "stretch",
            gap: "0.42rem",
            background: "linear-gradient(180deg, rgba(246, 248, 251, 0.72), rgba(255, 255, 255, 0.98))"
          }}
        >
          {messages.length === 0 ? (
            <div
              style={{
                minHeight: "100%",
                display: "grid",
                placeItems: "center",
                padding: "0.65rem 1rem 0.45rem"
              }}
            >
              <div style={{ display: "grid", gap: "0.24rem", justifyItems: "center", textAlign: "center", maxWidth: "25rem" }}>
                <strong style={{ fontSize: "0.84rem" }}>Start a new conversation</strong>
                <span style={{ color: "var(--muted)", lineHeight: 1.4, fontSize: "0.8rem" }}>
                  Ask about prior decisions, rent reductions, recurring issues, or other decision patterns.
                </span>
              </div>
            </div>
          ) : null}

          {messages.map((message) => {
            const isAssistant = message.role === "assistant";
            const isStreaming = message.streaming && streamingState?.messageId === message.id;
            const renderedContent = message.pending
              ? ""
              : isStreaming
                ? message.content.slice(0, streamingState?.visibleChars || 0)
                : message.content;

            return (
              <div
                key={message.id}
                style={{
                  display: "grid",
                  justifyItems: isAssistant ? "start" : "end"
                }}
              >
                <article
                  style={{
                    width: isAssistant ? "min(100%, 56rem)" : "fit-content",
                    maxWidth: isAssistant ? "100%" : "min(100%, 42rem)",
                    borderRadius: isAssistant ? "18px 18px 18px 8px" : "18px 18px 8px 18px",
                    border: isAssistant ? "1px solid rgba(24, 38, 56, 0.08)" : "1px solid rgba(20, 93, 160, 0.12)",
                    background: isAssistant ? "white" : "rgba(20, 93, 160, 0.09)",
                    padding: isAssistant ? "0.82rem 0.95rem" : "0.42rem 0.72rem 0.5rem",
                    boxShadow: isAssistant ? "0 10px 24px rgba(24, 38, 56, 0.05)" : "none"
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: "0.55rem", marginBottom: isAssistant ? "0.36rem" : "0.08rem", alignItems: "center", flexWrap: "wrap" }}>
                    <strong
                      style={{
                        fontSize: isAssistant ? "0.79rem" : "0.56rem",
                        letterSpacing: isAssistant ? "0.02em" : "0.08em",
                        color: isAssistant ? "#174d82" : "rgba(24, 38, 56, 0.46)"
                      }}
                    >
                      {isAssistant ? "ASSISTANT" : "YOU"}
                    </strong>
                    {isAssistant && message.scopeLabel ? (
                      <span style={{ color: "var(--muted)", fontSize: "0.76rem" }}>{message.scopeLabel}</span>
                    ) : null}
                  </div>

                  <div style={{ whiteSpace: "pre-wrap", lineHeight: 1.68, fontSize: isAssistant ? "0.98rem" : "0.95rem" }}>
                    {message.pending ? thinkingDots() : renderedContent}
                    {isStreaming ? <span className="assistant-stream-cursor" /> : null}
                  </div>

                  {isAssistant && !message.pending && !message.streaming && message.citations && message.citations.length > 0 ? (
                    <details
                      className="assistant-citations"
                      style={{
                        borderTop: "1px solid rgba(24, 38, 56, 0.08)"
                      }}
                    >
                      <summary
                        style={{
                          cursor: "pointer",
                          listStyle: "none",
                          fontSize: "0.8rem",
                          fontWeight: 700,
                          letterSpacing: "0.02em",
                          color: "var(--muted)"
                        }}
                      >
                        Referenced decisions ({message.citations.length})
                      </summary>
                      <div style={{ display: "grid", gap: "0.7rem", marginTop: "0.8rem" }}>
                        {message.citations.slice(0, 4).map((citation) => (
                          <article
                            key={citation.documentId}
                            style={{
                              border: "1px solid rgba(24, 38, 56, 0.08)",
                              borderRadius: "14px",
                              padding: "0.78rem 0.86rem",
                              background: "rgba(248, 250, 252, 0.92)"
                            }}
                          >
                            <div style={{ display: "flex", justifyContent: "space-between", gap: "0.75rem", flexWrap: "wrap", alignItems: "center" }}>
                              <div style={{ display: "grid", gap: "0.2rem" }}>
                                <strong>{citation.title}</strong>
                                <span style={{ color: "var(--muted)", fontSize: "0.84rem" }}>
                                  {citation.citation} · Judge {citation.authorName || "Unknown"}
                                </span>
                              </div>
                              <a
                                href={buildDecisionHref(citation.documentId, message.query || "", message.indexCodes || [])}
                                target="_blank"
                                rel="noopener noreferrer"
                                style={{
                                  textDecoration: "none",
                                  color: "#174d82",
                                  fontWeight: 700,
                                  fontSize: "0.86rem"
                                }}
                              >
                                Open Decision
                              </a>
                            </div>
                            <div style={{ display: "grid", gap: "0.42rem", marginTop: "0.7rem" }}>
                              {citation.primaryAuthorityPassage?.snippet ? (
                                <p style={{ margin: 0, lineHeight: 1.55, fontSize: "0.9rem" }}>
                                  <strong>Conclusions:</strong> {citation.primaryAuthorityPassage.snippet}
                                </p>
                              ) : null}
                              {citation.supportingFactPassage?.snippet ? (
                                <p style={{ margin: 0, lineHeight: 1.55, fontSize: "0.9rem" }}>
                                  <strong>Findings:</strong> {citation.supportingFactPassage.snippet}
                                </p>
                              ) : null}
                            </div>
                          </article>
                        ))}
                      </div>
                    </details>
                  ) : null}
                </article>
              </div>
            );
          })}
        </div>

        <div
          style={{
            position: "sticky",
            bottom: 0,
            zIndex: 1,
            borderTop: "1px solid rgba(24, 38, 56, 0.08)",
            padding: "0.18rem 0.42rem 0.2rem",
            background: "linear-gradient(180deg, rgba(255, 255, 255, 0.82), rgba(255, 255, 255, 0.98) 22%)",
            backdropFilter: "blur(14px)"
          }}
        >
          {error ? (
            <p style={{ margin: "0 0 0.7rem", color: "#8b2a2a", fontSize: "0.9rem" }}>{error}</p>
          ) : null}
          <form
            onSubmit={onSubmit}
            style={{
              display: "flex",
              alignItems: "flex-end",
              gap: "0.65rem",
              border: "1px solid rgba(24, 38, 56, 0.1)",
              borderRadius: "18px",
              background: "rgba(255, 255, 255, 0.98)",
              boxShadow: "0 16px 44px rgba(24, 38, 56, 0.08)",
              padding: "0.42rem 0.48rem 0.42rem 0.7rem"
            }}
          >
            <textarea
              ref={composerRef}
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  void sendMessage(draft);
                }
              }}
              rows={1}
              placeholder="Ask about prior decisions…"
              style={{
                resize: "none",
                minHeight: "1.6rem",
                maxHeight: "8.75rem",
                borderRadius: "12px",
                border: "none",
                outline: "none",
                padding: "0.35rem 0",
                font: "inherit",
                lineHeight: 1.5,
                background: "transparent",
                flex: 1,
                overflowY: composerOverflow
              }}
            />
            <button
              type="submit"
              disabled={!canSend}
              aria-label={loading ? "Thinking" : "Send"}
              style={{
                width: "2rem",
                height: "2rem",
                borderRadius: "999px",
                border: "none",
                background: canSend ? "#174d82" : "rgba(24, 38, 56, 0.12)",
                color: "white",
                display: "grid",
                placeItems: "center",
                cursor: canSend ? "pointer" : "not-allowed",
                flex: "0 0 auto",
                boxShadow: canSend ? "0 10px 18px rgba(23, 77, 130, 0.22)" : "none"
              }}
            >
              <ArrowUp size={16} />
            </button>
          </form>
        </div>
      </section>

      {isIndexCodeModalOpen ? (
        <div className="drafting-index-modal__backdrop" onClick={() => setIsIndexCodeModalOpen(false)}>
          <div className="drafting-index-modal" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label="Add index codes">
            <div className="drafting-index-modal__header">
              <div>
                <h3>Add Index Codes</h3>
                <p>Search by code or description, then add the ones you want into this assistant chat.</p>
              </div>
              <button type="button" className="drafting-index-modal__close" onClick={() => setIsIndexCodeModalOpen(false)} aria-label="Close">
                ×
              </button>
            </div>

            <div className="drafting-index-modal__toolbar">
              <input
                value={indexCodeFilterText}
                onChange={(event) => setIndexCodeFilterText(event.target.value)}
                placeholder="Filter index codes by code or description"
                className="field drafting-index-modal__search"
                autoFocus
              />
              <div className="drafting-index-modal__count">
                {indexCodes.length === 0 ? "No codes selected" : `${indexCodes.length} selected`}
              </div>
            </div>

            <div className="drafting-index-modal__list">
              {groupedIndexCodeOptions.map(([family, options]) => (
                <details key={family} className="drafting-index-modal__family" open>
                  <summary>
                    {family} family · {options.length} code{options.length === 1 ? "" : "s"}
                  </summary>
                  <div className="drafting-index-modal__family-list">
                    {options.map((option) => {
                      const selected = indexCodes.includes(option.code);
                      return (
                        <div key={option.code} className="drafting-index-modal__option">
                          <div className="drafting-index-modal__option-body">
                            <div className="drafting-index-modal__option-topline">
                              <strong>{option.code}</strong>
                            </div>
                            <div className="drafting-index-modal__option-description">{option.description}</div>
                            {(option.ordinance || option.rules) ? (
                              <div className="drafting-index-modal__option-meta">
                                {option.ordinance ? `Ord. ${option.ordinance}` : ""}
                                {option.ordinance && option.rules ? " · " : ""}
                                {option.rules ? `R&R ${option.rules}` : ""}
                              </div>
                            ) : null}
                          </div>
                          <button
                            type="button"
                            className={`drafting-index-modal__add ${selected ? "is-selected" : ""}`}
                            onClick={() => addIndexCode(option.code)}
                            disabled={selected}
                          >
                            {selected ? "Added" : "+"}
                          </button>
                        </div>
                      );
                    })}
                  </div>
                </details>
              ))}
            </div>

            <div className="drafting-index-modal__footer">
              <div className="drafting-index-modal__selected-line">
                {indexCodes.length > 0 ? `Selected: ${indexCodes.join(", ")}` : "Select one or more index codes to narrow the assistant search."}
              </div>
              <button type="button" className="button-secondary" onClick={() => setIsIndexCodeModalOpen(false)}>
                Done
              </button>
            </div>
          </div>
        </div>
      ) : null}

      <style jsx>{`
        @keyframes assistant-thinking {
          0%, 80%, 100% {
            transform: scale(0.72);
            opacity: 0.45;
          }
          40% {
            transform: scale(1);
            opacity: 1;
          }
        }

        @keyframes assistant-cursor {
          0%, 100% {
            opacity: 0;
          }
          50% {
            opacity: 1;
          }
        }

        .assistant-stream-cursor {
          display: inline-block;
          width: 0.55ch;
          height: 1.15em;
          margin-left: 0.08rem;
          vertical-align: -0.18em;
          border-radius: 2px;
          background: rgba(23, 77, 130, 0.55);
          animation: assistant-cursor 0.9s infinite ease-in-out;
        }

        :global(.assistant-citations) {
          margin-top: 0.5rem;
          padding-top: 0.45rem;
        }

        :global(.assistant-citations[open]) {
          margin-top: 0.8rem;
          padding-top: 0.72rem;
        }
      `}</style>
    </main>
  );
}
