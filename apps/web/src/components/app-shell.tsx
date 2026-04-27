"use client";

import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Suspense, useEffect, useState, type ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { FilePenLine, FilePlus2, Gavel, Landmark, LayoutDashboard, Plus, Trash2, UserRoundPlus } from "lucide-react";
import {
  assistantThreadsUpdatedEvent,
  createEmptyThread,
  loadAssistantThreads,
  saveAssistantThreads,
  type ConversationThread
} from "@/components/assistant-thread-store";

type NavItem = {
  href: string;
  label: string;
  icon: LucideIcon;
  match: (pathname: string) => boolean;
};

const navItems: NavItem[] = [
  { href: "/search", label: "Search", icon: Gavel, match: (pathname) => pathname.startsWith("/search") },
  { href: "/case-assistant?new=1", label: "Assistant", icon: UserRoundPlus, match: (pathname) => pathname.startsWith("/case-assistant") },
  { href: "/drafting", label: "Drafting", icon: FilePenLine, match: (pathname) => pathname.startsWith("/drafting") },
  { href: "/add-decision", label: "Add Decision", icon: FilePlus2, match: (pathname) => pathname.startsWith("/add-decision") },
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard, match: (pathname) => pathname.startsWith("/dashboard") }
];

export default function AppShell({ children }: { children: ReactNode }) {
  return (
    <Suspense fallback={<AppShellFrame>{children}</AppShellFrame>}>
      <AppShellInner>{children}</AppShellInner>
    </Suspense>
  );
}

function AppShellInner({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [assistantThreads, setAssistantThreads] = useState<ConversationThread[]>([]);

  useEffect(() => {
    function refreshThreads() {
      setAssistantThreads(loadAssistantThreads().slice(0, 8));
    }

    refreshThreads();
    window.addEventListener("storage", refreshThreads);
    window.addEventListener(assistantThreadsUpdatedEvent, refreshThreads as EventListener);
    return () => {
      window.removeEventListener("storage", refreshThreads);
      window.removeEventListener(assistantThreadsUpdatedEvent, refreshThreads as EventListener);
    };
  }, []);

  const activeThreadId = searchParams.get("thread") || "";
  function deleteAssistantThread(threadId: string) {
    const nextThreads = loadAssistantThreads().filter((thread) => thread.id !== threadId);
    if (nextThreads.length === 0) nextThreads.push(createEmptyThread([]));
    saveAssistantThreads(nextThreads);
    setAssistantThreads(nextThreads.slice(0, 8));

    if (pathname.startsWith("/case-assistant") && activeThreadId === threadId) {
      const nextThread = nextThreads[0];
      router.replace(nextThread ? `/case-assistant?thread=${encodeURIComponent(nextThread.id)}` : "/case-assistant?new=1");
    }
  }

  return (
    <AppShellFrame
      pathname={pathname}
      assistantThreads={assistantThreads}
      activeThreadId={activeThreadId}
      onDeleteThread={deleteAssistantThread}
    >
      {children}
    </AppShellFrame>
  );
}

function AppShellFrame({
  children,
  pathname = "",
  assistantThreads = [],
  activeThreadId = "",
  onDeleteThread
}: {
  children: ReactNode;
  pathname?: string;
  assistantThreads?: ConversationThread[];
  activeThreadId?: string;
  onDeleteThread?: (threadId: string) => void;
}) {
  const [threadPendingDelete, setThreadPendingDelete] = useState<ConversationThread | null>(null);

  useEffect(() => {
    if (!threadPendingDelete) return;
    const stillExists = assistantThreads.some((thread) => thread.id === threadPendingDelete.id);
    if (!stillExists) setThreadPendingDelete(null);
  }, [assistantThreads, threadPendingDelete]);

  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div className="app-sidebar__brand">
          <div className="app-sidebar__crest" aria-hidden="true">
            <Landmark />
          </div>
          <div>
            <p className="app-sidebar__eyebrow">Beedle AI Companion</p>
            <h1 className="app-sidebar__title">Quiet Authority</h1>
          </div>
        </div>

        <nav className="app-sidebar__nav" aria-label="Primary">
          {navItems.map((item) => {
            const active = item.match(pathname);
            const Icon = item.icon;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={`app-sidebar__link${active ? " is-active" : ""}`}
              >
                <span className="app-sidebar__icon" aria-hidden="true">
                  <Icon />
                </span>
                <span className="app-sidebar__label">{item.label}</span>
              </Link>
            );
          })}
        </nav>

        <div className="app-sidebar__threads">
          <div className="app-sidebar__threads-header">
            <span className="app-sidebar__threads-title">Conversations</span>
          </div>
          <div className="app-sidebar__threads-actions">
            <Link href="/case-assistant?new=1" className="app-sidebar__threads-new">
              <span className="app-sidebar__icon" aria-hidden="true">
                <Plus />
              </span>
              <span className="app-sidebar__label">New chat</span>
            </Link>
          </div>

          <div className="app-sidebar__threads-list">
            {assistantThreads.map((thread) => {
              const active = pathname.startsWith("/case-assistant") && activeThreadId === thread.id;
              return (
                <div key={thread.id} className={`app-sidebar__thread-row${active ? " is-active" : ""}`}>
                  <Link
                    href={`/case-assistant?thread=${encodeURIComponent(thread.id)}`}
                    className="app-sidebar__thread-link"
                  >
                    <span className="app-sidebar__thread-title">{thread.title}</span>
                  </Link>
                  {onDeleteThread ? (
                    <button
                      type="button"
                      className="app-sidebar__thread-delete"
                      aria-label={`Delete conversation ${thread.title}`}
                      title="Delete conversation"
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        setThreadPendingDelete(thread);
                      }}
                    >
                      <Trash2 />
                    </button>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      </aside>

      <div className="app-shell__content">{children}</div>

      {threadPendingDelete && onDeleteThread ? (
        <div className="app-modal-backdrop" role="presentation" onClick={() => setThreadPendingDelete(null)}>
          <section
            className="app-confirm-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-conversation-title"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="app-confirm-modal__icon" aria-hidden="true">
              <Trash2 />
            </div>
            <div>
              <p className="app-confirm-modal__eyebrow">Delete conversation</p>
              <h2 id="delete-conversation-title" className="app-confirm-modal__title">
                Remove this chat?
              </h2>
              <p className="app-confirm-modal__copy">
                This will delete "{threadPendingDelete.title}" from your conversation history on this browser.
              </p>
            </div>
            <div className="app-confirm-modal__actions">
              <button type="button" className="app-confirm-modal__button" onClick={() => setThreadPendingDelete(null)}>
                Cancel
              </button>
              <button
                type="button"
                className="app-confirm-modal__button app-confirm-modal__button--danger"
                onClick={() => {
                  onDeleteThread(threadPendingDelete.id);
                  setThreadPendingDelete(null);
                }}
              >
                Delete
              </button>
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}
