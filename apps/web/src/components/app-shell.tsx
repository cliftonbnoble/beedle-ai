"use client";

import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Suspense, useEffect, useRef, useState, type ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { FilePenLine, FilePlus2, Gavel, Landmark, LayoutDashboard, Plus, Trash2, UserRoundPlus } from "lucide-react";
import {
  type ConversationThread
} from "@/components/assistant-thread-store";

import { useAssistantThreads, newAssistantThread, browserAssistantStore } from "@/components/assistant-thread-browser";

import { DeleteConversationDialog } from "@/components/delete-conversation-dialog";

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
  const chatStore=useAssistantThreads();
  const assistantThreads=chatStore.threads.slice(0,8);

  const activeThreadId = searchParams.get("thread") || "";

  function startNewAssistantThread() {
    const next = newAssistantThread();
    router.push(`/case-assistant?thread=${encodeURIComponent(next.id)}`);
  }

  function deleteAssistantThread(threadId: string) {
    browserAssistantStore().remove(threadId);
    const nextThread=browserAssistantStore().snapshot().threads[0] || newAssistantThread();

    if (pathname.startsWith("/case-assistant") && activeThreadId === threadId) {
      router.replace(nextThread ? `/case-assistant?thread=${encodeURIComponent(nextThread.id)}` : "/case-assistant?new=1");
    }
  }

  return (
    <AppShellFrame
      pathname={pathname}
      assistantThreads={assistantThreads}
      activeThreadId={activeThreadId}
      onNewThread={startNewAssistantThread}
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
  onNewThread,
  onDeleteThread
}: {
  children: ReactNode;
  pathname?: string;
  assistantThreads?: ConversationThread[];
  activeThreadId?: string;
  onNewThread?: () => void;
  onDeleteThread?: (threadId: string) => void;
}) {
  const newChatRef = useRef<HTMLButtonElement>(null);
  const [deleteNotice, setDeleteNotice] = useState("");
  const [threadPendingDelete, setThreadPendingDelete] = useState<ConversationThread | null>(null);

  useEffect(() => {
    if (!threadPendingDelete) return;
    const stillExists = assistantThreads.some((thread) => thread.id === threadPendingDelete.id);
    if (!stillExists) setThreadPendingDelete(null);
  }, [assistantThreads, threadPendingDelete]);

  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <Link href="/dashboard" className="app-sidebar__brand" aria-label="Go to Beedle AI home">
          <div className="app-sidebar__crest" aria-hidden="true">
            <Landmark />
          </div>
          <h1 className="app-sidebar__title">Beedle AI</h1>
        </Link>

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
            <button ref={newChatRef} type="button" className="app-sidebar__threads-new" onClick={onNewThread}>
              <span className="app-sidebar__icon" aria-hidden="true">
                <Plus />
              </span>
              <span className="app-sidebar__label">New chat</span>
            </button>
          </div>

          <p className="sr-only" role="status">{deleteNotice}</p>
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
        <DeleteConversationDialog key={threadPendingDelete.id} title={threadPendingDelete.title}
          fallbackFocus={newChatRef} onCancel={() => setThreadPendingDelete(null)}
          onDelete={() => {
            onDeleteThread(threadPendingDelete.id);
            setDeleteNotice(`Conversation ${threadPendingDelete.title} removed from active chat history.`);
            setThreadPendingDelete(null);
          }} />
      ) : null}
    </div>
  );
}
