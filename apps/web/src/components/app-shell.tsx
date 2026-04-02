"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { useEffect, useState, type ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { FilePenLine, FilePlus2, Gavel, Landmark, LayoutDashboard, Plus, UserRoundPlus } from "lucide-react";
import {
  assistantThreadsUpdatedEvent,
  loadAssistantThreads,
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
  const pathname = usePathname();
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
              <Plus size={13} />
              New chat
            </Link>
          </div>

          <div className="app-sidebar__threads-list">
            {assistantThreads.map((thread) => {
              const active = pathname.startsWith("/case-assistant") && activeThreadId === thread.id;
              return (
                <Link
                  key={thread.id}
                  href={`/case-assistant?thread=${encodeURIComponent(thread.id)}`}
                  className={`app-sidebar__thread-link${active ? " is-active" : ""}`}
                >
                  <span className="app-sidebar__thread-title">{thread.title}</span>
                </Link>
              );
            })}
          </div>
        </div>
      </aside>

      <div className="app-shell__content">{children}</div>
    </div>
  );
}
