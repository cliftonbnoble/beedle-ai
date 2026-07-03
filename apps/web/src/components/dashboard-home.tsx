"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import type { LucideIcon } from "lucide-react";
import { ArrowRight, FilePenLine, FilePlus2, ListChecks, Search, SearchCheck, Settings2, UserRoundPlus } from "lucide-react";
import { canonicalIndexCodeOptions, canonicalJudgeNames } from "@beedle/shared";
import { getDashboardSummary, type DashboardSummary } from "../lib/api";
import { StatusPill } from "./status-pill";

type CapabilityCard = {
  title: string;
  href: string;
  summary: string;
  accent: "blue" | "indigo" | "neutral";
  icon: LucideIcon;
};

const capabilityCards = [
  {
    title: "Open Search",
    href: "/search",
    summary: "Search decisions with natural language, index codes, judges, and legal structure filters in one place.",
    accent: "blue",
    icon: SearchCheck
  },
  {
    title: "Assistant",
    href: "/case-assistant",
    summary: "Ground findings and law together to surface guidance, vulnerabilities, and supporting authorities.",
    accent: "indigo",
    icon: UserRoundPlus
  },
  {
    title: "Drafting",
    href: "/drafting",
    summary: "Turn findings and citations into structured drafting support while keeping the same workflow logic.",
    accent: "neutral",
    icon: FilePenLine
  }
] satisfies CapabilityCard[];

const operationalLinks = [
  { title: "Retrieval diagnostics", href: "/admin/retrieval", summary: "Inspect ranking signals, query paths, and source passages.", icon: Search },
  { title: "Ingestion review", href: "/admin/ingestion", summary: "Review staged decisions and corpus readiness metadata.", icon: ListChecks },
  { title: "Reference audit", href: "/admin/references", summary: "Check citation normalization and unresolved legal references.", icon: Settings2 },
  { title: "Manual intake shell", href: "/add-decision", summary: "See the planned upload workflow while backend wiring remains deferred.", icon: FilePlus2 }
] satisfies Array<{ title: string; href: string; summary: string; icon: LucideIcon }>;

export default function DashboardHome() {
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [summaryFailed, setSummaryFailed] = useState(false);

  useEffect(() => {
    let cancelled = false;

    getDashboardSummary()
      .then((result) => {
        if (!cancelled) {
          setSummary(result);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSummary(null);
          setSummaryFailed(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main className="page-shell page-dashboard">
      <section className="page-hero">
        <div>
          <p className="page-eyebrow">Chamber Overview</p>
          <h2 className="page-title">Judicial Dashboard</h2>
          <p className="page-copy">
            A single place to move between decision search, case analysis, drafting, and manual decision intake.
          </p>
        </div>
        <StatusPill label="Workspace" />
      </section>

      <section className="hero-card dashboard-metrics">
        <div className="metric-card">
          <span className="metric-value">
            {summary ? summary.searchableDecisionCount.toLocaleString() : summaryFailed ? "unavailable" : "…"}
          </span>
          <span className="metric-label">Decisions searchable</span>
          <span className="metric-accent metric-accent--blue" />
        </div>
        <div className="metric-card">
          <span className="metric-value">{canonicalIndexCodeOptions.length.toLocaleString()}</span>
          <span className="metric-label">Index codes available</span>
          <span className="metric-accent metric-accent--azure" />
        </div>
        <div className="metric-card">
          <span className="metric-value">{canonicalJudgeNames.length.toLocaleString()}</span>
          <span className="metric-label">Judges available</span>
          <span className="metric-accent metric-accent--neutral" />
        </div>
      </section>

      <section className="dashboard-grid">
        <div className="dashboard-main-column">
          <article className="feature-spotlight">
            <div className="feature-spotlight__content">
              <div className="feature-icon">
                <Search />
              </div>
              <h3>Decision Search</h3>
              <p>
                Search the decision corpus with a calmer interface, clearer matching language, and strong retrieval support
                behind the scenes.
              </p>
              <Link href="/search" className="inline-link">
                Launch Search
                <ArrowRight />
              </Link>
            </div>
            <div className="feature-spotlight__visual">
              <div className="feature-preview-card">
                <span>Conclusions of Law</span>
                <strong>Best passage surfaced</strong>
                <p>Results stay readable while preserving the same search logic and filters you already rely on.</p>
              </div>
            </div>
          </article>

          <div className="capability-grid">
            {capabilityCards.map((card) => (
              <article key={card.title} className={"action-card action-card--" + card.accent}>
                <div className="feature-icon">
                  <card.icon />
                </div>
                <h3>{card.title}</h3>
                <p>{card.summary}</p>
                <Link href={card.href} className="inline-link">
                  Open {card.title}
                  <ArrowRight />
                </Link>
              </article>
            ))}
          </div>
        </div>

        <aside className="dashboard-side-column">
          <section className="workspace-card">
            <div>
              <div className="eyebrow-with-icon">
                <ListChecks />
                <p className="page-eyebrow page-eyebrow--light">Operational Views</p>
              </div>
              <h3>Review surfaces</h3>
              <p>Jump into the real admin and review tools that are currently wired in this workspace.</p>
            </div>
            <div className="workflow-list">
              {operationalLinks.map((item) => (
                <Link key={item.href} href={item.href} className="workflow-row">
                  <div className="workflow-row__meta">
                    <span className="workflow-row__icon" aria-hidden="true">
                      <item.icon />
                    </span>
                    <span>
                      <strong>{item.title}</strong>
                      <small>{item.summary}</small>
                    </span>
                  </div>
                  <ArrowRight aria-hidden="true" />
                </Link>
              ))}
            </div>
            <div className="workspace-card__footer">
              Dashboard totals come from live API data where available; unfinished workflows are labeled as planned.
            </div>
          </section>
        </aside>
      </section>
    </main>
  );
}
