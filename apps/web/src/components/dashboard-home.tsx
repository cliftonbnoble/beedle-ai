"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import type { LucideIcon } from "lucide-react";
import { ArrowRight, BrainCircuit, FilePenLine, Scale, Search, SearchCheck, Sparkles, UserRoundPlus } from "lucide-react";
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

const aiModels = [
  { name: "Claude Opus 3.6", status: "Active", icon: Sparkles },
  { name: "Gemini Pro 3.1", status: "Standby", icon: BrainCircuit },
  { name: "GPT-4O Legal", status: "Standby", icon: Scale }
] satisfies Array<{ name: string; status: "Active" | "Standby"; icon: LucideIcon }>;

export default function DashboardHome() {
  const [summary, setSummary] = useState<DashboardSummary | null>(null);

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
        <StatusPill label="Systems operational" />
      </section>

      <section className="hero-card dashboard-metrics">
        <div className="metric-card">
          <span className="metric-value">
            {summary ? summary.searchableDecisionCount.toLocaleString() : "…"}
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

        <div className="dashboard-chart" aria-hidden="true">
          {[42, 58, 49, 63, 61, 74, 68, 88, 72, 65, 54, 44].map((value, index) => (
            <span key={index} style={{ height: value + "%" }} />
          ))}
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
          <section className="intelligence-card">
            <div>
              <div className="eyebrow-with-icon">
                <Sparkles />
                <p className="page-eyebrow page-eyebrow--light">AI Intelligence</p>
              </div>
              <h3>Model readiness</h3>
              <p>
                Connected to legal-specialized neural architectures for deep semantic parsing and drafting support.
              </p>
            </div>
            <div className="model-list">
              {aiModels.map((model) => (
                <div key={model.name} className="model-row">
                  <div className="model-row__meta">
                    <span className="model-row__icon" aria-hidden="true">
                      <model.icon />
                    </span>
                    <span className="model-row__name">{model.name}</span>
                  </div>
                  <strong className={`status-badge ${model.status === "Active" ? "status-badge--active" : "status-badge--standby"}`}>
                    {model.status}
                  </strong>
                </div>
              ))}
            </div>
            <div className="intelligence-card__footer">
              AI output remains advisory. Judicial decisions remain fully user-controlled.
            </div>
          </section>
        </aside>
      </section>
    </main>
  );
}
