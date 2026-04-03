"use client";

import { FormEvent, Suspense, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import { canonicalIndexCodeOptions, canonicalJudgeNames, type SearchResponse } from "@beedle/shared";
import { runSearch } from "@/lib/api";
import { DecisionSearchLoader } from "@/components/decision-search-loader";
import { StatusPill } from "@/components/status-pill";
import { friendlySectionLabel } from "./ui-helpers";
import { repairDisplayText } from "./text-cleanup";

function formatScore(score: number, topScore: number) {
  if (!Number.isFinite(score) || score <= 0 || !Number.isFinite(topScore) || topScore <= 0) return "0%";
  return `${Math.max(0, Math.min(100, Math.round((score / topScore) * 100)))}%`;
}

function formatDecisionDate(value?: string | null) {
  if (!value) return null;
  const date = new Date(`${value}T00:00:00`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(date);
}

function isConclusionsSection(value?: string) {
  return /conclusions? of law|authority_discussion|analysis_reasoning|order|disposition/i.test(String(value || ""));
}

function normalizeSnippetForComparison(value: string) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function areMeaningfullyDifferentSnippets(primary: string, candidate: string) {
  const left = normalizeSnippetForComparison(primary);
  const right = normalizeSnippetForComparison(candidate);
  if (!left || !right) return false;
  if (left === right) return false;
  if (left.includes(right) || right.includes(left)) return false;
  return true;
}

type AuthorityPreviewCandidate = {
  row: SearchResponse["results"][number];
  snippet: string;
};

function candidateKey(row: SearchResponse["results"][number]) {
  return `${row.chunkId}:${row.paragraphAnchor}`;
}

function buildEditorialPreview(primarySnippet: string, supplemental: AuthorityPreviewCandidate[]) {
  const includedKeys = new Set<string>();
  let previewText = primarySnippet.trim();

  for (const candidate of supplemental) {
    if (!candidate.snippet.trim()) continue;
    if (!areMeaningfullyDifferentSnippets(previewText, candidate.snippet)) continue;
    previewText = `${previewText} ${candidate.snippet.trim()}`.trim();
    includedKeys.add(candidateKey(candidate.row));
    if (previewText.length >= 520 || includedKeys.size >= 3) break;
  }

  return { previewText, includedKeys };
}

type SearchFilterState = {
  indexCodes: string[];
  rulesSection: string;
  ordinanceSection: string;
  partyName: string;
  judgeNames: string[];
  fromDate: string;
  toDate: string;
};

function buildSearchHref(
  query: string,
  corpusMode: "trusted_only" | "trusted_plus_provisional",
  limit: number,
  debugMode: boolean,
  filters: SearchFilterState,
  selectedDocumentId?: string
) {
  const params = new URLSearchParams();
  params.set("query", query);
  params.set("limit", String(limit));
  params.set("corpusMode", corpusMode);
  for (const indexCode of filters.indexCodes) params.append("indexCode", indexCode);
  if (filters.rulesSection) params.set("rulesSection", filters.rulesSection);
  if (filters.ordinanceSection) params.set("ordinanceSection", filters.ordinanceSection);
  if (filters.partyName) params.set("partyName", filters.partyName);
  for (const judgeName of filters.judgeNames) params.append("judgeName", judgeName);
  if (filters.fromDate) params.set("fromDate", filters.fromDate);
  if (filters.toDate) params.set("toDate", filters.toDate);
  if (debugMode) params.set("debugMode", "1");
  if (selectedDocumentId) params.set("selectedDocumentId", selectedDocumentId);
  return `/search?${params.toString()}`;
}

function buildDecisionHref(documentId: string, query: string, corpusMode: "trusted_only" | "trusted_plus_provisional", limit: number, filters: SearchFilterState) {
  const params = new URLSearchParams();
  params.set("query", query);
  params.set("corpusMode", corpusMode);
  params.set("limit", String(limit));
  params.set("approvedOnly", "0");
  params.set("selectedDocumentId", documentId);
  for (const indexCode of filters.indexCodes) params.append("indexCode", indexCode);
  if (filters.rulesSection) params.set("rulesSection", filters.rulesSection);
  if (filters.ordinanceSection) params.set("ordinanceSection", filters.ordinanceSection);
  if (filters.partyName) params.set("partyName", filters.partyName);
  for (const judgeName of filters.judgeNames) params.append("judgeName", judgeName);
  if (filters.fromDate) params.set("fromDate", filters.fromDate);
  if (filters.toDate) params.set("toDate", filters.toDate);
  return `/search/decision/${encodeURIComponent(documentId)}?${params.toString()}`;
}

function copyable(value: string) {
  return value && value.trim().length > 0 ? value : "n/a";
}

function currentLocalDateValue() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

const filterFieldStyle = {
  padding: "0.55rem 0.6rem",
  borderRadius: "9px",
  border: "1px solid rgba(24, 38, 56, 0.12)",
  width: "100%"
} as const;

const rowFourPanelHeight = 520;

function badgeStyle(tone: "gold" | "blue" | "green" | "neutral") {
  if (tone === "gold") {
    return { border: "1px solid #d6bf82", background: "rgba(184, 137, 0, 0.12)", color: "#6f5200" } as const;
  }
  if (tone === "blue") {
    return { border: "1px solid #a8c0dc", background: "rgba(20, 93, 160, 0.09)", color: "#174d82" } as const;
  }
  if (tone === "green") {
    return { border: "1px solid #a8cda6", background: "rgba(42, 125, 70, 0.10)", color: "#255e37" } as const;
  }
  return { border: "1px solid var(--border)", background: "rgba(80, 80, 80, 0.06)", color: "inherit" } as const;
}

function dedupeIndexCodeOptions(options: readonly (typeof canonicalIndexCodeOptions)[number][]) {
  const byCode = new Map<string, (typeof canonicalIndexCodeOptions)[number]>();
  for (const option of options) {
    const existing = byCode.get(option.code);
    if (!existing) {
      byCode.set(option.code, option);
      continue;
    }
    const existingReserved = existing.description.toLowerCase().includes("[reserved]");
    const nextReserved = option.description.toLowerCase().includes("[reserved]");
    if (existingReserved && !nextReserved) {
      byCode.set(option.code, option);
    }
  }
  return Array.from(byCode.values());
}

export default function SearchPage() {
  return (
    <Suspense fallback={<main className="page-shell"><section className="card" style={{ padding: "1.25rem" }}>Loading search…</section></main>}>
      <SearchPageInner />
    </Suspense>
  );
}

function SearchPageInner() {
  const searchParams = useSearchParams();
  const activeResultDocumentId = searchParams.get("selectedDocumentId") || "";
  const initialIndexCodes = searchParams.getAll("indexCode").filter(Boolean);
  const [query, setQuery] = useState(searchParams.get("query") || "");
  const corpusMode: "trusted_plus_provisional" = "trusted_plus_provisional";
  const [limit, setLimit] = useState(Math.max(1, Math.min(25, Number(searchParams.get("limit") || "12") || 12)));
  const [indexCodes, setIndexCodes] = useState<string[]>(initialIndexCodes);
  const [indexCodeFilterText, setIndexCodeFilterText] = useState("");
  const [rulesSection, setRulesSection] = useState(searchParams.get("rulesSection") || "");
  const [ordinanceSection, setOrdinanceSection] = useState(searchParams.get("ordinanceSection") || "");
  const [partyName, setPartyName] = useState(searchParams.get("partyName") || "");
  const [judgeNames, setJudgeNames] = useState<string[]>(searchParams.getAll("judgeName").filter(Boolean));
  const [indexFilterOpen, setIndexFilterOpen] = useState(false);
  const [judgeFilterOpen, setJudgeFilterOpen] = useState(false);
  const [fromDate, setFromDate] = useState(searchParams.get("fromDate") || "");
  const [toDate, setToDate] = useState(searchParams.get("toDate") || "");
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [response, setResponse] = useState<SearchResponse | null>(null);
  const [aggregatedResults, setAggregatedResults] = useState<SearchResponse["results"]>([]);
  const [copiedValue, setCopiedValue] = useState("");
  const [isCompactResultsLayout, setIsCompactResultsLayout] = useState(false);
  const [isSummaryCondensed, setIsSummaryCondensed] = useState(false);
  const [isSummaryUltraCondensed, setIsSummaryUltraCondensed] = useState(false);

  const canSubmit = useMemo(() => query.trim().length >= 2, [query]);
  const groupedByDecision = useMemo(() => {
    const groups = new Map<string, SearchResponse["results"]>();
    for (const row of aggregatedResults) {
      const current = groups.get(row.documentId) || [];
      current.push(row);
      groups.set(row.documentId, current);
    }
    return Array.from(groups.entries())
      .map(([documentId, rows]) => {
        const sorted = rows.slice().sort((a, b) => b.score - a.score);
        const top = sorted[0];
        if (!top) return null;
        return { documentId, top, rows: sorted };
      })
      .filter((row): row is { documentId: string; top: SearchResponse["results"][number]; rows: SearchResponse["results"] } => Boolean(row))
      .sort((a, b) => b.top.score - a.top.score);
  }, [aggregatedResults]);
  const canRequestDeeperResults = response?.hasMore ?? false;

  const topGroupedScore = groupedByDecision[0]?.top.score || 0;
  const activeReviewedGroup = useMemo(
    () => groupedByDecision.find((group) => group.documentId === activeResultDocumentId) || null,
    [activeResultDocumentId, groupedByDecision]
  );
  const dedupedIndexCodeOptions = useMemo(() => dedupeIndexCodeOptions(canonicalIndexCodeOptions), []);
  const effectiveIndexCodeSelection = useMemo(
    () => (indexCodes.length > 0 && indexCodes.length < dedupedIndexCodeOptions.length ? indexCodes : []),
    [dedupedIndexCodeOptions.length, indexCodes]
  );
  const effectiveJudgeSelection = useMemo(
    () => (judgeNames.length > 0 && judgeNames.length < canonicalJudgeNames.length ? judgeNames : []),
    [judgeNames]
  );
  const filteredIndexCodeOptions = useMemo(() => {
    const filter = indexCodeFilterText.trim().toLowerCase();
    if (!filter) return dedupedIndexCodeOptions;
    return dedupedIndexCodeOptions.filter((option) =>
      [option.code, option.description, option.ordinance, option.rules].some((value) =>
        value.toLowerCase().includes(filter)
      )
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
  const summaryChips = useMemo(() => {
    const chips: Array<{
      key: string;
      label: string;
      tone: "gold" | "blue" | "green" | "neutral";
      removable?: boolean;
      removeKind?: "index" | "judge" | "rules" | "ordinance" | "party" | "dates";
      value?: string;
    }> = [];
    if (query.trim()) chips.push({ key: `query-${query}`, label: `Query: ${query.trim()}`, tone: "gold" });
    if (effectiveJudgeSelection.length > 0) {
      for (const judgeName of effectiveJudgeSelection) {
        chips.push({
          key: `judge-${judgeName}`,
          label: `Judge: ${judgeName}`,
          tone: "green",
          removable: true,
          removeKind: "judge",
          value: judgeName
        });
      }
    }
    if (effectiveIndexCodeSelection.length > 0) {
      for (const code of effectiveIndexCodeSelection) {
        chips.push({
          key: `index-${code}`,
          label: `Index: ${code}`,
          tone: "blue",
          removable: true,
          removeKind: "index",
          value: code
        });
      }
    }
    if (rulesSection) chips.push({ key: `rules-${rulesSection}`, label: `R&R: ${rulesSection}`, tone: "blue", removable: true, removeKind: "rules" });
    if (ordinanceSection) {
      chips.push({
        key: `ord-${ordinanceSection}`,
        label: `Ordinance: ${ordinanceSection}`,
        tone: "blue",
        removable: true,
        removeKind: "ordinance"
      });
    }
    if (partyName) chips.push({ key: `party-${partyName}`, label: `Party: ${partyName}`, tone: "neutral", removable: true, removeKind: "party" });
    if (fromDate || toDate) {
      chips.push({
        key: `dates-${fromDate}-${toDate}`,
        label: `Dates: ${fromDate || "Any"} to ${toDate || "Any"}`,
        tone: "neutral",
        removable: true,
        removeKind: "dates"
      });
    }
    return chips;
  }, [effectiveIndexCodeSelection, effectiveJudgeSelection, fromDate, ordinanceSection, partyName, query, rulesSection, toDate]);

  useEffect(() => {
    function syncLayout() {
      setIsCompactResultsLayout(window.innerWidth < 860);
      setIsSummaryCondensed(window.scrollY > 140);
      setIsSummaryUltraCondensed(window.scrollY > 420);
    }

    syncLayout();
    window.addEventListener("resize", syncLayout);
    window.addEventListener("scroll", syncLayout, { passive: true });
    return () => {
      window.removeEventListener("resize", syncLayout);
      window.removeEventListener("scroll", syncLayout);
    };
  }, []);

  async function copyText(value: string) {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedValue(value);
      setTimeout(() => setCopiedValue(""), 1800);
    } catch {
      setCopiedValue("");
    }
  }

  function toggleJudgeName(judgeName: string) {
    setJudgeNames((current) =>
      current.includes(judgeName) ? current.filter((value) => value !== judgeName) : [...current, judgeName]
    );
  }

  function toggleIndexCode(indexCode: string) {
    setIndexCodes((current) =>
      current.includes(indexCode) ? current.filter((value) => value !== indexCode) : [...current, indexCode]
    );
  }

  function removeIndexCode(indexCode: string) {
    setIndexCodes((current) => current.filter((value) => value !== indexCode));
  }

  function removeJudgeName(judgeName: string) {
    setJudgeNames((current) => current.filter((value) => value !== judgeName));
  }

  async function executeSearch(nextResultLimit: number, options?: { offset?: number; append?: boolean }) {
    const offset = options?.offset ?? 0;
    const payload = {
      query,
      limit: nextResultLimit,
      offset,
      snippetMaxLength: 260,
      corpusMode,
      filters: {
        indexCodes: effectiveIndexCodeSelection.length > 0 ? effectiveIndexCodeSelection : undefined,
        rulesSection: rulesSection || undefined,
        ordinanceSection: ordinanceSection || undefined,
        partyName: partyName || undefined,
        judgeNames: effectiveJudgeSelection.length > 0 ? effectiveJudgeSelection : undefined,
        fromDate: fromDate || undefined,
        toDate: toDate || undefined,
        approvedOnly: false
      }
    };

    setError(null);
    const next = await runSearch(payload);
    if (options?.append) {
      setAggregatedResults((current) => {
        const merged = new Map(current.map((row) => [row.chunkId, row] as const));
        for (const row of next.results) merged.set(row.chunkId, row);
        return Array.from(merged.values());
      });
    } else {
      setAggregatedResults(next.results);
    }
    setResponse(next);
  }

  async function loadMoreDecisions() {
    if (loading || loadingMore) return;
    if (!canRequestDeeperResults) return;
    setLoadingMore(true);
    try {
      await executeSearch(limit, { offset: groupedByDecision.length, append: true });
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Unknown search failure");
    } finally {
      setLoadingMore(false);
    }
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    window.history.replaceState(
      {},
      "",
      buildSearchHref(query, corpusMode, limit, false, {
        indexCodes: effectiveIndexCodeSelection,
        rulesSection,
        ordinanceSection,
        partyName,
        judgeNames: effectiveJudgeSelection,
        fromDate,
        toDate
      })
    );

    try {
      await executeSearch(limit, { offset: 0 });
    } catch (nextError) {
      setAggregatedResults([]);
      setResponse(null);
      setError(nextError instanceof Error ? nextError.message : "Unknown search failure");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="page-shell page-search">
      <section className="page-hero">
        <div>
          <p className="page-eyebrow">Decision Retrieval</p>
          <h1 className="page-title" style={{ marginBottom: "0.25rem" }}>
            Open Search
          </h1>
          <p className="page-copy" style={{ marginTop: 0 }}>
            Search the decision corpus with natural language, judge filters, index codes, and cleaner result presentation.
          </p>
        </div>
        <StatusPill label="Search ready" />
      </section>

      <section className="card" style={{ padding: "1.2rem" }}>

        <form onSubmit={onSubmit} style={{ display: "grid", gap: "0.75rem" }}>
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search"
            aria-label="Search"
            style={{ padding: "0.75rem 0.8rem", borderRadius: "10px", border: "1px solid rgba(24, 38, 56, 0.14)" }}
          />

          <div style={{ display: "grid", gridTemplateColumns: isCompactResultsLayout ? "minmax(0, 1fr)" : "minmax(0, 320px)", gap: "0.3rem", alignItems: "start" }}>
            <span style={{ fontSize: "0.84rem", color: "var(--muted)" }}>Decisions to display</span>
            <input
              type="number"
              min={1}
              max={25}
              value={limit}
              onChange={(event) => setLimit(Math.max(1, Math.min(25, Number(event.target.value) || 12)))}
              aria-label="Decisions to display"
              style={filterFieldStyle}
            />
            <span style={{ fontSize: "0.8rem", color: "var(--muted)" }}>12 recommended. Higher numbers may take a little longer.</span>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(0, 1.2fr) minmax(0, 1.2fr) minmax(0, 1.2fr) minmax(170px, 0.7fr) minmax(170px, 0.7fr)",
              gap: "0.65rem",
              alignItems: "end"
            }}
          >
            <div style={{ display: "grid", gap: "0.3rem" }}>
              <span style={{ fontSize: "0.84rem", color: "var(--muted)" }}>R&R Section</span>
              <input
                value={rulesSection}
                onChange={(event) => setRulesSection(event.target.value)}
                placeholder="R&R Section"
                style={filterFieldStyle}
              />
            </div>
            <div style={{ display: "grid", gap: "0.3rem" }}>
              <span style={{ fontSize: "0.84rem", color: "var(--muted)" }}>Ordinance section</span>
              <input
                value={ordinanceSection}
                onChange={(event) => setOrdinanceSection(event.target.value)}
                placeholder="Ordinance section"
                style={filterFieldStyle}
              />
            </div>
            <div style={{ display: "grid", gap: "0.3rem" }}>
              <span style={{ fontSize: "0.84rem", color: "var(--muted)" }}>Party name</span>
              <input
                value={partyName}
                onChange={(event) => setPartyName(event.target.value)}
                placeholder="Party name"
                style={filterFieldStyle}
              />
            </div>
            <label style={{ display: "grid", gap: "0.3rem", fontSize: "0.84rem", color: "var(--muted)" }}>
              <span>From date</span>
              <input
                type="date"
                value={fromDate}
                onChange={(event) => setFromDate(event.target.value)}
                style={{ ...filterFieldStyle, color: "inherit" }}
              />
            </label>
            <label style={{ display: "grid", gap: "0.3rem", fontSize: "0.84rem", color: "var(--muted)" }}>
              <span style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "0.5rem" }}>
                <span>To date</span>
                <button
                  type="button"
                  onClick={() => setToDate(currentLocalDateValue())}
                  style={{
                    borderRadius: "999px",
                    border: "1px solid var(--border)",
                    background: "transparent",
                    padding: "0.18rem 0.45rem",
                    cursor: "pointer",
                    fontSize: "0.74rem",
                    color: "var(--muted)",
                    fontWeight: 600
                  }}
                >
                  {toDate === currentLocalDateValue() ? "Today selected" : "Use today"}
                </button>
              </span>
              <input
                type="date"
                value={toDate}
                onChange={(event) => setToDate(event.target.value)}
                style={{ ...filterFieldStyle, color: "inherit" }}
              />
            </label>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(0, 2fr) minmax(320px, 1fr)",
              gap: "0.75rem",
              alignItems: "start"
            }}
          >
            <div
              style={{
                border: "1px solid rgba(24, 38, 56, 0.10)",
                borderRadius: "10px",
                padding: "0.65rem",
                display: "flex",
                flexDirection: "column",
                height: indexFilterOpen ? rowFourPanelHeight : "auto",
                minHeight: indexFilterOpen ? rowFourPanelHeight : undefined
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "0.75rem", marginBottom: indexFilterOpen ? "0.5rem" : 0 }}>
                <div>
                  <div style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--muted)", marginBottom: "0.15rem" }}>Index code filter</div>
                  <div style={{ fontSize: "0.8rem", color: "var(--muted)", lineHeight: 1.2 }}>
                    {effectiveIndexCodeSelection.length === 0 ? "All index codes" : `${effectiveIndexCodeSelection.length} selected`}
                  </div>
                </div>
                <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
                  <button
                    type="button"
                    onClick={() => setIndexFilterOpen((current) => !current)}
                    aria-expanded={indexFilterOpen}
                    style={{ borderRadius: "8px", border: "1px solid var(--border)", background: "transparent", padding: "0.3rem 0.55rem", cursor: "pointer", fontSize: "0.8rem", fontWeight: 600 }}
                  >
                    {indexFilterOpen ? "▴ Close" : "▾ Open"}
                  </button>
                  <button
                    type="button"
                    onClick={() => setIndexCodes(dedupedIndexCodeOptions.map((option) => option.code))}
                    style={{ borderRadius: "8px", border: "1px solid var(--border)", background: "transparent", padding: "0.3rem 0.5rem", cursor: "pointer", fontSize: "0.8rem" }}
                  >
                    Select all
                  </button>
                  <button
                    type="button"
                    onClick={() => setIndexCodes([])}
                    style={{ borderRadius: "8px", border: "1px solid var(--border)", background: "transparent", padding: "0.3rem 0.5rem", cursor: "pointer", fontSize: "0.8rem" }}
                  >
                    Clear
                  </button>
                </div>
              </div>
              {indexFilterOpen ? (
                <>
                  <input
                    value={indexCodeFilterText}
                    onChange={(event) => setIndexCodeFilterText(event.target.value)}
                    placeholder="Filter index codes by code or description"
                    style={{ width: "100%", marginBottom: "0.5rem", padding: "0.5rem 0.55rem", borderRadius: "8px", border: "1px solid rgba(24, 38, 56, 0.12)" }}
                  />
                  <div style={{ display: "grid", gap: "0.55rem", overflowY: "auto", paddingRight: "0.2rem", flex: 1, minHeight: 0 }}>
                    {groupedIndexCodeOptions.map(([family, options]) => (
                      <details key={family} style={{ border: "1px solid rgba(24, 38, 56, 0.10)", borderRadius: "8px", padding: "0.35rem 0.4rem" }}>
                        <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                          {family} family · {options.length} code{options.length === 1 ? "" : "s"}
                        </summary>
                        <div style={{ display: "grid", gap: "0.4rem", marginTop: "0.55rem" }}>
                          {options.map((option) => {
                            const checked = indexCodes.includes(option.code);
                            return (
                              <label
                                key={option.code}
                                style={{
                                  display: "grid",
                                  gap: "0.15rem",
                                  padding: "0.42rem 0.45rem",
                                  borderRadius: "8px",
                                  background: checked ? "rgba(20, 93, 160, 0.08)" : "transparent",
                                  cursor: "pointer"
                                }}
                              >
                                <span style={{ display: "flex", alignItems: "center", gap: "0.55rem" }}>
                                  <input type="checkbox" checked={checked} onChange={() => toggleIndexCode(option.code)} />
                                  <strong>{option.code}</strong>
                                </span>
                                <span style={{ fontSize: "0.84rem" }}>{option.description}</span>
                              </label>
                            );
                          })}
                        </div>
                      </details>
                    ))}
                  </div>
                </>
              ) : null}
            </div>
            <div
              style={{
                border: "1px solid rgba(24, 38, 56, 0.10)",
                borderRadius: "10px",
                padding: "0.65rem",
                display: "flex",
                flexDirection: "column",
                height: judgeFilterOpen ? rowFourPanelHeight : "auto",
                minHeight: judgeFilterOpen ? rowFourPanelHeight : undefined
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "0.75rem", marginBottom: judgeFilterOpen ? "0.5rem" : 0 }}>
                <div>
                  <div style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--muted)", marginBottom: "0.15rem" }}>Judge filter</div>
                  <div style={{ fontSize: "0.8rem", color: "var(--muted)", lineHeight: 1.2 }}>
                    {effectiveJudgeSelection.length === 0 ? "All judges" : `${effectiveJudgeSelection.length} selected`}
                  </div>
                </div>
                <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
                  <button
                    type="button"
                    onClick={() => setJudgeFilterOpen((current) => !current)}
                    aria-expanded={judgeFilterOpen}
                    style={{ borderRadius: "8px", border: "1px solid var(--border)", background: "transparent", padding: "0.3rem 0.55rem", cursor: "pointer", fontSize: "0.8rem", fontWeight: 600 }}
                  >
                    {judgeFilterOpen ? "▴ Close" : "▾ Open"}
                  </button>
                  <button
                    type="button"
                    onClick={() => setJudgeNames([...canonicalJudgeNames])}
                    style={{ borderRadius: "8px", border: "1px solid var(--border)", background: "transparent", padding: "0.3rem 0.5rem", cursor: "pointer", fontSize: "0.8rem" }}
                  >
                    Select all
                  </button>
                  <button
                    type="button"
                    onClick={() => setJudgeNames([])}
                    style={{ borderRadius: "8px", border: "1px solid var(--border)", background: "transparent", padding: "0.3rem 0.5rem", cursor: "pointer", fontSize: "0.8rem" }}
                  >
                    Clear
                  </button>
                </div>
              </div>
              {judgeFilterOpen ? (
                <div style={{ display: "grid", gap: "0.35rem", flex: 1, overflowY: "auto", paddingRight: "0.2rem", minHeight: 0 }}>
                  {canonicalJudgeNames.map((judgeName) => {
                    const checked = judgeNames.includes(judgeName);
                    return (
                      <label
                        key={judgeName}
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: "0.55rem",
                          padding: "0.34rem 0.45rem",
                          borderRadius: "8px",
                          background: checked ? "rgba(20, 93, 160, 0.08)" : "transparent",
                          cursor: "pointer"
                        }}
                      >
                        <input type="checkbox" checked={checked} onChange={() => toggleJudgeName(judgeName)} />
                        <span>{judgeName}</span>
                      </label>
                    );
                  })}
                </div>
              ) : null}
            </div>
          </div>

          <button
            type="submit"
            disabled={!canSubmit || loading}
            style={{
              border: 0,
              borderRadius: "10px",
              padding: "0.75rem 1rem",
              background: "var(--accent)",
              color: "white",
              fontWeight: 600,
              cursor: canSubmit ? "pointer" : "not-allowed"
            }}
          >
            {loading ? "Searching..." : "Search"}
          </button>
        </form>
      </section>

      {!loading && !response && !error ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem" }}>
          <strong>Ready to search.</strong>
          <p style={{ marginBottom: 0, color: "var(--muted)" }}>
            Start with a natural research question and the app will return the strongest decision matches we currently have indexed.
          </p>
        </section>
      ) : null}

      {loading ? <DecisionSearchLoader /> : null}

      {error ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem", borderColor: "#f3a4a4" }}>
          <strong>Search failed:</strong> {error}
        </section>
      ) : null}

      {response ? (
        <section style={{ marginTop: "1rem", display: "grid", gap: "0.75rem" }}>
          <section
            className="card"
            style={{
              padding: isSummaryUltraCondensed ? "0.42rem 0.68rem" : isSummaryCondensed ? "0.55rem 0.8rem" : "0.8rem 0.95rem",
              borderColor: "rgba(24, 38, 56, 0.10)",
              background: "rgba(20, 93, 160, 0.08)",
              position: "sticky",
              top: "0.5rem",
              zIndex: 4,
              backdropFilter: "blur(12px)",
              boxShadow: "0 10px 24px rgba(24, 38, 56, 0.06)",
              transition: "padding 140ms ease, box-shadow 140ms ease, background 140ms ease"
            }}
          >
            <p
              style={{
                marginTop: 0,
                marginBottom: isSummaryUltraCondensed ? "0.18rem" : isSummaryCondensed ? "0.3rem" : "0.45rem",
                fontSize: isSummaryUltraCondensed ? "0.68rem" : isSummaryCondensed ? "0.74rem" : "0.8rem",
                fontWeight: 700,
                letterSpacing: "0.02em",
                color: "#174d82"
              }}
            >
              SEARCH SUMMARY
            </p>
            {summaryChips.some((chip) => chip.removable) ? (
              <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: isSummaryUltraCondensed ? "0.2rem" : isSummaryCondensed ? "0.3rem" : "0.45rem" }}>
                <button
                  type="button"
                  onClick={() => {
                    setIndexCodes([]);
                    setJudgeNames([]);
                    setRulesSection("");
                    setOrdinanceSection("");
                    setPartyName("");
                    setFromDate("");
                    setToDate("");
                  }}
                  style={{
                    borderRadius: "8px",
                    border: "1px solid var(--border)",
                    background: "transparent",
                    padding: isSummaryUltraCondensed ? "0.18rem 0.38rem" : isSummaryCondensed ? "0.24rem 0.45rem" : "0.3rem 0.55rem",
                    cursor: "pointer",
                    fontSize: isSummaryUltraCondensed ? "0.72rem" : isSummaryCondensed ? "0.76rem" : "0.8rem"
                  }}
                >
                  {isSummaryUltraCondensed ? "Clear filters" : "Clear selected filters"}
                </button>
              </div>
            ) : null}
            {activeReviewedGroup ? (
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  gap: "0.55rem",
                  flexWrap: "wrap",
                  marginBottom: isSummaryUltraCondensed ? "0.22rem" : "0.38rem",
                  padding: isSummaryUltraCondensed ? "0.22rem 0.32rem" : "0.32rem 0.42rem",
                  borderRadius: "10px",
                  border: "1px solid rgba(184, 137, 0, 0.16)",
                  background: "rgba(184, 137, 0, 0.07)"
                }}
              >
                <div style={{ minWidth: 0 }}>
                  <p
                    style={{
                      margin: 0,
                      fontSize: isSummaryUltraCondensed ? "0.68rem" : "0.74rem",
                      fontWeight: 700,
                      color: "#6f5200",
                      letterSpacing: "0.02em"
                    }}
                  >
                    CURRENTLY REVIEWING
                  </p>
                  <p
                    style={{
                      margin: "0.08rem 0 0",
                      fontSize: isSummaryUltraCondensed ? "0.76rem" : "0.82rem",
                      color: "var(--foreground)",
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      maxWidth: isCompactResultsLayout ? "100%" : "36rem"
                    }}
                  >
                    {activeReviewedGroup.top.title}
                  </p>
                </div>
                <a
                  href={`#result-${activeReviewedGroup.documentId}`}
                  style={{
                    borderRadius: "999px",
                    border: "1px solid rgba(184, 137, 0, 0.28)",
                    background: "rgba(255, 255, 255, 0.65)",
                    color: "#6f5200",
                    textDecoration: "none",
                    padding: isSummaryUltraCondensed ? "0.2rem 0.42rem" : "0.28rem 0.55rem",
                    fontSize: isSummaryUltraCondensed ? "0.72rem" : "0.78rem",
                    fontWeight: 700,
                    whiteSpace: "nowrap"
                  }}
                >
                  Jump back to this result
                </a>
              </div>
            ) : null}
            <div
              style={{
                display: "flex",
                flexWrap: isSummaryCondensed ? "nowrap" : "wrap",
                gap: isSummaryUltraCondensed ? "0.28rem" : isSummaryCondensed ? "0.35rem" : "0.45rem",
                overflowX: isSummaryCondensed ? "auto" : "visible",
                paddingBottom: isSummaryCondensed ? "0.1rem" : 0
              }}
            >
              {summaryChips.map((chip) => (
                chip.removable ? (
                <button
                  key={chip.key}
                  type="button"
                  onClick={() => {
                    if (chip.removeKind === "index" && chip.value) removeIndexCode(chip.value);
                    if (chip.removeKind === "judge" && chip.value) removeJudgeName(chip.value);
                    if (chip.removeKind === "rules") setRulesSection("");
                    if (chip.removeKind === "ordinance") setOrdinanceSection("");
                    if (chip.removeKind === "party") setPartyName("");
                    if (chip.removeKind === "dates") {
                      setFromDate("");
                      setToDate("");
                    }
                  }}
                  style={{
                    ...badgeStyle(chip.tone),
                    borderRadius: "999px",
                    padding: isSummaryUltraCondensed ? "0.17rem 0.4rem" : isSummaryCondensed ? "0.23rem 0.48rem" : "0.28rem 0.55rem",
                    fontSize: isSummaryUltraCondensed ? "0.7rem" : isSummaryCondensed ? "0.74rem" : "0.78rem",
                    fontWeight: 600,
                    cursor: "pointer",
                    whiteSpace: "nowrap"
                  }}
                >
                  {chip.label} ×
                </button>
                ) : (
                <span
                  key={chip.key}
                  style={{
                    ...badgeStyle(chip.tone),
                    borderRadius: "999px",
                    padding: isSummaryUltraCondensed ? "0.17rem 0.4rem" : isSummaryCondensed ? "0.23rem 0.48rem" : "0.28rem 0.55rem",
                    fontSize: isSummaryUltraCondensed ? "0.7rem" : isSummaryCondensed ? "0.74rem" : "0.78rem",
                    fontWeight: 600,
                    whiteSpace: "nowrap"
                  }}
                >
                  {chip.label}
                </span>
                )
              ))}
            </div>
          </section>
          <p style={{ margin: 0, color: "var(--muted)" }}>
            {groupedByDecision.length} ranked decision{groupedByDecision.length === 1 ? "" : "s"} currently loaded for "{response.query}".
          </p>
          <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.9rem" }}>
            Showing <strong>{groupedByDecision.length}</strong> ranked decision{groupedByDecision.length === 1 ? "" : "s"} so far, ordered by best score.
          </p>

          {response.total === 0 ? (
            <section className="card" style={{ padding: "1rem" }}>
              <strong>No results found.</strong>
              <p style={{ marginBottom: 0, color: "var(--muted)" }}>
                Try a more specific legal term or phrase. Very broad words like <code>the</code> are filtered now instead of returning noisy junk.
              </p>
            </section>
          ) : null}

          {groupedByDecision.map((group, index) => {
            const isTopResult = index === 0;
            const formattedDecisionDate = formatDecisionDate(group.top.decisionDate);
            const conclusionSnippet = repairDisplayText(
              group.top.primaryAuthorityPassage?.snippet || group.top.matchedPassage?.snippet || group.top.snippet,
              query
            );
            const authorityPreviewCandidates = group.rows
              .filter(
                (result, resultIndex) =>
                  resultIndex > 0 &&
                  isConclusionsSection(
                    result.primaryAuthorityPassage?.sectionLabel || result.matchedPassage?.sectionLabel || result.sectionLabel || result.chunkType || ""
                  )
              )
              .slice(0, 4)
              .map((result) => ({
                row: result,
                snippet: repairDisplayText(result.primaryAuthorityPassage?.snippet || result.matchedPassage?.snippet || result.snippet, query)
              }));
            const editorialPreview = buildEditorialPreview(conclusionSnippet, authorityPreviewCandidates);
            const remainingAuthorityRows = authorityPreviewCandidates
              .filter(({ row }) => !editorialPreview.includedKeys.has(candidateKey(row)))
              .map(({ row }) => row)
              .slice(0, 2);
            const previewLabel = editorialPreview.previewText.length >= 360
              ? "Expanded legal preview for this decision."
              : "Most relevant legal reasoning surfaced for this decision.";

            return (
            <article
              key={group.documentId}
              id={`result-${group.documentId}`}
              className="card"
              style={{
                padding: isTopResult ? "1.25rem" : "1.1rem",
                borderColor: activeResultDocumentId === group.documentId ? "#b88900" : isTopResult ? "rgba(184, 137, 0, 0.38)" : undefined,
                boxShadow:
                  activeResultDocumentId === group.documentId
                    ? "0 0 0 2px rgba(184, 137, 0, 0.14), 0 10px 28px rgba(184, 137, 0, 0.10)"
                    : isTopResult
                      ? "0 16px 34px rgba(24, 38, 56, 0.08), 0 0 0 1px rgba(184, 137, 0, 0.08)"
                    : undefined,
                background:
                  activeResultDocumentId === group.documentId
                    ? "rgba(184, 137, 0, 0.03)"
                    : isTopResult
                      ? "linear-gradient(180deg, rgba(184, 137, 0, 0.045) 0%, rgba(255, 255, 255, 0.98) 28%, rgba(255, 255, 255, 1) 100%)"
                      : undefined
              }}
            >
              <div
                style={
                  isCompactResultsLayout
                    ? { display: "grid", gridTemplateColumns: "minmax(0, 1fr)", gap: "0.8rem" }
                    : { display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "0.95rem", flexWrap: "wrap" }
                }
              >
                <div style={{ minWidth: 0, flex: "1 1 520px" }}>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem", marginBottom: "0.45rem", alignItems: "center" }}>
                    <span
                      style={{
                        ...badgeStyle(index === 0 ? "gold" : "neutral"),
                        borderRadius: "999px",
                        padding: "0.24rem 0.5rem",
                        fontSize: "0.74rem",
                        fontWeight: 700
                      }}
                    >
                      Result {index + 1}
                    </span>
                    {isTopResult ? (
                      <span
                        style={{
                          ...badgeStyle("gold"),
                          borderRadius: "999px",
                          padding: "0.24rem 0.5rem",
                          fontSize: "0.74rem",
                          fontWeight: 700
                        }}
                      >
                        Lead decision
                      </span>
                    ) : null}
                    {activeResultDocumentId === group.documentId ? (
                      <span
                        style={{
                          ...badgeStyle("gold"),
                          borderRadius: "999px",
                          padding: "0.24rem 0.5rem",
                          fontSize: "0.74rem",
                          fontWeight: 700
                        }}
                      >
                        Currently open in the reader
                      </span>
                    ) : null}
                  </div>
                  <h2 style={{ marginTop: 0, marginBottom: "0.28rem", fontSize: isCompactResultsLayout ? "1.05rem" : "1.12rem", lineHeight: 1.28 }}>
                    {group.top.title}
                  </h2>
                  <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.89rem", lineHeight: 1.45 }}>
                    <strong style={{ color: "var(--foreground)" }}>{group.top.citation}</strong> · Judge{" "}
                    <strong style={{ color: "var(--foreground)" }}>{group.top.authorName || "Unknown"}</strong>
                    {formattedDecisionDate ? (
                      <>
                        {" "}· <strong style={{ color: "var(--foreground)" }}>{formattedDecisionDate}</strong>
                      </>
                    ) : null}
                    {" "}·{" "}
                    <strong style={{ color: "var(--foreground)" }}>{group.rows.length}</strong> matched passage{group.rows.length === 1 ? "" : "s"}
                  </p>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem", marginTop: "0.45rem" }}>
                    <span
                      style={{
                        ...badgeStyle("neutral"),
                        borderRadius: "999px",
                        padding: "0.2rem 0.45rem",
                        fontSize: "0.72rem",
                        fontWeight: 600
                      }}
                    >
                      {group.top.corpusTier === "trusted" ? "Trusted source" : "Provisional source"}
                    </span>
                    <span
                      style={{
                        ...badgeStyle("neutral"),
                        borderRadius: "999px",
                        padding: "0.2rem 0.45rem",
                        fontSize: "0.72rem",
                        fontWeight: 600
                      }}
                    >
                      ID {group.documentId}
                    </span>
                  </div>
                </div>
                <div
                  style={
                    isCompactResultsLayout
                      ? {
                          display: "grid",
                          gridTemplateColumns: "minmax(0, 1fr)",
                          gap: "0.55rem",
                          width: "100%",
                          minWidth: 0
                        }
                      : {
                          flex: "0 0 220px",
                          minWidth: "220px",
                          display: "grid",
                          gap: "0.55rem",
                          justifyItems: "stretch"
                        }
                  }
                >
                  <div
                    style={{
                      border: isTopResult ? "1px solid rgba(184, 137, 0, 0.22)" : "1px solid rgba(20, 93, 160, 0.16)",
                      borderRadius: "12px",
                      background: isTopResult ? "rgba(184, 137, 0, 0.08)" : "rgba(20, 93, 160, 0.05)",
                      padding: isCompactResultsLayout ? "0.65rem 0.75rem" : "0.72rem"
                    }}
                  >
                    <p style={{ marginTop: 0, marginBottom: "0.18rem", fontSize: "0.76rem", fontWeight: 700, letterSpacing: "0.02em", color: isTopResult ? "#6f5200" : "#174d82" }}>
                      BEST SCORE
                    </p>
                    <p style={{ margin: 0, fontSize: "1.4rem", fontWeight: 700, lineHeight: 1 }}>{formatScore(group.top.score, topGroupedScore)}</p>
                    <p style={{ margin: "0.25rem 0 0", color: "var(--muted)", fontSize: "0.8rem" }}>
                      Best overall match in this decision
                    </p>
                  </div>
                  <a
                    href={buildDecisionHref(group.documentId, query, corpusMode, limit, {
                      indexCodes: effectiveIndexCodeSelection,
                      rulesSection,
                      ordinanceSection,
                      partyName,
                      judgeNames: effectiveJudgeSelection,
                      fromDate,
                      toDate
                    })}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      justifyContent: "center",
                      borderRadius: "10px",
                      background: "var(--accent)",
                      color: "white",
                      textDecoration: "none",
                      padding: isCompactResultsLayout ? "0.78rem 0.95rem" : "0.7rem 0.95rem",
                      fontWeight: 700,
                      width: "100%"
                    }}
                  >
                    Open Decision
                  </a>
                </div>
              </div>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "minmax(0, 1fr)",
                  gap: "0.75rem",
                  marginTop: "0.75rem"
                }}
              >
                <section
                  style={{
                    padding: isTopResult ? (isCompactResultsLayout ? "1rem" : "1.08rem 1.16rem") : isCompactResultsLayout ? "0.95rem" : "1rem 1.05rem",
                    borderRadius: "12px",
                    border: isTopResult ? "1px solid rgba(184, 137, 0, 0.24)" : "1px solid rgba(20, 93, 160, 0.18)",
                    background: isTopResult ? "linear-gradient(180deg, rgba(184, 137, 0, 0.08) 0%, rgba(184, 137, 0, 0.035) 100%)" : "rgba(20, 93, 160, 0.04)"
                  }}
                >
                  <p
                    style={{
                      marginTop: 0,
                      marginBottom: "0.28rem",
                      fontSize: isTopResult ? "0.82rem" : "0.8rem",
                      fontWeight: 700,
                      letterSpacing: "0.02em",
                      color: isTopResult ? "#6f5200" : "#174d82"
                    }}
                  >
                    CONCLUSIONS OF LAW
                  </p>
                  <p style={{ margin: 0, color: "var(--muted)", fontSize: isTopResult ? "0.86rem" : "0.84rem" }}>
                    {previewLabel}
                  </p>
                  <div style={{ display: "grid", gap: 0, marginTop: "0.62rem" }}>
                    <p
                      style={{
                        margin: 0,
                        lineHeight: isTopResult ? 1.78 : 1.68,
                        fontSize: isTopResult ? (isCompactResultsLayout ? "1.06rem" : "1.14rem") : isCompactResultsLayout ? "1rem" : "1.02rem",
                        fontWeight: isTopResult ? 500 : 450,
                        color: isTopResult ? "#162537" : "var(--foreground)"
                      }}
                    >
                      {editorialPreview.previewText}
                    </p>
                  </div>
                </section>
              </div>

              {remainingAuthorityRows.length > 0 ? (
                <section
                  style={{
                    marginTop: "0.75rem",
                    border: "1px solid rgba(24, 38, 56, 0.08)",
                    borderRadius: "12px",
                    padding: "0.8rem 0.85rem",
                    background: "rgba(24, 38, 56, 0.025)"
                  }}
                >
                  <p style={{ marginTop: 0, marginBottom: "0.45rem", fontSize: "0.82rem", fontWeight: 700, letterSpacing: "0.02em", color: "var(--foreground)" }}>
                    ADDITIONAL CONCLUSIONS OF LAW
                  </p>
                  <div style={{ display: "grid", gap: "0.55rem" }}>
                    {remainingAuthorityRows.map((result) => (
                      <div key={`${result.chunkId}:${result.paragraphAnchor}`} style={{ display: "grid", gap: "0.18rem" }}>
                        <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.78rem" }}>
                          {friendlySectionLabel(
                            result.primaryAuthorityPassage?.sectionLabel || result.matchedPassage?.sectionLabel || result.sectionLabel || result.chunkType || ""
                          )}
                        </p>
                        <p style={{ margin: 0, lineHeight: 1.52, fontSize: "0.92rem", color: "var(--foreground)" }}>
                          {repairDisplayText(result.primaryAuthorityPassage?.snippet || result.matchedPassage?.snippet || result.snippet, query)}
                        </p>
                      </div>
                    ))}
                  </div>
                </section>
              ) : null}
            </article>
            );
          })}

          {response.total > 0 ? (
            <section style={{ display: "grid", gap: "0.65rem", justifyItems: "center", padding: "0.5rem 0 0.75rem" }}>
              {canRequestDeeperResults ? (
                <>
                  <button
                    type="button"
                    onClick={() => void loadMoreDecisions()}
                    disabled={loadingMore}
                    style={{
                      borderRadius: "10px",
                      border: "1px solid var(--border)",
                      background: "transparent",
                      padding: "0.65rem 1rem",
                      cursor: loadingMore ? "progress" : "pointer"
                    }}
                  >
                    {loadingMore ? "Loading more decisions..." : `Load next ${limit} decisions`}
                  </button>
                </>
              ) : (
                <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.88rem" }}>
                  You’ve reached the end of the ranked decision list for this search.
                </p>
              )}
            </section>
          ) : null}
        </section>
      ) : null}
    </main>
  );
}
