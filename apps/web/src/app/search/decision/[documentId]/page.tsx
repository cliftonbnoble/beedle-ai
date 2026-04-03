"use client";

import { Suspense, type ReactNode, useEffect, useMemo, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";
import type { FileType, SearchResponse } from "@beedle/shared";
import { getDecisionRetrievalPreview, runSearch, type RetrievalPreviewResponse } from "@/lib/api";
import { friendlySectionLabel } from "../../ui-helpers";
import { repairDisplayText } from "../../text-cleanup";

function toSearchHref(searchParams: URLSearchParams) {
  const next = new URLSearchParams(searchParams.toString());
  next.delete("documentId");
  return `/search${next.toString() ? `?${next.toString()}` : ""}`;
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function getQueryTerms(query: string) {
  const terms = Array.from(
    new Set(
      query
        .split(/[\s,.;:()/"'-]+/)
        .map((term) => term.trim())
        .filter((term) => term.length >= 3)
    )
  );
  const hasLongTerm = terms.some((term) => term.length >= 4 && !/^\d+$/.test(term));
  return terms.filter((term) => term.length >= 4 || !hasLongTerm || /\d/.test(term));
}

function buildHighlightPatterns(query: string, terms: string[]) {
  const patterns: string[] = [];
  const phraseTerms = Array.from(
    new Set(
      query
        .split(/[\s,.;:()/"'-]+/)
        .map((term) => term.trim())
        .filter((term) => term.length >= 3)
    )
  );
  if (phraseTerms.length >= 2) {
    patterns.push(`(?<![A-Za-z0-9])${phraseTerms.map((term) => escapeRegExp(term)).join(`[\\s,.;:()/"'-]+`)}(?![A-Za-z0-9])`);
  }
  for (const term of terms) {
    patterns.push(`(?<![A-Za-z0-9])${escapeRegExp(term)}(?![A-Za-z0-9])`);
  }
  if (!patterns.length) return null;
  return new RegExp(patterns.join("|"), "gi");
}

function renderHighlightedText(text: string, query: string, terms: string[]) {
  const cleanedText = repairDisplayText(text, query);
  const pattern = buildHighlightPatterns(query, terms);
  if (!cleanedText || !pattern) return cleanedText;
  const parts: ReactNode[] = [];
  let lastIndex = 0;

  for (const match of cleanedText.matchAll(pattern)) {
    const index = match.index ?? -1;
    const matchedText = match[0] || "";
    if (index < 0 || !matchedText) continue;
    if (index > lastIndex) {
      parts.push(<span key={`text-${lastIndex}`}>{cleanedText.slice(lastIndex, index)}</span>);
    }
    parts.push(
      <mark
        key={`mark-${index}`}
        style={{ background: "rgba(239, 210, 88, 0.55)", padding: "0 0.08rem", borderRadius: "0.2rem" }}
      >
        {matchedText}
      </mark>
    );
    lastIndex = index + matchedText.length;
  }

  if (lastIndex < cleanedText.length) {
    parts.push(<span key={`text-${lastIndex}`}>{cleanedText.slice(lastIndex)}</span>);
  }

  return parts.length > 0 ? parts : cleanedText;
}

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

export default function DecisionDetailPage() {
  return (
    <Suspense fallback={<main className="page-shell"><section className="card" style={{ padding: "1.25rem" }}>Loading decision…</section></main>}>
      <DecisionDetailPageInner />
    </Suspense>
  );
}

function DecisionDetailPageInner() {
  const params = useParams<{ documentId: string }>();
  const searchParams = useSearchParams();
  const searchParamsKey = searchParams.toString();
  const documentId = String(params?.documentId || "");
  const query = searchParams.get("query") || "";
  const corpusMode = "trusted_plus_provisional";
  const approvedOnly = false;
  const judgeNames = useMemo(() => searchParams.getAll("judgeName").filter(Boolean), [searchParamsKey]);
  const indexCodes = useMemo(() => searchParams.getAll("indexCode").filter(Boolean), [searchParamsKey]);
  const jurisdiction = searchParams.get("jurisdiction") || undefined;
  const fileType = (searchParams.get("fileType") as FileType | null) || undefined;
  const rulesSection = searchParams.get("rulesSection") || undefined;
  const ordinanceSection = searchParams.get("ordinanceSection") || undefined;
  const partyName = searchParams.get("partyName") || undefined;
  const fromDate = searchParams.get("fromDate") || undefined;
  const toDate = searchParams.get("toDate") || undefined;
  const judgeNamesKey = judgeNames.join("|");
  const indexCodesKey = indexCodes.join("|");
  const [response, setResponse] = useState<SearchResponse | null>(null);
  const [preview, setPreview] = useState<RetrievalPreviewResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      if (!documentId || !query) return;
      setLoading(true);
      setError(null);
      try {
        const [next, previewDoc] = await Promise.all([
          runSearch({
            query,
            limit: 50,
            offset: 0,
            snippetMaxLength: 720,
            corpusMode,
            filters: {
              documentId,
              approvedOnly,
              jurisdiction,
              fileType,
              indexCodes: indexCodes.length > 0 ? indexCodes : undefined,
              rulesSection,
              ordinanceSection,
              partyName,
              judgeNames: judgeNames.length > 0 ? judgeNames : undefined,
              fromDate,
              toDate
            }
          }),
          getDecisionRetrievalPreview(documentId)
        ]);
        if (!cancelled) {
          setResponse(next);
          setPreview(previewDoc);
        }
      } catch (nextError) {
        if (!cancelled) setError(nextError instanceof Error ? nextError.message : "Failed to load decision details");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [
    approvedOnly,
    corpusMode,
    documentId,
    fileType,
    fromDate,
    indexCodesKey,
    judgeNamesKey,
    jurisdiction,
    ordinanceSection,
    partyName,
    query,
    rulesSection,
    toDate
  ]);

  const allRows = response?.results || [];
  const top = allRows[0] || null;
  const selectedJudgeName = judgeNames.length === 1 ? judgeNames[0] : "";
  const displayJudgeName = preview?.document.authorName || top?.authorName || selectedJudgeName || "Unknown";
  const queryTerms = useMemo(() => getQueryTerms(query), [query]);
  const previewChunks = preview?.chunks || [];
  const matchedChunkIds = useMemo(() => new Set(allRows.map((row) => row.chunkId)), [allRows]);
  const previewChunksBySection = useMemo(() => {
    const grouped = new Map<string, typeof previewChunks>();
    for (const chunk of previewChunks) {
      const list = grouped.get(chunk.provenance.sectionId) || [];
      list.push(chunk);
      grouped.set(chunk.provenance.sectionId, list);
    }
    return grouped;
  }, [previewChunks]);
  const matchedSectionCount = useMemo(
    () =>
      (preview?.document.sections || []).filter((section) => {
        const sectionChunks = previewChunksBySection.get(section.sectionId) || [];
        return sectionChunks.some((chunk) => matchedChunkIds.has(chunk.chunkId));
      }).length,
    [matchedChunkIds, preview?.document.sections, previewChunksBySection]
  );

  return (
    <main className="page-shell page-decision">
      <section className="card" style={{ padding: "1.25rem" }}>
        <p style={{ marginTop: 0, marginBottom: "0.75rem" }}>
          <a href={toSearchHref(searchParams)}>Back to search</a>
        </p>
        <h1 style={{ marginTop: 0, marginBottom: "0.35rem" }}>{top?.title || preview?.document.title || "Decision detail"}</h1>
        <p style={{ color: "var(--muted)", margin: 0 }}>
          {preview?.document.citation || top?.citation || "No citation available"}
        </p>
        <p style={{ color: "var(--muted)", margin: "0.35rem 0 0" }}>
          Judge: <strong>{displayJudgeName}</strong> · Decision date: <strong>{preview?.document.decisionDate || "n/a"}</strong>
        </p>
        {preview?.document.sourceLink ? (
          <p style={{ margin: "0.45rem 0 0" }}>
            <a href={preview.document.sourceLink} target="_blank" rel="noreferrer">
              Open source document
            </a>
          </p>
        ) : null}
      </section>

      {loading ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem" }}>
          <strong>Loading full decision...</strong>
        </section>
      ) : null}

      {error ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem", borderColor: "#f3a4a4" }}>
          <strong>Decision detail failed:</strong> {error}
        </section>
      ) : null}

      {!loading && !error ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem" }}>
          <div style={{ display: "grid", gap: "0.5rem", marginBottom: "1rem" }}>
            <h2 style={{ margin: 0, fontSize: "1rem" }}>Full decision</h2>
            <p style={{ margin: 0, color: "var(--muted)" }}>
              Highlighting reflects your current search for <strong>{query || "this decision"}</strong>.
            </p>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.45rem" }}>
              <span
                style={{
                  ...badgeStyle("gold"),
                  borderRadius: "999px",
                  padding: "0.24rem 0.5rem",
                  fontSize: "0.76rem",
                  fontWeight: 600
                }}
              >
                {matchedSectionCount} matched section{matchedSectionCount === 1 ? "" : "s"}
              </span>
              <span
                style={{
                  ...badgeStyle("neutral"),
                  borderRadius: "999px",
                  padding: "0.24rem 0.5rem",
                  fontSize: "0.76rem",
                  fontWeight: 600
                }}
              >
                {(preview?.document.sections || []).length} total section{(preview?.document.sections || []).length === 1 ? "" : "s"}
              </span>
            </div>
          </div>

          <div style={{ display: "grid", gap: "0.9rem" }}>
            {(preview?.document.sections || []).map((section) => {
              const sectionChunks = previewChunksBySection.get(section.sectionId) || [];
              const hasMatchedChunk = sectionChunks.some((chunk) => matchedChunkIds.has(chunk.chunkId));

              return (
                <article
                  key={section.sectionId}
                  style={{
                    border: `1px solid ${hasMatchedChunk ? "rgba(184, 137, 0, 0.24)" : "rgba(24, 38, 56, 0.10)"}`,
                    borderRadius: "12px",
                    padding: "0.9rem",
                    background: hasMatchedChunk ? "rgba(184, 137, 0, 0.04)" : "transparent"
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "0.75rem", flexWrap: "wrap", marginBottom: "0.55rem" }}>
                    <div>
                      <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.8rem" }}>
                        Section {section.sectionOrder + 1}
                      </p>
                      <h3 style={{ margin: "0.15rem 0 0", fontSize: "1rem" }}>
                        {friendlySectionLabel(section.heading || "Untitled section")}
                      </h3>
                    </div>
                    {hasMatchedChunk ? (
                      <span
                        style={{
                          ...badgeStyle("gold"),
                          borderRadius: "999px",
                          padding: "0.24rem 0.5rem",
                          fontSize: "0.76rem",
                          fontWeight: 600
                        }}
                      >
                        Matched by your search
                      </span>
                    ) : null}
                  </div>

                  <div style={{ display: "grid", gap: "0.65rem" }}>
                    {sectionChunks.map((chunk) => {
                      const isMatched = matchedChunkIds.has(chunk.chunkId);
                      return (
                        <div
                          key={chunk.chunkId}
                          style={{
                            borderLeft: isMatched ? "4px solid rgba(184, 137, 0, 0.85)" : "4px solid rgba(24, 38, 56, 0.12)",
                            paddingLeft: "0.75rem",
                            background: isMatched ? "rgba(184, 137, 0, 0.05)" : "transparent",
                            borderRadius: "0 8px 8px 0",
                            paddingTop: isMatched ? "0.22rem" : 0,
                            paddingBottom: isMatched ? "0.22rem" : 0
                          }}
                        >
                          <p style={{ margin: 0, whiteSpace: "pre-wrap", lineHeight: 1.68 }}>
                            {renderHighlightedText(chunk.sourceText, query, queryTerms)}
                          </p>
                        </div>
                      );
                    })}
                  </div>
                </article>
              );
            })}
          </div>
        </section>
      ) : null}
    </main>
  );
}
