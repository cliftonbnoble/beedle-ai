"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";
import type { FileType, SearchResponse } from "@beedle/shared";
import { getDecisionRetrievalPreview, runSearch, type RetrievalPreviewResponse } from "@/lib/api";
import { friendlySectionLabel } from "../../ui-helpers";
import { renderHighlightedSearchText } from "../../highlighting";

function toSearchHref(searchParams: URLSearchParams) {
  const next = new URLSearchParams(searchParams.toString());
  next.delete("documentId");
  return `/search${next.toString() ? `?${next.toString()}` : ""}`;
}

type RetrievalPreviewChunk = RetrievalPreviewResponse["chunks"][number];

type DisplayParagraph = {
  key: string;
  text: string;
  isMatched: boolean;
};

function parseParagraphAnchor(anchor: string) {
  const match = String(anchor || "").match(/^(.+)-p(\d+)$/);
  if (!match?.[1] || !match[2]) return null;
  return {
    prefix: match[1],
    index: Number.parseInt(match[2], 10)
  };
}

function splitChunkIntoParagraphs(chunk: RetrievalPreviewChunk) {
  const paragraphs = chunk.sourceText
    .split(/\n{2,}/)
    .map((paragraph) => paragraph.trim())
    .filter(Boolean);
  if (!paragraphs.length) return [];

  const start = parseParagraphAnchor(chunk.paragraphAnchorStart);
  const end = parseParagraphAnchor(chunk.paragraphAnchorEnd);
  const expectedCount = start && end && start.prefix === end.prefix ? end.index - start.index + 1 : 0;

  if (start && expectedCount === paragraphs.length) {
    return paragraphs.map((text, index) => ({
      anchor: `${start.prefix}-p${start.index + index}`,
      text
    }));
  }

  return paragraphs.map((text, index) => ({
    anchor: `${chunk.chunkId}-part-${index}`,
    text
  }));
}

function buildUniqueDisplayParagraphs(sectionChunks: RetrievalPreviewChunk[], matchedChunkIds: Set<string>): DisplayParagraph[] {
  const matchedParagraphAnchors = new Set<string>();

  for (const chunk of sectionChunks) {
    if (!matchedChunkIds.has(chunk.chunkId)) continue;
    for (const paragraph of splitChunkIntoParagraphs(chunk)) {
      matchedParagraphAnchors.add(paragraph.anchor);
    }
  }

  const seenParagraphAnchors = new Set<string>();
  const displayParagraphs: DisplayParagraph[] = [];

  for (const chunk of sectionChunks) {
    for (const paragraph of splitChunkIntoParagraphs(chunk)) {
      if (seenParagraphAnchors.has(paragraph.anchor)) continue;
      seenParagraphAnchors.add(paragraph.anchor);
      displayParagraphs.push({
        key: paragraph.anchor,
        text: paragraph.text,
        isMatched: matchedParagraphAnchors.has(paragraph.anchor)
      });
    }
  }

  return displayParagraphs;
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

          <div className="decision-reader">
            {(preview?.document.sections || []).map((section) => {
              const sectionChunks = previewChunksBySection.get(section.sectionId) || [];
              const hasMatchedChunk = sectionChunks.some((chunk) => matchedChunkIds.has(chunk.chunkId));
              const displayParagraphs = buildUniqueDisplayParagraphs(sectionChunks, matchedChunkIds);

              return (
                <article
                  key={section.sectionId}
                  className={`decision-reader__section${hasMatchedChunk ? " is-matched" : ""}`}
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

                  <div className="decision-reader__paragraphs">
                    {displayParagraphs.map((paragraph) => {
                      return (
                        <div
                          key={paragraph.key}
                          className={`decision-reader__paragraph-frame${paragraph.isMatched ? " is-matched" : ""}`}
                        >
                          <p className="decision-reader__paragraph">
                            {renderHighlightedSearchText(paragraph.text, query, {
                              markStyle: { background: "rgba(239, 210, 88, 0.55)", padding: "0 0.08rem", borderRadius: "0.2rem" }
                            })}
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
