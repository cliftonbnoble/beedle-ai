"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";
import type { FileType, SearchResponse } from "@beedle/shared";
import { getDecisionRetrievalPreview, runSearch, type RetrievalPreviewResponse } from "@/lib/api";
import { friendlySectionLabel } from "../../ui-helpers";
import { renderHighlightedSearchText } from "../../highlighting";
import { matchedPreviewChunks, parseParagraphAnchor } from "@/lib/decision-passages";

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
      <DecisionDetailRoute />
    </Suspense>
  );
}

function DecisionDetailRoute() {
  const params = useParams<{ documentId: string }>();
  const documentId = String(params?.documentId || "");
  // A new identity gets a fresh pane before effects run; stale document text cannot flash.
  return <DecisionDetailPageInner key={documentId} documentId={documentId} />;
}

function DecisionDetailPageInner({ documentId }: { documentId: string }) {
  const searchParams = useSearchParams();
  const searchParamsKey = searchParams.toString();
  const query = searchParams.get("query") || "";
  const corpusMode = searchParams.get("corpusMode")==="trusted_only"?"trusted_only":"trusted_plus_provisional";
  const approvedOnly = searchParams.get("approvedOnly")==="1";
  const judgeNames = useMemo(() => searchParams.getAll("judgeName").filter(Boolean), [searchParamsKey]);
  const indexCodes = useMemo(() => searchParams.getAll("indexCode").filter(Boolean), [searchParamsKey]);
  const indexCodeOperator = searchParams.get("indexCodeOperator") === "or" ? "or" : "and";
  const jurisdiction = searchParams.get("jurisdiction") || undefined;
  const fileType = (searchParams.get("fileType") as FileType | null) || undefined;
  const rulesSection = searchParams.get("rulesSection") || undefined;
  const ordinanceSection = searchParams.get("ordinanceSection") || undefined;
  const partyName = searchParams.get("partyName") || undefined;
  const decisionSeries = searchParams.get("decisionSeries")==="T"?"T":searchParams.get("decisionSeries")==="L"?"L":undefined;
  const fromDate = searchParams.get("fromDate") || undefined;
  const toDate = searchParams.get("toDate") || undefined;
  const [preview, setPreview] = useState<RetrievalPreviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [previewAttempt, setPreviewAttempt] = useState(0);
  const [contextAttempt, setContextAttempt] = useState(0);
  const [context, setContext] = useState<{key: string; response?: SearchResponse; error?: string; stale?: boolean; loading: boolean} | null>(null);
  const contextKey = JSON.stringify([documentId, preview?.corpusRevision ?? null, query, corpusMode, approvedOnly, jurisdiction, fileType,
    indexCodes, indexCodeOperator, rulesSection, ordinanceSection, partyName, decisionSeries, judgeNames, fromDate, toDate]);
  // Ignore old filter/query state synchronously, including the render before effect cleanup.
  const activeContext = context?.key === contextKey ? context : null;
  const response = query ? activeContext?.response : undefined;

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError(null); setPreview(null);
    async function loadDocument() {
      try {
        if (!documentId) throw new Error("No decision was selected.");
        const next = await getDecisionRetrievalPreview(documentId, { signal: controller.signal });
        if (!controller.signal.aborted) setPreview(next);
      } catch (nextError) {
        if (!controller.signal.aborted) setError(nextError instanceof Error ? nextError.message : "Failed to load decision details");
      } finally { if (!controller.signal.aborted) setLoading(false); }
    }
    void loadDocument();
    return () => controller.abort();
  }, [documentId, previewAttempt]);

  useEffect(() => {
    const controller = new AbortController();
    // Optional retrieval starts only after the server has returned this published document.
    if (!preview || !query.trim()) { setContext(null); return () => controller.abort(); }
    setContext({ key: contextKey, loading: true });
    async function loadMatches() {
      try {
        const next = await runSearch({
          query, limit: 50, offset: 0, snippetMaxLength: 720, corpusMode,
          filters: { documentId, approvedOnly, jurisdiction, fileType,
            indexCodes: indexCodes.length > 0 ? indexCodes : undefined,
            indexCodeOperator: indexCodes.length > 0 ? indexCodeOperator : undefined,
            rulesSection, ordinanceSection, partyName, decisionSeries,
            judgeNames: judgeNames.length > 0 ? judgeNames : undefined, fromDate, toDate }
        }, { signal: controller.signal });
        if (!controller.signal.aborted) {
          if (!preview?.corpusRevision || next.corpusRevision !== preview.corpusRevision) {
            setContext({ key: contextKey, loading: false, stale: true,
              error: "The decision or corpus changed. Reload the decision before using search matches." });
          } else setContext({ key: contextKey, loading: false, response: next });
        }
      } catch {
        if (!controller.signal.aborted) setContext({ key: contextKey, loading: false,
          error: "Try again, or return to search to check the query and filters." });
      }
    }
    void loadMatches();
    return () => controller.abort();
  }, [contextKey, contextAttempt, preview]);

  useEffect(() => {
    if (!preview || !window.location.hash) return;
    try { document.getElementById(decodeURIComponent(window.location.hash.slice(1)))?.scrollIntoView({ block: "start" }); }
    catch { /* An invalid URL fragment cannot prevent reading the document. */ }
  }, [preview]);

  const allRows = useMemo(() => response?.results.filter(row => row.documentId === documentId) || [], [response, documentId]);
  const displayJudgeName = preview?.document.authorName || "Unknown";
  const previewChunks = preview?.chunks || [];
  const matchedChunkIds = useMemo(() => matchedPreviewChunks(previewChunks, allRows, documentId), [allRows, previewChunks, documentId]);
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
        <h1 style={{ marginTop: 0, marginBottom: "0.35rem" }}>{preview?.document.title || "Decision detail"}</h1>
        <p style={{ color: "var(--muted)", margin: 0 }}>
          {preview?.document.citation || "No citation available"}
        </p>
        <p style={{ color: "var(--muted)", margin: "0.35rem 0 0" }}>
          Judge: <strong>{displayJudgeName}</strong> · Decision date: <strong>{preview?.document.decisionDate || "n/a"}</strong>
        </p>
        {preview ? <p role="note" aria-label="Review status" style={{ margin: "0.75rem 0 0", color: "var(--muted)" }}>
          {preview.document.corpusTier === "trusted" ? "Human-approved current revision." :
            preview.document.corpusTier === "provisional" ? "Provisional — not human approved. Extracted text and metadata have not been confirmed; check the original source." :
            preview.document.corpusTier === "held" ? "Held for review. This inspection does not grant publication or approval." :
            "Review status unavailable. Check the original source before relying on extracted text or metadata."}
        </p> : null}
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
          <strong role="status">Loading full decision...</strong>
        </section>
      ) : null}

      {error ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem", borderColor: "#f3a4a4" }}>
          <p role="alert"><strong>Decision detail failed:</strong> {error}</p>
          <button type="button" onClick={() => setPreviewAttempt(value => value + 1)}>Retry decision</button>
        </section>
      ) : null}

      {preview && query.trim() ? <section className="card" aria-label="Search context" style={{padding:"1rem",marginTop:"1rem"}}>
        <p role="status">{!activeContext || activeContext.loading ? "Loading search matches… The full decision is available below." :
          activeContext.error ? `Search matches are unavailable. You can still read the full decision. ${activeContext.error}` :
          matchedChunkIds.size ? "Search matches loaded for the current query and filters." : "No passages matched the current query and filters. The full decision is available below."}</p>
        {activeContext?.error ? activeContext.stale ?
          <button type="button" onClick={() => setPreviewAttempt(value => value + 1)}>Reload decision</button> :
          <button type="button" onClick={() => setContextAttempt(value => value + 1)}>Retry search matches</button> : null}
      </section> : null}

      {preview && !loading && !error ? (
        <section className="card" style={{ padding: "1rem", marginTop: "1rem" }}>
          <div style={{ display: "grid", gap: "0.5rem", marginBottom: "1rem" }}>
            <h2 style={{ margin: 0, fontSize: "1rem" }}>Full decision</h2>
            <p style={{ margin: 0, color: "var(--muted)" }}>
              {query.trim() ? <>Search-term highlighting for <strong>{query.length <= 2000 ? query : "an invalid query"}</strong>. Matched-passage indicators load separately.</> : "Reading the published decision without search context."}
            </p>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.45rem" }}>
              {response ? <span
                style={{
                  ...badgeStyle("gold"),
                  borderRadius: "999px",
                  padding: "0.24rem 0.5rem",
                  fontSize: "0.76rem",
                  fontWeight: 600
                }}
              >
                {matchedSectionCount} matched section{matchedSectionCount === 1 ? "" : "s"}
              </span> : null}
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

          {matchedChunkIds.size > 0 ? <nav aria-label="Matched passages"><ul>
            {previewChunks.filter(chunk => matchedChunkIds.has(chunk.chunkId)).map(chunk => <li key={chunk.chunkId}>
              <a href={`#${encodeURIComponent(splitChunkIntoParagraphs(chunk)[0]?.anchor || chunk.paragraphAnchorStart)}`}>Jump to {friendlySectionLabel(chunk.sectionLabel || "matched passage")}</a>
            </li>)}
          </ul></nav> : null}

          <div className="decision-reader">
            {(preview?.document.sections || []).map((section) => {
              const sectionChunks = previewChunksBySection.get(section.sectionId) || [];
              const hasMatchedChunk = sectionChunks.some((chunk) => matchedChunkIds.has(chunk.chunkId));
              const displayParagraphs = buildUniqueDisplayParagraphs(sectionChunks, matchedChunkIds);

              return (
                <article
                  key={section.sectionId}
                  id={section.sectionId}
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
                          id={paragraph.key}
                          className={`decision-reader__paragraph-frame${paragraph.isMatched ? " is-matched" : ""}`}
                        >
                          <p className="decision-reader__paragraph">
                            {renderHighlightedSearchText(paragraph.text, query.length <= 2000 ? query : "", {
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
