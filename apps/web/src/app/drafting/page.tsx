"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import { canonicalIndexCodeOptions, type DraftConclusionsResponse } from "@beedle/shared";
import { runDraftConclusions, runDraftExport } from "@/lib/api";
import { StatusPill } from "@/components/status-pill";

function dedupeIndexCodeOptions(options: typeof canonicalIndexCodeOptions) {
  const byCode = new Map<string, (typeof canonicalIndexCodeOptions)[number]>();
  for (const option of options) {
    if (!byCode.has(option.code)) byCode.set(option.code, option);
  }
  return Array.from(byCode.values());
}

function confidenceTone(confidence: DraftConclusionsResponse["confidence"]) {
  if (confidence === "high") return "#1f6d4f";
  if (confidence === "medium") return "#8b6e1f";
  return "#8b2a2a";
}

async function copyText(value: string) {
  await navigator.clipboard.writeText(value);
}

function downloadFile(filename: string, mimeType: string, content: string) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

export default function DraftingPage() {
  const [findingsText, setFindingsText] = useState("");
  const [lawText, setLawText] = useState("");
  const [indexCodes, setIndexCodes] = useState<string[]>([]);
  const [indexCodeFilterText, setIndexCodeFilterText] = useState("");
  const [isIndexCodeModalOpen, setIsIndexCodeModalOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [result, setResult] = useState<DraftConclusionsResponse | null>(null);

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
  const canRun = useMemo(() => findingsText.trim().length >= 12, [findingsText]);

  useEffect(() => {
    if (!isIndexCodeModalOpen) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [isIndexCodeModalOpen]);

  function addIndexCode(code: string) {
    setIndexCodes((current) => (current.includes(code) ? current : [...current, code]));
  }

  function removeIndexCode(code: string) {
    setIndexCodes((current) => current.filter((value) => value !== code));
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    setActionMessage(null);

    try {
      const next = await runDraftConclusions({
        findings_text: findingsText,
        law_text: lawText,
        index_codes: indexCodes,
        rules_sections: [],
        ordinance_sections: [],
        uploaded_doc_ids: [],
        issue_tags: []
      });
      setResult(next);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Unexpected draft generation error");
    } finally {
      setLoading(false);
    }
  }

  async function exportMarkdown() {
    if (!result) return;
    try {
      const payload = await runDraftExport({
        kind: "conclusions",
        format: "markdown",
        document_title: "Conclusions of Law Draft",
        conclusions: result
      });
      downloadFile(payload.filename, payload.mime_type, payload.content);
      setActionMessage("Exported draft as markdown.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Export failed");
    }
  }

  return (
    <main className="page-shell drafting-layout drafting-layout--wide">
      <section className="page-hero drafting-hero">
        <div>
          <p className="page-eyebrow">Drafting Workspace</p>
          <h2 className="page-title">Drafting</h2>
        </div>
        <StatusPill label="AI system drafting" />
      </section>

      <section className="workspace-grid drafting-workspace-grid">
        <section className="card form-card">
          <form onSubmit={onSubmit} className="form-grid drafting-form-grid">
            <label className="field-label">
              <span>Findings of Fact</span>
              <textarea
                value={findingsText}
                onChange={(event) => setFindingsText(event.target.value)}
                rows={10}
                className="field-textarea"
              />
            </label>

            <label className="field-label">
              <span>Relevant Law / Citations (optional)</span>
              <textarea
                value={lawText}
                onChange={(event) => setLawText(event.target.value)}
                rows={6}
                className="field-textarea"
                placeholder="Paste any ordinance language, rules, citations, or leave this blank."
              />
            </label>

            <div className="field-label drafting-index-field">
              <div className="drafting-index-field__header">
                <span>Index Codes</span>
                <button type="button" className="drafting-inline-button" onClick={() => setIsIndexCodeModalOpen(true)}>
                  + Add index codes
                </button>
              </div>
              <div className="drafting-index-field__shell">
                {indexCodes.length > 0 ? (
                  <>
                    <div className="drafting-index-field__chips">
                      {indexCodes.map((code) => (
                        <span key={code} className="drafting-index-chip">
                          <span>{code}</span>
                          <button type="button" onClick={() => removeIndexCode(code)} aria-label={`Remove ${code}`}>
                            ×
                          </button>
                        </span>
                      ))}
                    </div>
                    <div className="drafting-index-field__summary">{indexCodes.join(", ")}</div>
                  </>
                ) : (
                  <div className="drafting-index-field__empty">No index codes selected yet.</div>
                )}
              </div>
            </div>

            <button type="submit" disabled={!canRun || loading} className="button-primary">
              {loading ? "Generating..." : "Generate Conclusions Draft"}
            </button>
          </form>

          {error ? <p className="feedback-error">Error: {error}</p> : null}
          {actionMessage ? <p className="feedback-success">{actionMessage}</p> : null}
        </section>

        <section className="card result-card">
          {!result && !error ? <p className="result-placeholder">Generate a draft to review the output.</p> : null}
          {result ? (
            <div className="form-grid drafting-result-grid">
              <h2>Draft review</h2>
              <p style={{ margin: 0 }}>
                Confidence: <strong style={{ color: confidenceTone(result.confidence) }}>{result.confidence.toUpperCase()}</strong>
              </p>
              <div style={{ display: "flex", flexWrap: "wrap", gap: "0.6rem" }}>
                <button
                  type="button"
                  onClick={() => copyText(result.draft_text).then(() => setActionMessage("Copied full draft."))}
                  className="button-secondary"
                >
                  Copy full draft
                </button>
                <button type="button" onClick={() => void exportMarkdown()} className="button-secondary">
                  Export markdown
                </button>
              </div>
              <pre className="text-output">{result.draft_text}</pre>
            </div>
          ) : null}
        </section>
      </section>

      {isIndexCodeModalOpen ? (
        <div className="drafting-index-modal__backdrop" onClick={() => setIsIndexCodeModalOpen(false)}>
          <div className="drafting-index-modal" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label="Add index codes">
            <div className="drafting-index-modal__header">
              <div>
                <h3>Add Index Codes</h3>
                <p>Search by code or description, then add the ones you want into this draft.</p>
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
                {indexCodes.length > 0 ? `Selected: ${indexCodes.join(", ")}` : "Select one or more index codes to attach to this draft."}
              </div>
              <button type="button" className="button-secondary" onClick={() => setIsIndexCodeModalOpen(false)}>
                Done
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </main>
  );
}
