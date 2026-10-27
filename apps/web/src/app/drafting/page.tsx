"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import { legalResearchContextSchema, canonicalIndexCodeOptions, type DraftConclusionsResponse } from "@beedle/shared";
import { copyTextToClipboard, dedupeIndexCodeOptions } from "@/lib/ui-helpers";
import { runDraftConclusions, runDraftExport } from "@/lib/api";
import { DraftReview } from "@/components/draft-review";
import { StatusPill } from "@/components/status-pill";

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
  const [lawJurisdiction,setLawJurisdiction]=useState("");
  const [lawDate,setLawDate]=useState("");
  const [lawEdition,setLawEdition]=useState("");
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
    const scope=lawJurisdiction.trim()||lawDate||lawEdition.trim()?legalResearchContextSchema.safeParse({jurisdiction:lawJurisdiction.trim()||undefined,as_of_date:lawDate||undefined,edition_id:lawEdition.trim()||undefined}):null;
    if(scope&&!scope.success){setError(scope.error.issues.map(issue=>issue.message).join("; "));return;}
    setLoading(true);
    setError(null);
    setActionMessage(null);

    try {
      const next = await runDraftConclusions({
        law_context:scope?.success?scope.data:undefined,
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

  async function exportReview(format: "markdown" | "text" | "html", copy = false) {
    if (!result) return;
    try {
      const payload = await runDraftExport({
        kind: "conclusions",
        format,
        document_title: "Conclusions of Law Draft",
        conclusions: result
      });
      if (copy) {
        const ok = await copyTextToClipboard(payload.content);
        setActionMessage(ok ? "Copied review package with evidence and limitations." : "Copy failed — clipboard unavailable.");
      } else {
        downloadFile(payload.filename, payload.mime_type, payload.content);
        setActionMessage(`Exported ${format} review package with evidence and limitations.`);
      }
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

            <fieldset><legend>Law edition context (optional)</legend>
              <p>Use a reviewed jurisdiction and applicable date to select historical law. Leaving them blank keeps edition applicability unknown. Date matching requires legal review.</p>
              <label className="field-label">Law jurisdiction<input aria-label="Law jurisdiction" value={lawJurisdiction} maxLength={200} onChange={event=>setLawJurisdiction(event.target.value)} /></label>
              <label className="field-label">Applicable law date<input aria-label="Applicable law date" type="date" value={lawDate} onChange={event=>setLawDate(event.target.value)} /></label>
              <label className="field-label">Exact edition ID (optional)<input aria-label="Exact law edition" value={lawEdition} maxLength={128} onChange={event=>setLawEdition(event.target.value)} /></label>
            </fieldset>
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
              <DraftReview result={result} />
              <p>Copy and exports include the draft, limitations, evidence, and source provenance. All output remains unreviewed.</p>
              <div style={{ display: "flex", flexWrap: "wrap", gap: "0.6rem" }}>
                <button type="button" onClick={() => void exportReview("text", true)} className="button-secondary">Copy review package</button>
                <button type="button" onClick={() => void exportReview("markdown")} className="button-secondary">Export Markdown review</button>
                <button type="button" onClick={() => void exportReview("text")} className="button-secondary">Export text review</button>
                <button type="button" onClick={() => void exportReview("html")} className="button-secondary">Export HTML review</button>
                <button type="button" onClick={() => downloadFile("draft-review.json", "application/json", JSON.stringify(result, null, 2))} className="button-secondary">Save review JSON</button>
              </div>
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
