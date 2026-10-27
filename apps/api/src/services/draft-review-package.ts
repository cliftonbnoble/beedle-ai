import type { DraftConclusionsResponse } from "@beedle/shared";

export function reviewPackageSections(input: DraftConclusionsResponse): Array<{ heading: string; text: string }> {
  const sourceLines = [...new Map([...input.supporting_authorities.map(row => [row.citation_id, {
    reference_edition:row.reference_edition,
    id: row.citation_id, title: row.title, citation: row.citation, citation_anchor: row.citation_anchor,
    source_link: row.source_link, snippet: row.snippet, kind: (row.source_kind || "legacy source") as string
  }] as const), ...input.citations.filter(row => !input.supporting_authorities.some(source => source.citation_id === row.id)).map(row => [row.id, { ...row, kind: "legacy citation",reference_edition:undefined }] as const)]).values()]
    .map(source => `${source.id}\n${source.kind}: ${source.title} — ${source.citation}\nAnchor: ${source.citation_anchor}\nSource: ${source.source_link}\nCaptured excerpt: ${source.snippet}\nEdition metadata (legal review required): ${JSON.stringify(source.reference_edition||{status:"unknown",reason:"legacy_or_unversioned_source"})}`);
  const provenance = input.provenance?.sources.map(source => `${source.citation_id}: document ${source.document_id}; revision ${source.document_revision || "unavailable"}; revision status ${source.revision_status || "unavailable"}; extraction ${source.extraction_version || "unavailable"}; excerpt SHA-256 ${source.excerpt_sha256}`).join("\n") || "Source version provenance unavailable for this historical draft.";
  const paragraphs = input.paragraph_support.map(row => [
    `${row.paragraph_id} — ${row.support_level} (human review required)`, row.text,
    `Candidate source IDs: ${row.citation_ids.join(", ") || "none"}`, ...row.support_notes,
    ...row.claims.flatMap(claim => [`${claim.claim_id}: ${claim.kind}; ${claim.status}\n${claim.text}`,
      `Flags: ${claim.flags.join(", ") || "none detected; this is not a verification"}`,
      ...claim.evidence.map(span => `Candidate ${span.citation_id} (${span.document_id}), excerpt offsets ${span.start}–${span.end}, ${span.relation}\n${span.text}\nReview flags: ${span.flags.join(", ") || "none detected"}`)])
  ].join("\n")).join("\n\n");
  return [
    { heading: "Review status", text: `UNREVIEWED DRAFT — HUMAN REVIEW REQUIRED\nAssessment: ${input.assessment_version}\nGenerated: ${input.generated_at || "unavailable (historical draft)"}\nGeneration mode: ${input.generation_mode}\nModel: ${input.model || "none / unavailable"}\nFallback reason: ${input.fallback_reason || "none"}\nConfidence label: ${input.confidence}; this does not certify legal accuracy.\n${input.assessment_version === "legacy-1" ? "Legacy support labels are uncalibrated historical scores; do not treat them as verified evidence." : "All evidence matches remain unverified retrieval candidates."}` },
    { heading: "Limitations", text: input.limitations.join("\n") || "No limitations were captured. Human verification is still required." },
    { heading: "Query summary", text: input.query_summary },
    ...(input.research_context ? [{ heading: "Supplied context", text: `Findings (user supplied):\n${input.research_context.provided_findings}\n\nLaw (user supplied):\n${input.research_context.supplied_law}\nRequested rules: ${input.research_context.requested_rules.join(", ")}\nRequested ordinances: ${input.research_context.requested_ordinance.join(", ")}\nLaw scope: ${input.research_context.law_scope}\nLaw context: ${JSON.stringify(input.research_context.law_context||{status:"unknown"})}` }] : []),
    { heading: "Draft text", text: input.draft_text },
    { heading: "Paragraph evidence", text: paragraphs || "No paragraph evidence recorded." },
    { heading: "Sources and captured excerpts", text: sourceLines.join("\n\n") || "No sources recorded." },
    { heading: "Provenance", text: `Corpus version: ${input.provenance?.corpus_version || "unavailable; not a complete corpus snapshot"}\nRevisions observed after retrieval may differ from the captured excerpt. The excerpt hash identifies the actual text used.\n${provenance}` },
    { heading: "Research notes", text: input.reasoning_notes.join("\n") },
    { heading: "Diagnostic signals (not verified confidence)", text: JSON.stringify(input.confidence_signals, null, 2) }
  ];
}

export function renderReviewPackage(input: DraftConclusionsResponse, title: string, format: "markdown" | "text" | "html"): string {
  const sections = reviewPackageSections(input);
  if (format === "text") return [title, ...sections.map(section => `${section.heading}\n${section.text}`)].join("\n\n");
  if (format === "markdown") {
    // Fence untrusted source/model text so embedded Markdown/HTML cannot become
    // executable markup or replace the review package's headings and caveats.
    return [`# ${title.replace(/[\r\n]/g, " ").replace(/[\\`*_{}[\]()#+.!<>|]/g, "\\$&")}`, ...sections.map(section => {
      const longest = Math.max(2, ...(section.text.match(/`+/g) || []).map(run => run.length));
      const fence = "`".repeat(longest + 1);
      return `## ${section.heading}\n\n${fence}text\n${section.text}\n${fence}`;
    })].join("\n\n");
  }
  const escape = (text: string) => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  const links = input.citations.filter(source => /^https?:\/\//i.test(source.source_link)).map(source => `<li><a href="${escape(source.source_link)}" rel="noopener noreferrer">${escape(source.id)} — ${escape(source.title)}</a></li>`).join("");
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'"><title>${escape(title)}</title><style>body{max-width:65rem;margin:2rem auto;padding:0 1rem;font:16px/1.5 system-ui;color:#18222b}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit}section{border-top:1px solid #ccc;margin:1.5rem 0}a{color:#164b83}</style></head><body><h1>${escape(title)}</h1>${sections.map(section => `<section><h2>${escape(section.heading)}</h2><pre>${escape(section.text)}</pre></section>`).join("")}<nav aria-label="Source links"><h2>Open sources</h2><ul>${links}</ul></nav></body></html>`;
}
