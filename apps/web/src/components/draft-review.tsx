import type { DraftConclusionsResponse } from "@beedle/shared";

const sourceId = (id: string) => `draft-source-${encodeURIComponent(id)}`;
const safeHref = (value: string) => /^https?:\/\//i.test(value) ? value : undefined;

export function DraftReview({ result }: { result: DraftConclusionsResponse }) {
  const unsupported = result.paragraph_support.filter(row => row.support_level === "unsupported").length;
  const flagged = result.paragraph_support.filter(row => row.claims.some(claim => claim.flags.length || claim.evidence.some(span => span.flags.length))).length;
  return <>
    <h2>Draft review</h2>
    <div className="feedback-error" role="status">
      <strong>Unreviewed draft — human review required</strong>
      <p>{unsupported} unsupported paragraph(s); {flagged} paragraph(s) with review flags. Retrieval matches do not verify a claim.</p>
      {result.assessment_version === "legacy-1" ? <p>Legacy support labels are uncalibrated historical scores.</p> : null}
    </div>
    <section aria-label="Draft limitations">
      <h3>Limitations to resolve</h3>
      <ul>{result.limitations.map((limitation, index) => <li key={index}>{limitation}</li>)}</ul>
      {result.generation_mode === "heuristic_fallback" ? <p className="feedback-error">Structured fallback: {result.fallback_reason || "LLM drafting was unavailable."}</p> : null}
    </section>
    <p>Generated: {result.generated_at || "unavailable (historical draft)"} · Model: {result.model || "none / unavailable"} · Assessment: {result.assessment_version}</p>
    <p>Corpus snapshot: {result.provenance?.corpus_version || "unavailable"}. Confidence label: {result.confidence}; legal accuracy has not been certified.</p>
    {result.research_context?.law_context?<p>Law context: {result.research_context.law_context.jurisdiction||"jurisdiction unknown"} · {result.research_context.law_context.as_of_date||"date unknown"} · {result.research_context.law_context.edition_id||"automatic edition selection"}</p>:<p>Historical law context was not established for this draft.</p>}
    <h3>Draft text</h3>
    <pre className="text-output">{result.draft_text}</pre>
    <section aria-label="Paragraph evidence">
      <h3>Paragraph evidence</h3>
      {result.paragraph_support.map(row => <details key={row.paragraph_id} open>
        <summary><strong>{row.paragraph_id}: {row.support_level} — review required</strong></summary>
        <p>{row.text}</p>
        <ul>{row.support_notes.map((note, index) => <li key={index}>{note}</li>)}</ul>
        {row.claims.map(claim => <div key={claim.claim_id}>
          <p><strong>{claim.claim_id}: {claim.kind.replaceAll("_", " ")} / {claim.status}</strong></p>
          <p>{claim.text}</p>
          {claim.flags.length ? <p className="feedback-error">Review flags: {claim.flags.join(", ")}</p> : null}
          {claim.evidence.length ? claim.evidence.map(span => <blockquote key={span.citation_id}>
            <p>{span.text}</p>
            <p><a href={`#${sourceId(span.citation_id)}`}>Captured passage: {span.citation_id}</a> · {span.relation.replaceAll("_", " ")} · excerpt offsets {span.start}–{span.end}</p>
            {span.flags.length ? <p className="feedback-error">Review flags: {span.flags.join(", ")}</p> : null}
          </blockquote>) : <p className="feedback-error">No candidate evidence mapped to this claim.</p>}
        </div>)}
      </details>)}
    </section>
    <section aria-label="Captured sources">
      <h3>Captured sources</h3>
      {result.supporting_authorities.length === 0 ? <p>No source evidence recorded.</p> : null}
      {result.supporting_authorities.map(source => {
        const provenance = result.provenance?.sources.find(item => item.citation_id === source.citation_id);
        return <article key={source.citation_id} id={sourceId(source.citation_id)} style={{ overflowWrap: "anywhere", marginBottom: "1.5rem" }}>
          <h4>{source.title} · {source.citation}</h4>
          <p>{source.source_kind?.replaceAll("_", " ") || "Legacy source"} · Anchor: {source.citation_anchor}</p>
          {source.reference_edition?<div>
            <p>Captured law edition: {source.reference_edition.edition_id||"unknown"} · {source.reference_edition.status==="matched"?"jurisdiction/date metadata matched; legal review required":`${source.reference_edition.status} — legal review required`}</p>
            <p>{source.reference_edition.reason.replaceAll("_"," ")}. Context: {source.reference_edition.jurisdiction||"unknown jurisdiction"} · {source.reference_edition.as_of_date||"unknown date"}.</p>
            <p>Edition interval: {source.reference_edition.effective_from||"unknown"} through {source.reference_edition.effective_to||"no declared end"} (end exclusive). Registry epoch: {source.reference_edition.registry_epoch??"unknown"}.</p>
            <details><summary>Edition provenance and full provision integrity</summary><p>Body SHA-256: {source.reference_edition.body_sha256||"unknown"}</p><p>Descriptor SHA-256: {source.reference_edition.descriptor_sha256||"unknown"}</p>
              <pre className="text-output">{JSON.stringify(source.reference_edition.descriptor,null,2)}</pre><p>Source hashes and reviewer names are importer declarations; this application does not independently certify their provenance or legal applicability.</p>
            </details>
          </div>:source.source_kind==="provision_candidate"||source.source_kind==="unversioned_law_excerpt"?<p>Historical source edition unknown; legal review required.</p>:null}
          <blockquote>{source.snippet}</blockquote>
          <p>{safeHref(source.source_link) ? <a href={source.source_link} target="_blank" rel="noopener noreferrer">Open source for {source.citation}</a> : "Source link unavailable"}</p>
          <p>Document revision: {provenance?.document_revision || source.source_revision || "unavailable"}; extraction version: {provenance?.extraction_version || "unavailable"}</p>
          {provenance?.revision_status === "observed_after_retrieval" ? <p>Version metadata was observed after retrieval and may differ from the captured excerpt. Use the excerpt hash to identify the text used.</p> : null}
          <details><summary>Source identifiers and excerpt integrity</summary><p>{source.citation_id}</p><p>SHA-256: {provenance?.excerpt_sha256 || "unavailable"}</p></details>
        </article>;
      })}
    </section>
  </>;
}
