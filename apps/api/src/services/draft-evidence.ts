import type { CaseAssistantResponse, DraftConclusionsResponse } from "@beedle/shared";
import { extractCitations } from "./legal-citations";

type Authority = CaseAssistantResponse["similar_cases"][number];
type Claim = DraftConclusionsResponse["paragraph_support"][number]["claims"][number];
const stop = new Set("the and for that this with from have has were was are not but their there which shall should would could under about into only more less than then these those its his her had been upon may must".split(" "));
const tokens = (text: string) => new Set((text.toLowerCase().match(/[a-z0-9]+/g) || []).filter(token => token.length > 2 && !stop.has(token)));
const negated = (text: string) => /\b(?:not|never|no|without|cannot|denied|fails?|failed)\b|n['’]t\b/i.test(text);

function sentences(text: string): Array<{ text: string; start: number; end: number }> {
  const result: Array<{ text: string; start: number; end: number }> = [];
  // Keep decimal legal citations intact by splitting only at sentence whitespace.
  const parts = text.split(/(?<=[.!?])\s+(?=[A-Z“"0-9])/);
  let offset = 0;
  for (const part of parts) {
    const start = text.indexOf(part, offset);
    if (part.trim()) result.push({ text: part, start, end: start + part.length });
    offset = start + part.length;
  }
  return result;
}

function assessClaim(text: string, id: string, authorities: Authority[], findings: string): Claim {
  const query = tokens(text);
  const cited = [...text.matchAll(/\[([^\]]+)\]/g)].map(match => match[1]!);
  const known = new Set(authorities.map(authority => authority.citation_id));
  const flags = cited.some(citation => !known.has(citation)) ? ["unknown_citation_id"] : [];
  const provisions = extractCitations(text).filter(c => c.canonical);
  const candidates = authorities.flatMap(authority => {
    return sentences(authority.snippet).map(span => {
      const sourceTokens = tokens(span.text);
      const overlap = [...query].filter(token => sourceTokens.has(token)).length;
      const score = overlap / Math.max(query.size, 1);
      if (overlap < 3 || score < 0.35) return null;
      const spanFlags: string[] = [];
      if (negated(text) !== negated(span.text)) spanFlags.push("possible_negation_conflict");
      const sourceProvisions = extractCitations(span.text);
      if (provisions.length && !provisions.every(citation => sourceProvisions.some(source => source.context === citation.context && source.canonical === citation.canonical && source.part === citation.part))) {
        spanFlags.push("provision_not_matched");
      }
      return { score, evidence: { citation_id: authority.citation_id, document_id: authority.document_id,
        text: span.text, start: span.start, end: span.end,
        relation: (span.text.includes(text) ? "exact_quote" : "retrieval_similarity") as "exact_quote" | "retrieval_similarity",
        flags: spanFlags } };
    }).filter((item): item is NonNullable<typeof item> => item !== null);
  }).sort((a, b) => b.score - a.score);
  // Multiple chunks from the same decision are one source, not independent votes.
  const seen = new Set<string>();
  const evidence = candidates.filter(item => {
    if (seen.has(item.evidence.document_id)) return false;
    seen.add(item.evidence.document_id); return true;
  }).slice(0, 3).map(item => item.evidence);
  return { claim_id: id, text,
    kind: findings.includes(text) ? "provided_fact" : provisions.length ? "legal_proposition" : "unclassified",
    status: evidence.length && !flags.length ? "unverified" : "unsupported", flags, evidence };
}

export function buildParagraphSupport(draftText: string, authorityByCitationId: Map<string, Authority>, findings = ""): DraftConclusionsResponse["paragraph_support"] {
  const authorities = [...authorityByCitationId.values()];
  return draftText.split(/\n\s*\n+/).map(paragraph => paragraph.trim()).filter(Boolean).map((paragraph, index) => {
    const claims = sentences(paragraph).map((span, claimIndex) => assessClaim(span.text, `p${index + 1}c${claimIndex + 1}`, authorities, findings));
    return { paragraph_id: `p${index + 1}`, section_id: "conclusions_of_law", text: paragraph,
      support_level: claims.every(claim => claim.status === "unverified") ? "unverified" : "unsupported",
      citation_ids: [...new Set(claims.flatMap(claim => claim.evidence.map(span => span.citation_id)))],
      support_notes: ["Retrieval similarity only; no claim has been verified. Review each proposition against the original source and current record.",
        ...new Set(claims.flatMap(claim => [...claim.flags, ...claim.evidence.flatMap(span => span.flags)]))], claims };
  });
}
