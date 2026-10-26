import type { CaseAssistantResponse, DraftConclusionsRequest } from "@beedle/shared";
import type { Env } from "../lib/types";
import { RequestValidationError } from "../lib/http";
import { citationIdentity, citationLabel, parseCitation, extractCitations, type CitationContext } from "./legal-citations";

export const lawScopeWarning = "Catalog provisions are candidates for review: jurisdiction, effective edition, amendments, and applicability have not been verified. Do not describe them as confirmed governing law.";
export const provisionTables = { rule: "legal_rules_sections", ordinance: "legal_ordinance_sections" } as const;
export interface ProvisionRow { id: string; citation: string; part?: string | null; sectionNumber?: string; heading: string | null; body: string; anchor: string | null; revision: string; normalized: string }

export async function retrieveRequestedLaw(env: Env, parsed: DraftConclusionsRequest) {
  const inferred = extractCitations(parsed.law_text);
  const authorities: CaseAssistantResponse["relevant_law"] = [];
  const warnings: string[] = [lawScopeWarning];
  const references: Record<CitationContext, string[]> = { rule: [...parsed.rules_sections], ordinance: [...parsed.ordinance_sections] };
  for (const citation of inferred) if (citation.context && citation.canonical) references[citation.context].push(citationLabel(citation));
  const totalProvisions = new Set((Object.entries(references) as Array<[CitationContext, string[]]>).flatMap(([kind, values]) => values.map(value => `${kind}:${citationIdentity(value, kind)}`))).size;
  if (totalProvisions > 6) throw new RequestValidationError("A draft can review at most six distinct provisions; split this request rather than silently omitting law.");
  for (const kind of ["rule", "ordinance"] as const) {
    const keys = [...new Set(references[kind].map(value => {
      const key = citationIdentity(value, kind);
      if (!key) throw new RequestValidationError(`Invalid or conflicting ${kind} citation: ${value}`);
      return key;
    }))];
    if (keys.length > 50) throw new RequestValidationError("Provide at most 50 distinct provisions of each kind.");
    if (!keys.length) continue;
    // Also query the bare index for retained catalogs normalized before Part
    // identities were preserved, then require the exact qualifier below.
    const lookupKeys = [...new Set(keys.flatMap(key => key.startsWith("part:") ? [key, key.slice(key.lastIndexOf(":") + 1)] : [key]))];
    const placeholders = lookupKeys.map(() => "?").join(",");
    const matchClause = kind === "rule" ? `(normalized_citation IN (${placeholders}) OR normalized_bare_citation IN (${placeholders}))` : `normalized_citation IN (${placeholders})`;
    const rows = await env.DB.prepare(`SELECT id, citation, ${kind === "rule" ? "part, section_number AS sectionNumber," : ""} heading, body_text AS body,
      source_page_anchor AS anchor, updated_at AS revision, normalized_citation AS normalized
      FROM ${provisionTables[kind]} WHERE active = 1 AND ${matchClause}`)
      .bind(...(kind === "rule" ? [...lookupKeys, ...lookupKeys] : lookupKeys)).all<ProvisionRow>();
    for (const key of keys) {
      const matching = (rows.results || []).filter(row => {
        const label = kind === "rule" && row.part ? `Part ${row.part.replace(/^part\s+/i, "")}-${row.sectionNumber}` : row.citation;
        const parsed = parseCitation(label, kind);
        return citationIdentity(label, kind) === key || (!key.startsWith("part:") && parsed.canonical === key);
      });
      if (matching.length !== 1) { warnings.push(`Requested ${kind} ${key}: ${matching.length ? "ambiguous catalog entries" : "exact provision unavailable"}; no parent substituted.`); continue; }
      const row = matching[0]!;
      if (!row.body.trim()) { warnings.push(`Requested ${kind} ${key} has no provision text.`); continue; }
      const source = `${(env.SOURCE_PROXY_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "")}/references/${kind}/${encodeURIComponent(row.id)}?revision=${encodeURIComponent(row.revision)}`;
      authorities.push({ document_id: `reference:${kind}:${row.id}`, title: `${kind === "rule" ? "Rule" : "Ordinance"} ${row.citation}`,
        citation: row.citation, citation_anchor: row.anchor || key, citation_id: `reference:${kind}:${row.id}:${row.revision}`,
        source_link: source, source_file_ref: `reference:${kind}:${row.id}`, snippet: row.body.slice(0, 8000),
        section_label: row.heading || `${kind} provision`, support_type: "inference", source_kind: "provision_candidate",
        source_revision: row.revision, why_it_matches: `Exact requested ${kind} ${key}. ${lawScopeWarning}` });
      if (row.body.length > 8000) warnings.push(`${kind} ${key}: excerpt limited to 8,000 characters; read the complete linked provision.`);
    }
  }
  if (!authorities.length) warnings.push("No exact rule or ordinance text was retrieved. Supply specific provisions and verify their applicability before drafting a legal conclusion.");
  return { authorities, warnings };
}
