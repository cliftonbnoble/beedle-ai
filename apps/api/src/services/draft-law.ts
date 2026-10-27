import type { CaseAssistantResponse, DraftConclusionsRequest } from '@beedle/shared';
import type { Env } from '../lib/types';
import { RequestValidationError } from '../lib/http';
import { citationIdentity, citationLabel, extractCitations, type CitationContext } from './legal-citations';
import { assertReferenceEditionEpoch, editionProvisions, resolveReferenceEdition } from './reference-editions';
export const lawScopeWarning = 'Catalog provisions are candidates for review: jurisdiction, effective edition, amendments, and applicability have not been verified. Do not describe them as confirmed governing law.';
export const provisionTables = { rule: 'legal_rules_sections', ordinance: 'legal_ordinance_sections' } as const;
export interface ProvisionRow {
    id: string;
    citation: string;
    part?: string | null;
    sectionNumber?: string;
    heading: string | null;
    body: string;
    anchor: string | null;
    revision: string;
    normalized: string;
}
export async function retrieveRequestedLaw(env: Env, parsed: DraftConclusionsRequest) {
    const inferred = extractCitations(parsed.law_text);
    const references: Record<CitationContext, string[]> = { rule: [...parsed.rules_sections], ordinance: [...parsed.ordinance_sections] };
    for (const citation of inferred)
        if (citation.context && citation.canonical)
            references[citation.context].push(citationLabel(citation));
    const total = new Set((Object.entries(references) as Array<[
        CitationContext,
        string[]
    ]>).flatMap(([kind, values]) => values.map(value => `${kind}:${citationIdentity(value, kind)}`))).size;
    if (total > 6)
        throw new RequestValidationError('A draft can review at most six distinct provisions; split this request rather than silently omitting law.');
    for (const kind of ['rule', 'ordinance'] as const)
        if (references[kind].some(value => !citationIdentity(value, kind)))
            throw new RequestValidationError(`Invalid or conflicting ${kind} citation.`);
    const edition = await resolveReferenceEdition(env, parsed.law_context, { fallbackToCurrent: true });
    const warnings = [lawScopeWarning, `Edition applicability: ${edition.status} (${edition.reason}). Human review is required.`];
    const authorities: CaseAssistantResponse['relevant_law'] = [];
    const hasExplicitContext = Boolean(parsed.law_context?.jurisdiction || parsed.law_context?.as_of_date);
    if (!edition.edition_id || edition.status === 'unavailable' || edition.status === 'ambiguous' || (hasExplicitContext && edition.status !== 'matched')) {
        await assertReferenceEditionEpoch(env, edition);
        warnings.push('No unique applicable edition was established for the supplied context. No current-catalog text was substituted.');
        return { authorities, warnings, edition };
    }
    for (const kind of ['rule', 'ordinance'] as const) {
        const keys = [...new Set(references[kind].map(value => citationIdentity(value, kind)))];
        const provisions = await editionProvisions(env, edition.edition_id, kind, references[kind]);
        for (const key of keys) {
            const row = provisions.get(key);
            if (!row) {
                warnings.push(`Requested ${kind} ${key}: exact provision unavailable or ambiguous in the selected edition; no parent or different edition substituted.`);
                continue;
            }
            const source = new URL(`${(env.SOURCE_PROXY_BASE_URL || 'http://127.0.0.1:8787').replace(/\/$/, '')}/references/${kind}/${encodeURIComponent(row.id)}`);
            source.searchParams.set('catalog', edition.edition_id);
            source.searchParams.set('revision', row.revision);
            source.searchParams.set('sha256', row.bodySha256);
            authorities.push({ document_id: `reference:${kind}:${row.id}`, title: `${kind === 'rule' ? 'Rule' : 'Ordinance'} ${row.citation}`, citation: row.citation,
                citation_anchor: row.anchor || key, citation_id: `reference:${edition.edition_id}:${kind}:${row.id}:${row.bodySha256}`,
                source_link: source.toString(), source_file_ref: `reference:${edition.edition_id}:${kind}:${row.id}`, snippet: row.body.slice(0, 8000), section_label: row.heading || `${kind} provision`, support_type: 'inference', source_kind: 'provision_candidate',
                source_revision: row.revision, reference_edition: { ...edition, provision_id: row.id, body_sha256: row.bodySha256 }, why_it_matches: `Exact requested ${kind} ${key} in immutable edition ${edition.edition_id}. ${lawScopeWarning}` });
            if (row.body.length > 8000)
                warnings.push(`${kind} ${key}: excerpt limited to 8,000 characters; read the complete linked provision.`);
        }
    }
    if (!authorities.length)
        warnings.push('No exact rule or ordinance text was retrieved. Supply specific provisions and verify their applicability before drafting a legal conclusion.');
    await assertReferenceEditionEpoch(env, edition);
    return { authorities, warnings, edition };
}
