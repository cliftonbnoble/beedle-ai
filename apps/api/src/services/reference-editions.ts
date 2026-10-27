import { legalEditionDescriptorSchema, legalEffectiveDateSchema, referenceEditionBindingSchema } from '@beedle/shared';
import type { LegalEditionDescriptor, LegalResearchContext, ReferenceEditionBinding } from '@beedle/shared';
import type { Env } from '../lib/types';
import { sha256Bytes } from '../lib/digests';
import { ServiceUnavailableError, RequestConflictError, RequestValidationError } from '../lib/http';
import { citationIdentity, parseCitation, type CitationContext } from './legal-citations';
export const unknownReferenceEdition: LegalEditionDescriptor = { policy: 'unknown', jurisdiction: null, effective_from: null, effective_to: null, sources: null, reviewed_by: null, reviewed_at: null, notes: 'Imported catalog; source-file hashes, jurisdiction and historical applicability have not been established.' };
const fold = (value: string) => value.trim().replace(/[A-Z]/g, char => char.toLowerCase());
export async function stageReferenceEdition(env: Env, catalogId: string, descriptor: LegalEditionDescriptor = unknownReferenceEdition) {
    const parsed = legalEditionDescriptorSchema.parse(descriptor);
    const text = JSON.stringify(parsed);
    const supersedes = parsed.supersedes || [];
    const parents = supersedes.length ? (await env.DB.prepare(`SELECT v.id,v.payload_sha256 AS payloadSha256,e.jurisdiction_key AS jurisdiction FROM legal_catalog_versions v JOIN legal_reference_editions e ON e.catalog_id=v.id WHERE v.id IN (SELECT value FROM json_each(?)) AND v.published_at IS NOT NULL AND v.state IN ('published','retained')`).bind(JSON.stringify(supersedes)).all<{
        id: string;
        payloadSha256: string | null;
        jurisdiction: string | null;
    }>()).results : [];
    if (parents.length !== supersedes.length || parents.some(row => row.jurisdiction !== fold(parsed.jurisdiction || '')))
        throw new RequestValidationError('Supersession must name published editions in the same jurisdiction.');
    await env.DB.prepare('INSERT INTO legal_reference_editions(catalog_id,descriptor_json,descriptor_sha256,created_at) VALUES(?,?,?,?)').bind(catalogId, text, await sha256Bytes(new TextEncoder().encode(text)), new Date().toISOString()).run();
    if (parents.length)
        await env.DB.batch(parents.map(parent => env.DB.prepare('INSERT INTO legal_reference_edition_supersessions(newer_catalog_id,older_catalog_id,older_payload_sha256) VALUES(?,?,?)').bind(catalogId, parent.id, parent.payloadSha256)));
}
export async function readReferenceEdition(env: Env, catalogId: string, publishedOnly = true) {
    const row = await env.DB.prepare(`SELECT e.catalog_id AS id,e.descriptor_json AS descriptor,e.descriptor_sha256 AS sha256 FROM legal_reference_editions e JOIN legal_catalog_versions v ON v.id=e.catalog_id WHERE e.catalog_id=? ${publishedOnly ? "AND v.published_at IS NOT NULL AND v.state IN ('published','retained')" : ''}`)
        .bind(catalogId).first<{
        id: string;
        descriptor: string;
        sha256: string | null;
    }>();
    if (!row)
        return null;
    let descriptor: LegalEditionDescriptor;
    try {
        descriptor = legalEditionDescriptorSchema.parse(JSON.parse(row.descriptor));
    }
    catch {
        throw new ServiceUnavailableError('Retained edition metadata is unavailable; restore a verified snapshot.');
    }
    if (row.sha256 && await sha256Bytes(new TextEncoder().encode(JSON.stringify(descriptor))) !== row.sha256)
        throw new ServiceUnavailableError('Reference edition descriptor digest mismatch; restore a verified snapshot.');
    const edges = (await env.DB.prepare('SELECT older_catalog_id AS id FROM legal_reference_edition_supersessions WHERE newer_catalog_id=? ORDER BY older_catalog_id').bind(row.id).all<{
        id: string;
    }>()).results.map(edge => edge.id);
    if (JSON.stringify(edges) !== JSON.stringify([...(descriptor.supersedes || [])].sort()))
        throw new ServiceUnavailableError('Reference edition supersession manifest mismatch; restore a verified snapshot.');
    const supersededBy = (await env.DB.prepare(`SELECT DISTINCT x.newer_catalog_id AS id FROM legal_reference_edition_supersessions x JOIN legal_catalog_versions n ON n.id=x.newer_catalog_id
      WHERE (x.older_catalog_id=? OR (x.older_payload_sha256 IS NOT NULL AND x.older_payload_sha256=(SELECT payload_sha256 FROM legal_catalog_versions WHERE id=?))) AND n.published_at IS NOT NULL AND n.state IN ('published','retained') ORDER BY x.newer_catalog_id LIMIT 9`).bind(row.id, row.id).all<{
        id: string;
    }>()).results.map(item => item.id);
    return { id: row.id, descriptor, sha256: row.sha256, supersededBy: supersededBy.slice(0, 8), moreSupersessions: supersededBy.length > 8 };
}
export async function verifyReferenceEdition(env: Env, catalogId: string) {
    const row = await readReferenceEdition(env, catalogId, false);
    if (!row)
        throw new ServiceUnavailableError('Reference edition provenance is missing; complete migrations/import before publication.');
    return row;
}
export async function resolveReferenceEdition(env: Env, context?: LegalResearchContext, options: {
    fallbackToCurrent?: boolean;
    registryEpoch?: number;
} = {}): Promise<ReferenceEditionBinding> {
    const binding: ReferenceEditionBinding = referenceEditionBindingSchema.parse({ edition_id: null, catalog_version: null, descriptor_sha256: null, status: 'unknown', reason: 'applicable_context_unknown', jurisdiction: context?.jurisdiction || null, as_of_date: context?.as_of_date || null, effective_from: null, effective_to: null });
    binding.registry_epoch = options.registryEpoch ?? null;
    binding.date_basis = context?.as_of_date ? "user_supplied" : "unknown";
    const knownContext = Boolean(context?.jurisdiction && context.as_of_date && legalEffectiveDateSchema.safeParse(context.as_of_date).success);
    let selected = context?.edition_id;
    if (!selected && !knownContext && !options.fallbackToCurrent)
        return binding;
    const registry = await env.DB.prepare('SELECT active_version AS version,epoch FROM legal_catalog_state WHERE singleton=1').first<{
        version: string;
        epoch: number;
    }>();
    if (!registry)
        throw new RequestValidationError('Legal registry migration is required.');
    if (options.registryEpoch !== undefined && registry.epoch !== options.registryEpoch)
        throw new RequestConflictError('Legal edition registry changed; reload and retry.');
    binding.registry_epoch = registry.epoch;
    if (!selected && knownContext) {
        const rows = (await env.DB.prepare(`SELECT min(e.catalog_id) AS id FROM legal_reference_editions e JOIN legal_catalog_versions v ON v.id=e.catalog_id
   WHERE e.jurisdiction_key=? AND e.policy='reviewed_interval' AND e.effective_from<=? AND (e.effective_to IS NULL OR ?<e.effective_to)
    AND v.published_at IS NOT NULL AND v.state IN ('published','retained')
    AND NOT EXISTS(SELECT 1 FROM legal_reference_edition_supersessions x JOIN legal_catalog_versions n ON n.id=x.newer_catalog_id WHERE (x.older_catalog_id=e.catalog_id OR (x.older_payload_sha256 IS NOT NULL AND x.older_payload_sha256=v.payload_sha256)) AND n.published_at IS NOT NULL AND n.state IN ('published','retained'))
   GROUP BY COALESCE(v.payload_sha256,e.catalog_id),COALESCE(e.descriptor_sha256,e.catalog_id) ORDER BY id LIMIT 2`)
            .bind(fold(context!.jurisdiction!), context!.as_of_date!, context!.as_of_date!).all<{
            id: string;
        }>()).results;
        if (rows.length !== 1)
            return { ...binding, status: rows.length ? 'ambiguous' : 'unavailable', reason: rows.length ? 'overlapping_reviewed_editions' : 'no_reviewed_edition_for_context' };
        selected = rows[0]!.id;
    }
    else if (!selected && !context && options.fallbackToCurrent) {
        selected = registry.version;
    }
    if (!selected)
        return binding;
    const edition = await readReferenceEdition(env, selected);
    if (!edition)
        return { ...binding, status: 'unavailable', reason: 'edition_not_published' };
    const resolved = { ...binding, edition_id: edition.id, catalog_version: edition.id, descriptor_sha256: edition.sha256, descriptor: edition.descriptor, effective_from: edition.descriptor.effective_from, effective_to: edition.descriptor.effective_to };
    if (edition.supersededBy.length)
        return { ...resolved, status: 'unavailable', reason: 'edition_applicability_assertion_superseded' };
    if (!knownContext)
        return { ...resolved, reason: 'edition_selected_without_applicable_context' };
    if (edition.descriptor.policy !== 'reviewed_interval')
        return { ...resolved, reason: 'edition_applicability_unreviewed' };
    if (fold(edition.descriptor.jurisdiction!) !== fold(context!.jurisdiction!))
        return { ...resolved, status: 'unavailable', reason: 'jurisdiction_mismatch' };
    const date = context!.as_of_date!;
    if (date < edition.descriptor.effective_from! || (edition.descriptor.effective_to && date >= edition.descriptor.effective_to))
        return { ...resolved, status: 'unavailable', reason: 'outside_reviewed_interval' };
    return { ...resolved, status: 'matched', reason: 'edition_metadata_matches_context_legal_review_required' };
}
export type EditionProvision = {
    id: string;
    citation: string;
    part: string | null;
    sectionNumber: string;
    heading: string | null;
    body: string;
    anchor: string | null;
    revision: string;
    bodySha256: string;
};
export async function editionProvisions(env: Env, editionId: string, kind: CitationContext, values: string[]): Promise<Map<string, EditionProvision>> {
    const keys = [...new Set(values.map(value => citationIdentity(value, kind)))];
    if (keys.some(key => !key) || keys.length > 50)
        throw new RequestValidationError('Provide at most 50 valid exact references of each kind.');
    if (!keys.length)
        return new Map();
    const lookup = [...new Set(keys.flatMap(key => key.startsWith('part:') ? [key, key.slice(key.lastIndexOf(':') + 1)] : [key]))];
    const table = kind === 'rule' ? 'legal_rules_sections' : 'legal_ordinance_sections';
    const rows = (await env.DB.prepare(`SELECT row_id AS id,payload_json AS payload FROM legal_catalog_rows WHERE catalog_id=? AND table_name=? AND json_extract(payload_json,'$.active')=1
  AND (json_extract(payload_json,'$.normalized_citation') IN (SELECT value FROM json_each(?)) ${kind === 'rule' ? "OR json_extract(payload_json,'$.normalized_bare_citation') IN (SELECT value FROM json_each(?))" : ''}) ORDER BY row_id LIMIT 101`)
        .bind(editionId, table, JSON.stringify(lookup), ...(kind === 'rule' ? [JSON.stringify(lookup)] : [])).all<{
        id: string;
        payload: string;
    }>()).results;
    if (rows.length > 100)
        throw new RequestValidationError('Too many ambiguous retained provisions; specify an exact Part and subsection.');
    const result = new Map<string, EditionProvision>();
    for (const key of keys) {
        const matching = rows.map(row => ({ id: row.id, data: JSON.parse(row.payload) })).filter(({ data }) => {
            const label = kind === 'rule' && data.part ? `Part ${String(data.part).replace(/^part\s+/i, '')}-${data.section_number}` : data.citation;
            const parsed = parseCitation(label, kind);
            return citationIdentity(label, kind) === key || (!key.startsWith('part:') && parsed.canonical === key);
        });
        if (matching.length !== 1)
            continue;
        const { id, data } = matching[0]!;
        if (typeof data.body_text !== 'string' || !data.body_text.trim())
            continue;
        result.set(key, { id, citation: data.citation, part: data.part || null, sectionNumber: data.section_number, heading: data.heading || null, body: data.body_text, anchor: data.source_page_anchor || null, revision: data.updated_at, bodySha256: await sha256Bytes(new TextEncoder().encode(data.body_text)) });
    }
    return result;
}
export async function assertReferenceEditionEpoch(env: Env, binding: ReferenceEditionBinding) {
    if (binding.registry_epoch === null)
        return;
    const epoch = await env.DB.prepare('SELECT epoch FROM legal_catalog_state WHERE singleton=1').first<number>('epoch');
    if (epoch !== binding.registry_epoch)
        throw new RequestConflictError('Legal edition registry changed during retrieval; retry against the current registry.');
}
