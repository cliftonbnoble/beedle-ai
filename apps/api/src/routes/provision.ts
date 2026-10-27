import type { Env } from '../lib/types';
import { json, toErrorResponse } from '../lib/http';
import { sha256Bytes } from '../lib/digests';
import { lawScopeWarning, provisionTables, type ProvisionRow } from '../services/draft-law';
import { readReferenceEdition } from '../services/reference-editions';
export async function handleGetProvision(request: Request & {
    params?: {
        kind?: string;
        id?: string;
    };
}, env: Env) {
    try {
        const { kind, id } = request.params || {};
        if ((kind !== 'rule' && kind !== 'ordinance') || !id || id.length > 200)
            return json({ error: 'Provision not found' }, { status: 404 });
        const params = new URL(request.url).searchParams;
        const catalog = params.get('catalog');
        if (catalog && !/^[A-Za-z0-9-]{1,128}$/.test(catalog))
            return json({ error: 'Invalid edition identifier' }, { status: 400 });
        const rows = (await env.DB.prepare(`SELECT c.row_id AS id,json_extract(c.payload_json,'$.citation') AS citation,
   json_extract(c.payload_json,'$.heading') AS heading,json_extract(c.payload_json,'$.body_text') AS body,
   json_extract(c.payload_json,'$.source_page_anchor') AS anchor,json_extract(c.payload_json,'$.updated_at') AS revision,c.catalog_id AS catalogVersion
   FROM legal_catalog_rows c JOIN legal_catalog_versions v ON v.id=c.catalog_id WHERE c.row_id=? AND c.table_name=? AND json_extract(c.payload_json,'$.active')=1
    AND v.published_at IS NOT NULL AND v.state IN ('published','retained') ${catalog ? 'AND c.catalog_id=?' : ''} ORDER BY c.catalog_id LIMIT 2`)
            .bind(id, provisionTables[kind], ...(catalog ? [catalog] : [])).all<ProvisionRow & {
            catalogVersion: string;
        }>()).results;
        if (!rows.length)
            return json({ error: 'Provision not found' }, { status: 404 });
        if (rows.length !== 1)
            return json({ error: 'This historical link is ambiguous. Retain the captured excerpt and select its exact edition.' }, { status: 409 });
        const row = rows[0]!;
        const digest = await sha256Bytes(new TextEncoder().encode(row.body));
        if ((params.get('revision') && params.get('revision') !== row.revision) || (params.get('sha256') && params.get('sha256') !== digest))
            return json({ error: 'This provision does not match the captured revision/hash. Retain the draft excerpt and research again.' }, { status: 409 });
        const edition = await readReferenceEdition(env, row.catalogVersion);
        if (!edition)
            return json({ error: 'Edition provenance unavailable' }, { status: 503 });
        return json({ ...row, kind, bodySha256: digest, edition, review_required: true, limitation: (edition.supersededBy.length?'This edition applicability assertion was superseded; retain its historical text for review. ':'')+lawScopeWarning }, { headers: { 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' } });
    }
    catch (error) {
        return toErrorResponse(error);
    }
}
