import type { Env } from "../lib/types";
import { json, toErrorResponse } from "../lib/http";
import { lawScopeWarning, provisionTables, type ProvisionRow } from "../services/draft-law";

export async function handleGetProvision(request: Request & { params?: { kind?: string; id?: string } }, env: Env) {
  try {
    const { kind, id } = request.params || {};
    if ((kind !== "rule" && kind !== "ordinance") || !id || id.length > 200) return json({ error: "Provision not found" }, { status: 404 });
    const row = await env.DB.prepare(`SELECT c.row_id AS id,json_extract(c.payload_json,'$.citation') AS citation,
      json_extract(c.payload_json,'$.heading') AS heading,json_extract(c.payload_json,'$.body_text') AS body,
      json_extract(c.payload_json,'$.source_page_anchor') AS anchor,json_extract(c.payload_json,'$.updated_at') AS revision,
      c.catalog_id AS catalogVersion FROM legal_catalog_rows c JOIN legal_catalog_versions v ON v.id=c.catalog_id
      WHERE c.row_id=? AND c.table_name=? AND json_extract(c.payload_json,'$.active')=1 AND v.published_at IS NOT NULL`)
      .bind(id,provisionTables[kind]).first<ProvisionRow>();
    if (!row) return json({ error: "Provision not found" }, { status: 404 });
    const expected = new URL(request.url).searchParams.get("revision");
    if (expected && expected !== row.revision) return json({ error: "This provision has changed. Retain the draft's captured excerpt and research again before relying on the new text." }, { status: 409 });
    return json({ ...row, kind, review_required: true, limitation: lawScopeWarning }, { headers: { "cache-control": "no-store", "x-content-type-options": "nosniff" } });
  } catch (error) { return toErrorResponse(error); }
}
