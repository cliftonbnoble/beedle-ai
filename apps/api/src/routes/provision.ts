import type { Env } from "../lib/types";
import { json, toErrorResponse } from "../lib/http";
import { lawScopeWarning, provisionTables, type ProvisionRow } from "../services/draft-law";

export async function handleGetProvision(request: Request & { params?: { kind?: string; id?: string } }, env: Env) {
  try {
    const { kind, id } = request.params || {};
    if ((kind !== "rule" && kind !== "ordinance") || !id || id.length > 200) return json({ error: "Provision not found" }, { status: 404 });
    const row = await env.DB.prepare(`SELECT id, citation, heading, body_text AS body,
      source_page_anchor AS anchor, updated_at AS revision FROM ${provisionTables[kind]} WHERE id = ? AND active = 1`)
      .bind(id).first<ProvisionRow>();
    if (!row) return json({ error: "Provision not found" }, { status: 404 });
    const expected = new URL(request.url).searchParams.get("revision");
    if (expected && expected !== row.revision) return json({ error: "This provision has changed. Retain the draft's captured excerpt and research again before relying on the new text." }, { status: 409 });
    return json({ ...row, kind, review_required: true, limitation: lawScopeWarning }, { headers: { "cache-control": "no-store", "x-content-type-options": "nosniff" } });
  } catch (error) { return toErrorResponse(error); }
}
