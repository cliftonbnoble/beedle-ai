import type { Env } from "../lib/types";
import { sha256Bytes } from "../lib/digests";
import { RequestConflictError } from "../lib/http";
import { withDocumentRevision } from "./document-revision";

type Identity = { expectedRevision: number; mutationId: string };
type Receipt = { mutationId: string; action: string; requestDigest: string; expectedRevision: number; appliedRevision: number; createdAt: string };

// This contains metadata and review state only, never document text or a claimed actor.
export const reviewerSnapshotSql = `json_object('index_codes',json(index_codes_json),'rules_sections',json(rules_sections_json),
  'ordinance_sections',json(ordinance_sections_json),'case_number',case_number,'decision_date',decision_date,
  'author_name',author_name,'outcome_label',outcome_label,'confirmed',qc_required_confirmed,
  'overrides',json(reviewer_overrides_json),'catalog',reference_catalog_version,'approved_at',approved_at,
  'rejected_at',rejected_at,'rejected_reason',rejected_reason)`;

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") return `{${Object.entries(value).filter(([,v])=>v!==undefined)
    .sort(([a],[b])=>a<b?-1:a>b?1:0).map(([k,v])=>`${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
  return JSON.stringify(value);
}

export async function reviewMutation(env: Env, documentId: string, action: "metadata" | "reject", payload: Identity & object) {
  const digest = await sha256Bytes(new TextEncoder().encode(canonical({documentId,action,payload})));
  const readReceipt = async () => {
    const receipt = await env.DB.prepare(`SELECT mutation_id AS mutationId,action,request_digest AS requestDigest,
      expected_revision AS expectedRevision,applied_revision AS appliedRevision,created_at AS createdAt
      FROM reviewer_mutations WHERE document_id=? AND mutation_id=?`).bind(documentId,payload.mutationId).first<Receipt>();
    if (receipt && (receipt.action !== action || receipt.requestDigest !== digest)) {
      throw new RequestConflictError("This mutation ID was already used for a different review. Reload before making a new edit.");
    }
    return receipt;
  };
  const existing = await readReceipt();
  return {
    existing: existing ? {...existing,replayed:true} : null,
    replay: async () => {
      const receipt = await readReceipt();
      return receipt ? {...receipt,replayed:true} : null;
    },
    async commit(beforeJson: string, statements: D1PreparedStatement[]) {
      const now = new Date().toISOString();
      try {
        await withDocumentRevision(env,documentId,payload.expectedRevision,[...statements,
          env.DB.prepare(`INSERT INTO reviewer_mutations(document_id,mutation_id,action,request_digest,expected_revision,
            applied_revision,before_json,after_json,actor_id,created_at)
            SELECT id,?,?,?,?,document_revision,?,${reviewerSnapshotSql},NULL,? FROM documents WHERE id=?`)
            .bind(payload.mutationId,action,digest,payload.expectedRevision,beforeJson,now,documentId)]);
      } catch (error) {
        // A committed response can be lost, or a duplicate delivery can lose the guard race.
        const receipt = await readReceipt();
        if (receipt) return {...receipt,replayed:true};
        throw error;
      }
      const receipt = await readReceipt();
      if (!receipt) throw new Error("Missing committed reviewer receipt");
      return {...receipt,replayed:false};
    }
  };
}
