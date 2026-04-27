import { json } from "../lib/http";
import type { Env } from "../lib/types";

function contentTypeForKey(key: string): string {
  if (key.endsWith(".pdf")) return "application/pdf";
  if (key.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (key.endsWith(".txt")) return "text/plain; charset=utf-8";
  if (key.endsWith(".md") || key.endsWith(".markdown")) return "text/markdown; charset=utf-8";
  return "application/octet-stream";
}

export async function handleGetSource(request: Request, env: Env): Promise<Response> {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) {
    return json({ error: "documentId is required" }, { status: 400 });
  }

  const row = await env.DB.prepare("SELECT source_r2_key as sourceKey FROM documents WHERE id = ?")
    .bind(documentId)
    .first<{ sourceKey: string }>();

  if (!row?.sourceKey) {
    return json({ error: "Source document not found" }, { status: 404 });
  }

  const object = await env.SOURCE_BUCKET.get(row.sourceKey);
  if (!object) {
    return json({ error: "Source object not found in storage" }, { status: 404 });
  }

  const headers = new Headers();
  headers.set("content-type", object.httpMetadata?.contentType || contentTypeForKey(row.sourceKey));
  headers.set("cache-control", "no-store");
  headers.set("content-disposition", `inline; filename="${row.sourceKey.split("/").pop() || "source"}"`);
  return new Response(object.body, { status: 200, headers });
}
