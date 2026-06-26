import { json } from "../lib/http";
import type { Env } from "../lib/types";

function contentTypeForKey(key: string): string {
  if (key.endsWith(".pdf")) return "application/pdf";
  if (key.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (key.endsWith(".txt")) return "text/plain; charset=utf-8";
  if (key.endsWith(".md") || key.endsWith(".markdown")) return "text/markdown; charset=utf-8";
  return "application/octet-stream";
}

function safeFilename(value: string) {
  return String(value || "source")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120) || "source";
}

async function reconstructedSourceMarkdown(env: Env, documentId: string, meta: { title: string; citation: string; sourceKey: string }) {
  const paragraphRows = await env.DB.prepare(
    `SELECT s.heading as heading, p.text as text
     FROM document_sections s
     JOIN section_paragraphs p ON p.section_id = s.id
     WHERE s.document_id = ?
     ORDER BY s.section_order ASC, p.paragraph_order ASC`
  )
    .bind(documentId)
    .all<{ heading: string; text: string }>();

  const lines = [
    `# ${meta.title || meta.citation || documentId}`,
    "",
    `- Citation: ${meta.citation || "Unknown"}`,
    `- Source object unavailable: ${meta.sourceKey}`,
    `- Reconstructed from stored searchable text.`,
    ""
  ];

  let currentHeading = "";
  const paragraphs = paragraphRows.results || [];
  for (const row of paragraphs) {
    const heading = String(row.heading || "").trim();
    const text = String(row.text || "").trim();
    if (!text) continue;
    if (heading && heading !== currentHeading) {
      currentHeading = heading;
      lines.push(`## ${heading}`, "");
    }
    lines.push(text, "");
  }

  if (!paragraphs.length) {
    const chunkRows = await env.DB.prepare(
      `SELECT section_label as sectionLabel, chunk_text as text
       FROM document_chunks
       WHERE document_id = ?
       ORDER BY chunk_order ASC`
    )
      .bind(documentId)
      .all<{ sectionLabel: string; text: string }>();

    currentHeading = "";
    for (const row of chunkRows.results || []) {
      const heading = String(row.sectionLabel || "").trim();
      const text = String(row.text || "").trim();
      if (!text) continue;
      if (heading && heading !== currentHeading) {
        currentHeading = heading;
        lines.push(`## ${heading}`, "");
      }
      lines.push(text, "");
    }
  }

  const markdown = lines.join("\n").trim();
  return markdown.includes("Reconstructed from stored searchable text.") && markdown.split("\n").length > 5 ? `${markdown}\n` : "";
}

export async function handleGetSource(request: Request, env: Env): Promise<Response> {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) {
    return json({ error: "documentId is required" }, { status: 400 });
  }

  const row = await env.DB.prepare("SELECT title, citation, source_r2_key as sourceKey FROM documents WHERE id = ?")
    .bind(documentId)
    .first<{ title: string; citation: string; sourceKey: string }>();

  if (!row?.sourceKey) {
    return json({ error: "Source document not found" }, { status: 404 });
  }

  const object = await env.SOURCE_BUCKET.get(row.sourceKey);
  if (!object) {
    const fallback = await reconstructedSourceMarkdown(env, documentId, row);
    if (!fallback) {
      return json({ error: "Source object not found in storage" }, { status: 404 });
    }

    const headers = new Headers();
    headers.set("content-type", "text/markdown; charset=utf-8");
    headers.set("cache-control", "no-store");
    headers.set("content-disposition", `inline; filename="${safeFilename(row.citation || row.title || documentId)}-source-fallback.md"`);
    headers.set("x-beedle-source-fallback", "r2-missing-db-text");
    return new Response(fallback, { status: 200, headers });
  }

  const headers = new Headers();
  headers.set("content-type", object.httpMetadata?.contentType || contentTypeForKey(row.sourceKey));
  headers.set("cache-control", "no-store");
  headers.set("content-disposition", `inline; filename="${safeFilename(row.sourceKey.split("/").pop() || "source")}"`);
  return new Response(object.body, { status: 200, headers });
}
