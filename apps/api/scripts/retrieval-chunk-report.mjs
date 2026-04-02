import fs from "node:fs/promises";
import path from "node:path";
import { buildRetrievalChunkReport, formatRetrievalChunkMarkdown } from "./retrieval-chunk-report-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const idsInput = process.env.RETRIEVAL_DOC_IDS || "";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "5", 10);
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const includeText = (process.env.RETRIEVAL_INCLUDE_TEXT || "1") !== "0";
const reportName = process.env.RETRIEVAL_CHUNK_REPORT_NAME || "retrieval-chunk-report.json";
const markdownName = process.env.RETRIEVAL_CHUNK_MARKDOWN_NAME || "retrieval-chunk-report.md";

async function fetchJson(url) {
  const response = await fetch(url);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url} but received non-JSON payload.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function resolveDocumentIds() {
  if (idsInput.trim()) {
    return idsInput.split(",").map((id) => id.trim()).filter(Boolean);
  }

  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || []).map((doc) => doc.id).filter(Boolean);
}

async function main() {
  const ids = await resolveDocumentIds();
  const documents = [];

  for (const documentId of ids) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${documentId}/chunks?includeText=${includeText ? "1" : "0"}`;
    const preview = await fetchJson(detailUrl);
    documents.push(preview);
  }

  const report = buildRetrievalChunkReport({
    apiBase,
    input: { documentIds: ids, realOnly, includeText, limit },
    documents
  });

  const reportsDir = path.resolve(process.cwd(), "reports");
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatRetrievalChunkMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval chunk JSON report written to ${jsonPath}`);
  console.log(`Retrieval chunk Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
