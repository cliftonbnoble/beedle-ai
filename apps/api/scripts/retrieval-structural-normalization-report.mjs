import fs from "node:fs/promises";
import path from "node:path";
import {
  buildRetrievalStructuralNormalizationReport,
  formatRetrievalStructuralNormalizationMarkdown
} from "./retrieval-structural-normalization-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const idsInput = process.env.RETRIEVAL_DOC_IDS || "";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const includeText = (process.env.RETRIEVAL_INCLUDE_TEXT || "0") !== "0";
const reportName = process.env.RETRIEVAL_STRUCTURAL_NORMALIZATION_REPORT_NAME || "retrieval-structural-normalization-report.json";
const markdownName =
  process.env.RETRIEVAL_STRUCTURAL_NORMALIZATION_MARKDOWN_NAME || "retrieval-structural-normalization-report.md";

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

async function resolveDocuments() {
  if (idsInput.trim()) {
    const ids = idsInput.split(",").map((id) => id.trim()).filter(Boolean);
    return ids.map((id) => ({ id, isLikelyFixture: false }));
  }

  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || []).map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture) })).filter((doc) => doc.id);
}

async function main() {
  const resolvedDocs = await resolveDocuments();
  const previews = [];

  for (const doc of resolvedDocs) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=${includeText ? "1" : "0"}`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }

  const report = buildRetrievalStructuralNormalizationReport({
    apiBase,
    input: { documentIds: resolvedDocs.map((doc) => doc.id), realOnly, includeText, limit },
    documents: previews
  });

  const reportsDir = path.resolve(process.cwd(), "reports");
  await fs.mkdir(reportsDir, { recursive: true });

  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatRetrievalStructuralNormalizationMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval structural normalization JSON report written to ${jsonPath}`);
  console.log(`Retrieval structural normalization Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
