import fs from "node:fs/promises";
import path from "node:path";
import { buildRetrievalEvalReport, formatRetrievalEvalMarkdown } from "./retrieval-eval-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const idsInput = process.env.RETRIEVAL_DOC_IDS || "";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "100", 10);
const realOnly = (process.env.RETRIEVAL_EVAL_REAL_ONLY || "1") !== "0";
const includeText = (process.env.RETRIEVAL_EVAL_INCLUDE_TEXT || "0") !== "0";
const reportName = process.env.RETRIEVAL_EVAL_REPORT_NAME || "retrieval-eval-report.json";
const markdownName = process.env.RETRIEVAL_EVAL_MARKDOWN_NAME || "retrieval-eval-report.md";
const embeddingPrepName = process.env.RETRIEVAL_EVAL_EMBEDDING_PREP_NAME || "retrieval-admitted-embedding-prep.json";
const bundleName = process.env.RETRIEVAL_EVAL_BUNDLE_NAME || "retrieval-admitted-corpus-bundle.json";

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
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=1`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }

  const report = buildRetrievalEvalReport({
    apiBase,
    input: { documentIds: resolvedDocs.map((doc) => doc.id), realOnly, includeText, limit },
    documents: previews,
    includeText
  });

  const reportsDir = path.resolve(process.cwd(), "reports");
  await fs.mkdir(reportsDir, { recursive: true });

  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);
  const prepPath = path.resolve(reportsDir, embeddingPrepName);
  const bundlePath = path.resolve(reportsDir, bundleName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatRetrievalEvalMarkdown(report));
  await fs.writeFile(
    prepPath,
    JSON.stringify(
      {
        generatedAt: report.generatedAt,
        readOnly: true,
        admittedDocumentIds: report.admittedCorpus.admittedDocumentIds,
        rowCount: report.embeddingPrep.rowCount,
        rows: report.embeddingPrep.rows
      },
      null,
      2
    )
  );
  await fs.writeFile(
    bundlePath,
    JSON.stringify(
      {
        generatedAt: report.generatedAt,
        readOnly: true,
        admitNowDocumentIds: report.admittedCorpus.documentsEligibleForInitialEmbedding,
        holdDocumentIds: report.admittedCorpus.heldDocumentIds,
        excludeDocumentIds: report.admittedCorpus.excludedDocumentIds
      },
      null,
      2
    )
  );

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval eval JSON report written to ${jsonPath}`);
  console.log(`Retrieval eval Markdown report written to ${mdPath}`);
  console.log(`Admitted embedding prep JSON written to ${prepPath}`);
  console.log(`Admitted corpus bundle JSON written to ${bundlePath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
