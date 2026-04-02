import fs from "node:fs/promises";
import path from "node:path";
import {
  buildRetrievalActivationBundleReport,
  formatRetrievalActivationBundleMarkdown
} from "./retrieval-activation-bundle-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const idsInput = process.env.RETRIEVAL_DOC_IDS || "";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const includeText = (process.env.RETRIEVAL_INCLUDE_TEXT || "1") !== "0";

const reportsDir = path.resolve(process.cwd(), process.env.RETRIEVAL_REPORTS_DIR || "reports");
const reportName = process.env.RETRIEVAL_ACTIVATION_BUNDLE_REPORT_NAME || "retrieval-activation-bundle-report.json";
const markdownName = process.env.RETRIEVAL_ACTIVATION_BUNDLE_MARKDOWN_NAME || "retrieval-activation-bundle-report.md";
const embeddingPayloadName = process.env.RETRIEVAL_TRUSTED_EMBEDDING_PAYLOAD_NAME || "retrieval-trusted-embedding-payload.json";
const searchPayloadName = process.env.RETRIEVAL_TRUSTED_SEARCH_PAYLOAD_NAME || "retrieval-trusted-search-payload.json";
const activationManifestName = process.env.RETRIEVAL_TRUSTED_ACTIVATION_MANIFEST_NAME || "retrieval-trusted-activation-manifest.json";
const rollbackManifestName = process.env.RETRIEVAL_TRUSTED_ROLLBACK_MANIFEST_NAME || "retrieval-trusted-rollback-manifest.json";

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
    const ids = idsInput
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean);
    return ids.map((id) => ({ id, isLikelyFixture: false }));
  }

  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || [])
    .map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture) }))
    .filter((doc) => doc.id);
}

async function main() {
  const resolvedDocs = await resolveDocuments();
  const previews = [];

  for (const doc of resolvedDocs) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=${includeText ? "1" : "0"}`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }

  const report = buildRetrievalActivationBundleReport({
    apiBase,
    input: { documentIds: resolvedDocs.map((doc) => doc.id), realOnly, includeText, limit },
    documents: previews,
    includeText,
    outputFileNames: {
      embeddingPayloadFile: embeddingPayloadName,
      searchPayloadFile: searchPayloadName
    }
  });

  await fs.mkdir(reportsDir, { recursive: true });

  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);
  const embeddingPayloadPath = path.resolve(reportsDir, embeddingPayloadName);
  const searchPayloadPath = path.resolve(reportsDir, searchPayloadName);
  const activationManifestPath = path.resolve(reportsDir, activationManifestName);
  const rollbackManifestPath = path.resolve(reportsDir, rollbackManifestName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatRetrievalActivationBundleMarkdown(report));
  await fs.writeFile(embeddingPayloadPath, JSON.stringify(report.payloads.trustedEmbeddingPayload, null, 2));
  await fs.writeFile(searchPayloadPath, JSON.stringify(report.payloads.trustedSearchPayload, null, 2));
  await fs.writeFile(activationManifestPath, JSON.stringify(report.payloads.activationManifest, null, 2));
  await fs.writeFile(rollbackManifestPath, JSON.stringify(report.payloads.rollbackManifest, null, 2));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval activation bundle JSON report written to ${jsonPath}`);
  console.log(`Retrieval activation bundle Markdown report written to ${mdPath}`);
  console.log(`Trusted embedding payload JSON written to ${embeddingPayloadPath}`);
  console.log(`Trusted search payload JSON written to ${searchPayloadPath}`);
  console.log(`Trusted activation manifest JSON written to ${activationManifestPath}`);
  console.log(`Trusted rollback manifest JSON written to ${rollbackManifestPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
