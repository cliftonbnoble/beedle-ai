import fs from "node:fs/promises";
import path from "node:path";
import {
  buildRetrievalBatchExpansionReport,
  formatRetrievalBatchExpansionMarkdown
} from "./retrieval-batch-expansion-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);
const batchSize = Number.parseInt(process.env.RETRIEVAL_NEXT_BATCH_SIZE || "10", 10);

const reportName = process.env.RETRIEVAL_BATCH_EXPANSION_REPORT_NAME || "retrieval-batch-expansion-report.json";
const markdownName = process.env.RETRIEVAL_BATCH_EXPANSION_MARKDOWN_NAME || "retrieval-batch-expansion-report.md";
const candidatesName = process.env.RETRIEVAL_NEXT_BATCH_CANDIDATES_NAME || "retrieval-next-batch-candidates.json";
const manifestName = process.env.RETRIEVAL_NEXT_BATCH_MANIFEST_NAME || "retrieval-next-batch-manifest.json";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON response.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function resolveDocuments() {
  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || [])
    .map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture) }))
    .filter((doc) => doc.id);
}

async function loadPreviews(documentRows) {
  const previews = [];
  for (const doc of documentRows || []) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=1`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }
  return previews;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [activationWriteReport, corpusAdmission, referenceEnrichment, promotionRehearsal] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-activation-write-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-corpus-admission-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-reference-enrichment-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-promotion-rehearsal-report.json"))
  ]);

  const trustedDocumentIds = (activationWriteReport?.documentsActivated || [])
    .map((row) => row?.documentId)
    .filter(Boolean);

  if (!trustedDocumentIds.length) {
    throw new Error("No activated trusted document IDs found in retrieval-activation-write-report.json");
  }

  const documents = await loadPreviews(await resolveDocuments());

  const report = await buildRetrievalBatchExpansionReport({
    apiBase,
    documents,
    corpusAdmissionReport: corpusAdmission,
    referenceEnrichmentReport: referenceEnrichment,
    promotionRehearsalReport: promotionRehearsal,
    trustedDocumentIds,
    batchSize,
    liveLimit: 20,
    fetchSearchDebug: (payload) =>
      fetchJson(`${apiBase}/admin/retrieval/debug`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      })
  });

  const reportPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const candidatesPath = path.resolve(reportsDir, candidatesName);
  const manifestPath = path.resolve(reportsDir, manifestName);

  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatRetrievalBatchExpansionMarkdown(report));
  await fs.writeFile(
    candidatesPath,
    JSON.stringify(
      {
        generatedAt: report.generatedAt,
        readOnly: true,
        proposedNextBatch: report.proposedNextBatch,
        candidateRanking: report.candidateRanking,
        candidateExclusions: report.candidateExclusions
      },
      null,
      2
    )
  );
  await fs.writeFile(
    manifestPath,
    JSON.stringify(
      {
        generatedAt: report.generatedAt,
        readOnly: true,
        baselineTrustedDocIds: report.manifests.baselineTrustedDocIds,
        nextBatchDocIds: report.manifests.nextBatchDocIds,
        expandedTrustedDocIds: report.manifests.expandedTrustedDocIds,
        activationRecommendation: report.summary.activationRecommendation,
        regressionGate: report.regressionGate
      },
      null,
      2
    )
  );

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval batch expansion JSON report written to ${reportPath}`);
  console.log(`Retrieval batch expansion Markdown report written to ${markdownPath}`);
  console.log(`Retrieval next batch candidates JSON written to ${candidatesPath}`);
  console.log(`Retrieval next batch manifest JSON written to ${manifestPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
