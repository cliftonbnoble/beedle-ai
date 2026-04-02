import fs from "node:fs/promises";
import path from "node:path";
import { formatRetrievalActivationWriteMarkdown, validateActivationWriteReport } from "./retrieval-activation-write-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), process.env.RETRIEVAL_REPORTS_DIR || "reports");

const embeddingPayloadPath = path.resolve(
  reportsDir,
  process.env.RETRIEVAL_TRUSTED_EMBEDDING_PAYLOAD_NAME || "retrieval-trusted-embedding-payload.json"
);
const searchPayloadPath = path.resolve(
  reportsDir,
  process.env.RETRIEVAL_TRUSTED_SEARCH_PAYLOAD_NAME || "retrieval-trusted-search-payload.json"
);
const activationManifestPath = path.resolve(
  reportsDir,
  process.env.RETRIEVAL_TRUSTED_ACTIVATION_MANIFEST_NAME || "retrieval-trusted-activation-manifest.json"
);
const rollbackManifestPath = path.resolve(
  reportsDir,
  process.env.RETRIEVAL_TRUSTED_ROLLBACK_MANIFEST_NAME || "retrieval-trusted-rollback-manifest.json"
);

const reportName = process.env.RETRIEVAL_ACTIVATION_WRITE_REPORT_NAME || "retrieval-activation-write-report.json";
const markdownName = process.env.RETRIEVAL_ACTIVATION_WRITE_MARKDOWN_NAME || "retrieval-activation-write-report.md";
const verificationName = process.env.RETRIEVAL_ACTIVATION_VERIFICATION_NAME || "retrieval-activation-verification.json";

const dryRun = (process.env.RETRIEVAL_ACTIVATION_DRY_RUN || "0") === "1";
const performVectorUpsert = (process.env.RETRIEVAL_PERFORM_VECTOR_UPSERT || "1") !== "0";
const requestTimeoutMs = Math.max(
  1_000,
  Number.parseInt(process.env.RETRIEVAL_ACTIVATION_WRITE_TIMEOUT_MS || "300000", 10)
);

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function postWrite(payload) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(`Activation write timed out after ${requestTimeoutMs}ms`), requestTimeoutMs);
  const response = await fetch(`${apiBase}/admin/retrieval/activation/write`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
    signal: controller.signal
  }).finally(() => clearTimeout(timeout));

  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from activation write endpoint, got: ${raw.slice(0, 300)}`);
  }

  if (!response.ok) {
    throw new Error(`Activation write failed (${response.status}): ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  const [embeddingPayload, searchPayload, activationManifest, rollbackManifest] = await Promise.all([
    readJson(embeddingPayloadPath),
    readJson(searchPayloadPath),
    readJson(activationManifestPath),
    readJson(rollbackManifestPath)
  ]);

  const payload = {
    embeddingPayload,
    searchPayload,
    activationManifest,
    rollbackManifest,
    dryRun,
    performVectorUpsert
  };

  const writeReport = await postWrite(payload);
  const verificationChecks = validateActivationWriteReport(writeReport, {
    trustedDocIds: activationManifest.documentsToActivate || [],
    trustedChunkIds: activationManifest.chunksToActivate || []
  });

  await fs.mkdir(reportsDir, { recursive: true });
  const reportPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const verificationPath = path.resolve(reportsDir, verificationName);

  await fs.writeFile(reportPath, JSON.stringify(writeReport, null, 2));
  await fs.writeFile(markdownPath, formatRetrievalActivationWriteMarkdown(writeReport));
  await fs.writeFile(
    verificationPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        readOnly: Boolean(writeReport.readOnly),
        summary: writeReport.summary,
        verificationSummary: writeReport.verificationSummary,
        activationBatchSummary: writeReport.activationBatchSummary,
        rollbackVerificationSummary: writeReport.rollbackVerificationSummary,
        verification: writeReport.verification,
        verificationChecks
      },
      null,
      2
    )
  );

  console.log(JSON.stringify(writeReport.summary, null, 2));
  console.log(`Retrieval activation write JSON report written to ${reportPath}`);
  console.log(`Retrieval activation write Markdown report written to ${markdownPath}`);
  console.log(`Retrieval activation verification JSON written to ${verificationPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
