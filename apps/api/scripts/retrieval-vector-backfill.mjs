import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME || "retrieval-vector-backfill-report.json";
const markdownName = process.env.RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME || "retrieval-vector-backfill-report.md";

function formatMarkdown(report) {
  return [
    "# Retrieval Vector Backfill Report",
    "",
    "## Summary",
    "",
    `- Generated at: ${report.generatedAt}`,
    `- Dry run: ${report.dryRun}`,
    `- AI available: ${report.aiAvailable}`,
    `- Vector binding present: ${report.vectorBindingPresent}`,
    `- Vector namespace: ${report.vectorNamespace}`,
    `- Embedding model: ${report.embeddingModel}`,
    `- Offset: ${report.offset ?? 0}`,
    `- Discovered chunks: ${report.counts.discoveredChunkCount}`,
    `- Processed: ${report.counts.processedCount}`,
    `- Embedded: ${report.counts.embeddedCount}`,
    `- Upserted: ${report.counts.upsertedCount}`,
    `- Failed: ${report.counts.failedCount}`,
    "",
    "## Source Counts",
    "",
    `- Document chunks: ${report.sourceCounts.documentChunkCount}`,
    `- Trusted chunks: ${report.sourceCounts.trustedChunkCount}`,
    "",
    "## Batches",
    "",
    ...report.batches.map(
      (row) =>
        `- Batch ${row.batchIndex}: rows=${row.rowCount}, embedded=${row.embeddedCount}, upserted=${row.upsertedCount}, failed=${row.failedCount}`
    )
  ].join("\n");
}

async function main() {
  const response = await fetch(`${apiBase}/admin/retrieval/vectors/backfill`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      batchSize: Number.parseInt(process.env.RETRIEVAL_VECTOR_BACKFILL_BATCH_SIZE || "25", 10),
      limit: process.env.RETRIEVAL_VECTOR_BACKFILL_LIMIT ? Number.parseInt(process.env.RETRIEVAL_VECTOR_BACKFILL_LIMIT, 10) : undefined,
      offset: Number.parseInt(process.env.RETRIEVAL_VECTOR_BACKFILL_OFFSET || "0", 10),
      dryRun: (process.env.RETRIEVAL_VECTOR_BACKFILL_DRY_RUN || "0") === "1",
      includeDocumentChunks: (process.env.RETRIEVAL_VECTOR_BACKFILL_INCLUDE_DOCUMENT_CHUNKS || "1") === "1",
      includeTrustedChunks: (process.env.RETRIEVAL_VECTOR_BACKFILL_INCLUDE_TRUSTED_CHUNKS || "1") === "1"
    })
  });

  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON response from vector backfill endpoint, got: ${raw.slice(0, 300)}`);
  }

  if (!response.ok) {
    throw new Error(`Vector backfill failed (${response.status}): ${JSON.stringify(body)}`);
  }

  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(body, null, 2));
  await fs.writeFile(mdPath, formatMarkdown(body));

  console.log(JSON.stringify(body.counts, null, 2));
  console.log(`Retrieval vector backfill JSON report written to ${jsonPath}`);
  console.log(`Retrieval vector backfill Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
