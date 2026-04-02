import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const status = process.env.REPROCESS_STATUS || "staged";
const decisionOnly = process.env.REPROCESS_DECISION_ONLY !== "0";
const limit = Number(process.env.REPROCESS_LIMIT || "200");
const reportName = process.env.REPROCESS_REPORT_NAME || "pilot-reprocess-report.json";

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    body = { raw };
  }
  if (!response.ok) {
    throw new Error(`API ${response.status} ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

function summarize(listBody) {
  const docs = listBody.documents || [];
  return {
    documents: docs.length,
    warning_count: docs.reduce((sum, d) => sum + (d.warningCount || 0), 0),
    unresolved_reference_count: docs.reduce((sum, d) => sum + (d.unresolvedReferenceCount || 0), 0),
    critical_exception_docs: docs.filter((d) => (d.criticalExceptionCount || 0) > 0).length,
    filtered_noise_count: docs.reduce((sum, d) => sum + (d.filteredNoiseCount || 0), 0),
    low_confidence_taxonomy_docs: docs.filter((d) => Boolean(d.lowConfidenceTaxonomy)).length,
    avg_extraction_confidence:
      docs.length > 0 ? Number((docs.reduce((sum, d) => sum + Number(d.extractionConfidence || 0), 0) / docs.length).toFixed(3)) : 0,
    with_warnings: docs.filter((d) => (d.warningCount || 0) > 0).length,
    with_unresolved: docs.filter((d) => (d.unresolvedReferenceCount || 0) > 0).length
  };
}

async function main() {
  const params = new URLSearchParams();
  params.set("status", status);
  params.set("limit", String(limit));
  params.set("sort", "unresolvedReferenceDesc");
  if (decisionOnly) params.set("fileType", "decision_docx");
  const before = await fetchJson(`${apiBase}/admin/ingestion/documents?${params.toString()}`);
  const targetDocs = before.documents || [];

  console.log(`Reprocessing ${targetDocs.length} documents from status=${status}`);
  const results = [];
  for (const doc of targetDocs) {
    try {
      const detail = await fetchJson(`${apiBase}/admin/ingestion/documents/${doc.id}/reprocess`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}"
      });
      results.push({
        id: doc.id,
        title: doc.title,
        ok: true,
        warningCount: Array.isArray(detail.extractionWarnings) ? detail.extractionWarnings.length : 0,
        unresolvedReferenceCount: typeof detail.unresolvedReferenceCount === "number" ? detail.unresolvedReferenceCount : 0
      });
      console.log(`REPROCESSED ${doc.id} ${doc.title}`);
    } catch (error) {
      results.push({ id: doc.id, title: doc.title, ok: false, error: error instanceof Error ? error.message : String(error) });
      console.log(`FAILED ${doc.id} ${doc.title}`);
    }
  }

  const after = await fetchJson(`${apiBase}/admin/ingestion/documents?${params.toString()}`);
  const output = {
    apiBase,
    generatedAt: new Date().toISOString(),
    filters: { status, decisionOnly, limit },
    before: summarize(before),
    after: summarize(after),
    delta: {
      warning_count: summarize(after).warning_count - summarize(before).warning_count,
      unresolved_reference_count: summarize(after).unresolved_reference_count - summarize(before).unresolved_reference_count,
      filtered_noise_count: summarize(after).filtered_noise_count - summarize(before).filtered_noise_count,
      low_confidence_taxonomy_docs: summarize(after).low_confidence_taxonomy_docs - summarize(before).low_confidence_taxonomy_docs,
      avg_extraction_confidence: Number((summarize(after).avg_extraction_confidence - summarize(before).avg_extraction_confidence).toFixed(3)),
      with_warnings: summarize(after).with_warnings - summarize(before).with_warnings,
      with_unresolved: summarize(after).with_unresolved - summarize(before).with_unresolved
    },
    results
  };

  const outputPath = path.resolve(process.cwd(), "reports", reportName);
  await fs.writeFile(outputPath, JSON.stringify(output, null, 2));
  console.log(`\nBefore: ${JSON.stringify(output.before)}`);
  console.log(`After: ${JSON.stringify(output.after)}`);
  console.log(`Delta: ${JSON.stringify(output.delta)}`);
  console.log(`Report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
