import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number(process.env.CORPUS_LIMIT || "800");

async function fetchJson(pathname, init) {
  const response = await fetch(`${apiBase}${pathname}`, init);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  return { status: response.status, body };
}

function anchorOk(value) {
  return /^[A-Za-z0-9-]+#[a-z0-9_]+-p\d+(?:-c\d+)?$/.test(String(value || ""));
}

async function main() {
  const list = await fetchJson(`/admin/ingestion/documents?status=all&sort=createdAtDesc&limit=${limit}`);
  if (list.status !== 200) {
    throw new Error(`Failed to list ingestion docs: ${list.status} ${JSON.stringify(list.body)}`);
  }

  const documents = list.body.documents || [];
  const detailRows = [];
  const caseTypeDistribution = new Map();
  const warningDistribution = new Map();

  for (const doc of documents) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;

    const payload = detail.body;
    const chunkRows = payload.chunks || [];
    const anchorIssues = chunkRows.filter((chunk) => !anchorOk(chunk.citationAnchor)).length;
    const chunkWarnings = chunkRows.flatMap((chunk) => chunk.chunkWarnings || []);

    const caseType = payload.taxonomySuggestion?.caseTypeId || "unknown";
    caseTypeDistribution.set(caseType, (caseTypeDistribution.get(caseType) || 0) + 1);
    for (const warning of payload.extractionWarnings || []) {
      warningDistribution.set(warning, (warningDistribution.get(warning) || 0) + 1);
    }

    detailRows.push({
      id: payload.id,
      title: payload.title,
      status: payload.rejectedAt ? "rejected" : payload.approvedAt ? "approved" : payload.searchableAt ? "searchable" : "staged",
      fileType: payload.fileType,
      extractionConfidence: payload.extractionConfidence || 0,
      taxonomySuggestion: payload.taxonomySuggestion || null,
      missingRequiredMetadata:
        payload.fileType === "decision_docx" &&
        (!(payload.indexCodes || []).length || !(payload.rulesSections || []).length || !(payload.ordinanceSections || []).length),
      sectionCount: (payload.sections || []).length,
      chunkCount: chunkRows.length,
      anchorIssues,
      chunkWarnings
    });
  }

  const summary = {
    total: detailRows.length,
    approved: detailRows.filter((item) => item.status === "approved").length,
    searchable: detailRows.filter((item) => item.status === "searchable").length,
    staged: detailRows.filter((item) => item.status === "staged").length,
    rejected: detailRows.filter((item) => item.status === "rejected").length,
    missingRequiredMetadata: detailRows.filter((item) => item.missingRequiredMetadata).length,
    withWarnings: detailRows.filter((item) => item.chunkWarnings.length > 0).length,
    totalAnchorIssues: detailRows.reduce((sum, row) => sum + row.anchorIssues, 0),
    avgExtractionConfidence:
      detailRows.length > 0
        ? Number((detailRows.reduce((sum, row) => sum + row.extractionConfidence, 0) / detailRows.length).toFixed(3))
        : 0
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    summary,
    caseTypeDistribution: Object.fromEntries(Array.from(caseTypeDistribution.entries()).sort((a, b) => b[1] - a[1])),
    warningDistribution: Object.fromEntries(Array.from(warningDistribution.entries()).sort((a, b) => b[1] - a[1])),
    anomalies: {
      noSections: detailRows.filter((item) => item.sectionCount === 0).map((item) => ({ id: item.id, title: item.title })),
      noChunks: detailRows.filter((item) => item.chunkCount === 0).map((item) => ({ id: item.id, title: item.title })),
      anchorIssues: detailRows.filter((item) => item.anchorIssues > 0).map((item) => ({ id: item.id, title: item.title, anchorIssues: item.anchorIssues })),
      lowConfidence: detailRows
        .filter((item) => item.extractionConfidence < 0.45)
        .map((item) => ({ id: item.id, title: item.title, extractionConfidence: item.extractionConfidence }))
    },
    details: detailRows
  };

  const outputPath = path.resolve(process.cwd(), "reports", "corpus-quality-report.json");
  await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
  console.log("Corpus quality summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Detailed report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
