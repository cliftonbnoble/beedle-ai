import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

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
  const list = await fetchJson("/admin/ingestion/documents");
  if (list.status !== 200) {
    throw new Error(`Failed to list ingestion docs: ${list.status} ${JSON.stringify(list.body)}`);
  }

  const documents = list.body.documents || [];
  const details = [];

  for (const doc of documents) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;

    const payload = detail.body;
    const sectionCount = (payload.sections || []).length;
    const chunkCount = (payload.chunks || []).length;
    const anchorIssues = (payload.chunks || []).filter((chunk) => !anchorOk(chunk.citationAnchor)).length;

    details.push({
      id: payload.id,
      title: payload.title,
      fileType: payload.fileType,
      qcPassed: payload.qcPassed,
      qcRequiredConfirmed: payload.qcRequiredConfirmed,
      approvedAt: payload.approvedAt,
      rejectedAt: payload.rejectedAt,
      extractionConfidence: payload.extractionConfidence,
      warnings: payload.extractionWarnings || [],
      sectionCount,
      chunkCount,
      anchorIssues,
      indexCodes: payload.indexCodes || []
    });
  }

  const first = details[0];
  const querySet = [
    { name: "keyword_topic", queryType: "keyword", query: "variance", filters: { approvedOnly: false } },
    { name: "exact_phrase", queryType: "exact_phrase", query: "neighborhood notice", filters: { approvedOnly: false } },
    { name: "citation_lookup", queryType: "citation_lookup", query: first?.title ? first.title.split(" ").slice(0, 2).join(" ") : "decision", filters: { approvedOnly: false } },
    {
      name: "party_name",
      queryType: "party_name",
      query: first?.title ? first.title.split(" ")[0] : "applicant",
      filters: { approvedOnly: false, partyName: first?.title?.split(" ")?.[0] || undefined }
    },
    {
      name: "index_code_filtered",
      queryType: "index_code",
      query: "rule",
      filters: {
        approvedOnly: false,
        indexCode: first?.indexCodes?.[0] || "IC-104"
      }
    },
    {
      name: "rules_ordinance_filtered",
      queryType: "rules_ordinance",
      query: "ordinance",
      filters: {
        approvedOnly: false,
        rulesSection: "Rule 3.1",
        ordinanceSection: "Ordinance 77-19"
      }
    }
  ];

  const searchChecks = [];
  for (const check of querySet) {
    const response = await fetchJson("/admin/retrieval/debug", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: check.query, queryType: check.queryType, limit: 8, filters: check.filters })
    });

    searchChecks.push({
      ...check,
      status: response.status,
      total: response.body?.total ?? null,
      top: (response.body?.results || []).slice(0, 3).map((row) => ({
        title: row.title,
        citationAnchor: row.citationAnchor,
        sourceLink: row.sourceLink
      }))
    });
  }

  const summary = {
    totalDocs: details.length,
    pendingReview: details.filter((item) => !item.approvedAt && !item.rejectedAt).length,
    approved: details.filter((item) => item.approvedAt).length,
    rejected: details.filter((item) => item.rejectedAt).length,
    avgExtractionConfidence:
      details.length > 0 ? Number((details.reduce((sum, item) => sum + (item.extractionConfidence || 0), 0) / details.length).toFixed(2)) : 0,
    totalAnchorIssues: details.reduce((sum, item) => sum + item.anchorIssues, 0)
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    summary,
    details,
    searchChecks
  };

  const outputPath = path.resolve(process.cwd(), "reports", "pilot-eval-report.json");
  await fs.writeFile(outputPath, JSON.stringify(report, null, 2));

  console.log("Pilot evaluation summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Detailed report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
