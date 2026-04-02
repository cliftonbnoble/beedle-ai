import fs from "node:fs/promises";
import path from "node:path";

const reportAPath = process.env.PILOT_A_REPORT || path.resolve(process.cwd(), "reports", "pilot-import-report.json");
const reportBPath = process.env.PILOT_B_REPORT || path.resolve(process.cwd(), "reports", "pilot-import-report-2.json");
const outputName = process.env.PILOT_COMPARISON_REPORT_NAME || "pilot-comparison-report.json";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function metric(summary = {}) {
  const docs = Number(summary.succeeded || summary.documents || 0);
  const approved = Number(summary.approved || 0);
  const unresolved = Number(summary.unresolved_reference_count || 0);
  const warnings = Number(summary.warning_count || 0);
  const filteredNoise = Number(summary.filtered_noise_count || 0);
  const lowTax = Number(summary.low_confidence_taxonomy_count || summary.low_confidence_taxonomy_docs || 0);
  const avgConf = Number(summary.avg_extraction_confidence || 0);
  return {
    docs,
    approved,
    approval_rate: docs > 0 ? Number((approved / docs).toFixed(3)) : 0,
    unresolved_reference_rate: docs > 0 ? Number((unresolved / docs).toFixed(3)) : 0,
    avg_warning_count: docs > 0 ? Number((warnings / docs).toFixed(3)) : 0,
    avg_filtered_noise_count: docs > 0 ? Number((filteredNoise / docs).toFixed(3)) : 0,
    low_taxonomy_rate: docs > 0 ? Number((lowTax / docs).toFixed(3)) : 0,
    avg_extraction_confidence: avgConf
  };
}

async function main() {
  const [a, b] = await Promise.all([readJson(reportAPath), readJson(reportBPath)]);
  const aMetric = metric(a.summary);
  const bMetric = metric(b.summary);
  const output = {
    generatedAt: new Date().toISOString(),
    batch_a: { path: reportAPath, label: a.pilotLabel || "pilot_a", metrics: aMetric },
    batch_b: { path: reportBPath, label: b.pilotLabel || "pilot_b", metrics: bMetric },
    delta_b_minus_a: Object.fromEntries(
      Object.keys(aMetric).map((key) => [key, Number((bMetric[key] - aMetric[key]).toFixed(3))])
    )
  };

  const outputPath = path.resolve(process.cwd(), "reports", outputName);
  await fs.writeFile(outputPath, JSON.stringify(output, null, 2));
  console.log(JSON.stringify(output, null, 2));
  console.log(`\nComparison report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
