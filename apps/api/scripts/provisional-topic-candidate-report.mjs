import fs from "node:fs/promises";
import path from "node:path";
import { execFileSync } from "node:child_process";
import {
  computeTopicSignals,
  computeWorthReprocessing,
  formatCandidateMarkdown,
  isLikelyFixtureDoc,
  TOPIC_FAMILIES
} from "./provisional-topic-candidate-utils.mjs";

const d1Path =
  process.env.D1_PATH ||
  path.resolve(
    process.cwd(),
    ".wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite"
  );

const r2Path =
  process.env.R2_PATH ||
  path.resolve(
    process.cwd(),
    ".wrangler/state/v3/r2/miniflare-R2BucketObject/a33fd6adb9e3675b7ca938da61720e6d6041832412a5f4eac8b94e5f7295f950.sqlite"
  );

const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.PROVISIONAL_TOPIC_CANDIDATE_REPORT_NAME || "provisional-topic-candidate-report.json";
const markdownName = process.env.PROVISIONAL_TOPIC_CANDIDATE_MARKDOWN_NAME || "provisional-topic-candidate-report.md";
const batchSize = Number.parseInt(process.env.PROVISIONAL_TOPIC_BATCH_SIZE || "12", 10);

function sqliteJson(dbPath, sql) {
  const raw = execFileSync("sqlite3", ["-json", `file:${dbPath}?mode=ro&immutable=1`, sql], {
    cwd: process.cwd(),
    encoding: "utf8"
  });
  return raw.trim() ? JSON.parse(raw) : [];
}

function buildTopicHitSql() {
  const clauses = [];
  for (const [topic, terms] of Object.entries(TOPIC_FAMILIES)) {
    const [directTerm, ...synonyms] = terms;
    clauses.push(`SUM(CASE WHEN lower(c.chunk_text) LIKE '%${directTerm.replace(/'/g, "''")}%' THEN 1 ELSE 0 END) AS ${topic}DirectHits`);
    clauses.push(
      `SUM(CASE WHEN ${synonyms
        .map((term) => `lower(c.chunk_text) LIKE '%${term.replace(/'/g, "''")}%'`)
        .join(" OR ")} THEN 1 ELSE 0 END) AS ${topic}SynonymHits`
    );
    for (const [index, term] of terms.entries()) {
      const key = `${topic}_${index}_${term.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase()}_hit`;
      clauses.push(`SUM(CASE WHEN lower(c.chunk_text) LIKE '%${term.replace(/'/g, "''")}%' THEN 1 ELSE 0 END) AS ${key}`);
    }
  }
  return clauses.join(",\n      ");
}

function queryCandidateRows() {
  const sql = `
    WITH ref_issues AS (
      SELECT
        document_id,
        COUNT(*) AS unresolvedReferenceCount,
        SUM(CASE WHEN normalized_value LIKE '%37.7%' OR raw_value LIKE '%37.7%' THEN 1 ELSE 0 END) AS unsafe37xReferenceCount
      FROM document_reference_issues
      GROUP BY document_id
    )
    SELECT
      d.id,
      d.title,
      d.citation,
      d.source_r2_key AS sourceR2Key,
      d.decision_date AS decisionDate,
      d.searchable_at AS searchableAt,
      d.qc_passed AS qcPassed,
      d.qc_required_confirmed AS qcRequiredConfirmed,
      d.extraction_confidence AS extractionConfidence,
      d.extraction_warnings_json AS extractionWarningsJson,
      COUNT(c.id) AS chunkCount,
      SUM(CASE WHEN c.chunk_text LIKE '%<w:%' OR c.chunk_text LIKE '%</w:%' OR c.chunk_text LIKE '%w:rPr%' THEN 1 ELSE 0 END) AS xmlChunkCount,
      SUM(CASE WHEN length(trim(c.chunk_text)) < 40 THEN 1 ELSE 0 END) AS tinyChunkCount,
      SUM(CASE WHEN lower(c.section_label) IN ('body', 'introduction', 'decision', 'city and county of san francisco') THEN 1 ELSE 0 END) AS lowValueSectionCount,
      SUM(CASE WHEN lower(c.section_label) IN ('findings of fact', 'conclusions of law', 'order') THEN 1 ELSE 0 END) AS usefulSectionCount,
      COALESCE(MAX(ref_issues.unresolvedReferenceCount), 0) AS unresolvedReferenceCount,
      COALESCE(MAX(ref_issues.unsafe37xReferenceCount), 0) AS unsafe37xReferenceCount,
      ${buildTopicHitSql()}
    FROM documents d
    JOIN document_chunks c ON c.document_id = d.id
    LEFT JOIN ref_issues ON ref_issues.document_id = d.id
    WHERE d.file_type = 'decision_docx'
      AND d.searchable_at IS NULL
      AND d.rejected_at IS NULL
    GROUP BY
      d.id,
      d.title,
      d.citation,
      d.source_r2_key,
      d.decision_date,
      d.searchable_at,
      d.qc_passed,
      d.qc_required_confirmed,
      d.extraction_confidence,
      d.extraction_warnings_json
    ORDER BY d.decision_date DESC, d.created_at DESC
  `;

  const d1Rows = sqliteJson(d1Path, sql);
  const r2Keys = new Set(sqliteJson(r2Path, "SELECT key FROM _mf_objects").map((row) => row.key));
  return d1Rows.map((row) => ({ ...row, r2ObjectPresent: r2Keys.has(row.sourceR2Key) ? 1 : 0 }));
}

function buildReport(rows) {
  const stagedRealRows = rows.filter((row) => !isLikelyFixtureDoc(row));
  const enriched = stagedRealRows.map((row) => {
    const topicSignals = computeTopicSignals(row);
    const heuristic = computeWorthReprocessing(row);
    return {
      ...row,
      topicSignals,
      heuristic
    };
  });

  const topicBuckets = Object.fromEntries(
    Object.keys(TOPIC_FAMILIES).map((topic) => [
      topic,
      enriched
        .filter((row) => row.topicSignals[topic].totalHits > 0)
        .sort((a, b) => b.heuristic.score - a.heuristic.score || b.topicSignals[topic].totalHits - a.topicSignals[topic].totalHits)
        .map((row) => ({
          id: row.id,
          title: row.title,
          citation: row.citation,
          sourceR2Key: row.sourceR2Key,
          decisionDate: row.decisionDate,
          extractionConfidence: row.extractionConfidence,
          unresolvedReferenceCount: row.unresolvedReferenceCount,
          topic: row.topicSignals[topic],
          heuristic: row.heuristic
        }))
    ])
  );

  const recommendedBatch = enriched
    .filter((row) => row.heuristic.worthReprocessing)
    .sort((a, b) => b.heuristic.score - a.heuristic.score || b.heuristic.strongestTopicHitCount - a.heuristic.strongestTopicHitCount)
    .slice(0, batchSize)
    .map((row) => ({
      id: row.id,
      title: row.title,
      citation: row.citation,
      sourceR2Key: row.sourceR2Key,
      topicSignals: row.topicSignals,
      heuristic: row.heuristic
    }));

  return {
    generatedAt: new Date().toISOString(),
    inputs: {
      d1Path,
      r2Path,
      batchSize
    },
    summary: {
      realStagedDecisionCount: stagedRealRows.length,
      topicLikelyCount: enriched.filter((row) => row.heuristic.strongestTopicHitCount > 0).length,
      worthReprocessingCount: recommendedBatch.length
    },
    heuristicDefinition: {
      requireTopicSignal: true,
      requireR2ObjectPresent: true,
      blockOnXmlRatioAbove: 0.45,
      blockOnTinyChunkRatioAbove: 0.35,
      blockOnExtractionConfidenceBelow: 0.5,
      blockOnUsefulSectionCountBelow: 1
    },
    topicBuckets,
    recommendedBatch,
    blockedButTopicLikely: enriched
      .filter((row) => row.heuristic.strongestTopicHitCount > 0 && !row.heuristic.worthReprocessing)
      .sort((a, b) => b.heuristic.score - a.heuristic.score)
      .map((row) => ({
        id: row.id,
        title: row.title,
        strongestTopic: row.heuristic.strongestTopic,
        heuristic: row.heuristic
      }))
  };
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const report = buildReport(queryCandidateRows());
  const reportPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);

  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(markdownPath, formatCandidateMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Provisional topic candidate JSON report written to ${reportPath}`);
  console.log(`Provisional topic candidate Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
