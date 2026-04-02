import fs from "node:fs/promises";
import path from "node:path";
import { execFileSync } from "node:child_process";
import {
  computeTopicSignals,
  computeWorthReprocessing,
  isLikelyFixtureDoc,
  isLikelyFixtureSourceKey,
  scoreSourceImportCandidate,
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
const reportPath = path.resolve(reportsDir, "provisional-topic-source-acquisition-report.json");
const markdownPath = path.resolve(reportsDir, "provisional-topic-source-acquisition-report.md");

function sqliteJson(dbPath, sql) {
  const raw = execFileSync("sqlite3", ["-json", `file:${dbPath}?mode=ro&immutable=1`, sql], {
    cwd: process.cwd(),
    encoding: "utf8"
  });
  return raw.trim() ? JSON.parse(raw) : [];
}

function queryTopicDocs() {
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
      d.searchable_at AS searchableAt,
      d.extraction_confidence AS extractionConfidence,
      COUNT(c.id) AS chunkCount,
      SUM(CASE WHEN c.chunk_text LIKE '%<w:%' OR c.chunk_text LIKE '%</w:%' OR c.chunk_text LIKE '%w:rPr%' THEN 1 ELSE 0 END) AS xmlChunkCount,
      SUM(CASE WHEN length(trim(c.chunk_text)) < 40 THEN 1 ELSE 0 END) AS tinyChunkCount,
      SUM(CASE WHEN lower(c.section_label) IN ('body', 'introduction', 'decision', 'city and county of san francisco') THEN 1 ELSE 0 END) AS lowValueSectionCount,
      SUM(CASE WHEN lower(c.section_label) IN ('findings of fact', 'conclusions of law', 'order') THEN 1 ELSE 0 END) AS usefulSectionCount,
      COALESCE(MAX(ref_issues.unresolvedReferenceCount), 0) AS unresolvedReferenceCount,
      COALESCE(MAX(ref_issues.unsafe37xReferenceCount), 0) AS unsafe37xReferenceCount,
      ${Object.entries(TOPIC_FAMILIES)
        .flatMap(([topic, terms]) => {
          const [directTerm, ...synonyms] = terms;
          const clauses = [
            `SUM(CASE WHEN lower(c.chunk_text) LIKE '%${directTerm.replace(/'/g, "''")}%' THEN 1 ELSE 0 END) AS ${topic}DirectHits`,
            `SUM(CASE WHEN ${synonyms
              .map((term) => `lower(c.chunk_text) LIKE '%${term.replace(/'/g, "''")}%'`)
              .join(" OR ")} THEN 1 ELSE 0 END) AS ${topic}SynonymHits`
          ];
          for (const [index, term] of terms.entries()) {
            const key = `${topic}_${index}_${term.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase()}_hit`;
            clauses.push(`SUM(CASE WHEN lower(c.chunk_text) LIKE '%${term.replace(/'/g, "''")}%' THEN 1 ELSE 0 END) AS ${key}`);
          }
          return clauses;
        })
        .join(",\n      ")}
    FROM documents d
    JOIN document_chunks c ON c.document_id = d.id
    LEFT JOIN ref_issues ON ref_issues.document_id = d.id
    WHERE d.file_type = 'decision_docx'
      AND d.rejected_at IS NULL
      AND d.citation NOT LIKE 'BEE-%'
      AND d.title NOT LIKE 'Harness %'
      AND d.title NOT LIKE 'Retrieval %'
      AND d.title NOT LIKE 'R5 %'
    GROUP BY
      d.id, d.title, d.citation, d.source_r2_key, d.searchable_at, d.extraction_confidence
  `;
  return sqliteJson(d1Path, sql);
}

function queryUnmatchedR2Objects() {
  const sql = `
    ATTACH DATABASE '${d1Path.replace(/'/g, "''")}' AS d1;
    SELECT key
    FROM _mf_objects
    WHERE key LIKE 'decision_docx/%'
      AND key NOT IN (
        SELECT source_r2_key FROM d1.documents WHERE source_r2_key IS NOT NULL
      )
    ORDER BY key
  `;
  return sqliteJson(r2Path, sql);
}

function toMarkdown(report) {
  const lines = [
    "# Provisional Topic Source Acquisition Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Searchable topic-bearing real decisions: \`${report.summary.searchableTopicBearingCount}\``,
    `- Blocked topic-bearing local docs: \`${report.summary.blockedTopicBearingCount}\``,
    `- Unmatched local R2 decision objects: \`${report.summary.unmatchedDecisionObjectCount}\``,
    `- Viable unmatched import candidates: \`${report.summary.viableUnmatchedImportCount}\``,
    ""
  ];

  lines.push("## Conclusion");
  lines.push("");
  if (report.summary.viableUnmatchedImportCount === 0) {
    lines.push("- No strong unmatched local source objects were found for `cooling`, `ventilation`, or `mold`.");
    lines.push("- Better topic coverage will require either stronger parser cleanup on blocked docs or new source acquisition.");
  } else {
    lines.push("- Some unmatched local decision objects appear worth importing.");
  }
  lines.push("");

  lines.push("## Best Local Blocked Docs");
  lines.push("");
  if (!report.blockedTopicDocs.length) {
    lines.push("- None");
  } else {
    for (const row of report.blockedTopicDocs) {
      lines.push(
        `- \`${row.id}\` | ${row.title} | strongestTopic=${row.heuristic.strongestTopic} | score=${row.heuristic.score} | blockers=${row.heuristic.blockers.join(", ")}`
      );
    }
  }
  lines.push("");

  lines.push("## Unmatched Local R2 Decision Objects");
  lines.push("");
  if (!report.unmatchedImportCandidates.length) {
    lines.push("- None worth importing.");
  } else {
    for (const row of report.unmatchedImportCandidates) {
      lines.push(
        `- \`${row.key}\` | strongestTopic=${row.importHeuristic.strongestTopic} | score=${row.importHeuristic.score} | worthImporting=${row.importHeuristic.worthImporting}`
      );
    }
  }
  lines.push("");

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const topicDocs = queryTopicDocs()
    .filter((row) => !isLikelyFixtureDoc(row))
    .map((row) => {
      const topicSignals = computeTopicSignals(row);
      const heuristic = computeWorthReprocessing(row);
      return { ...row, topicSignals, heuristic };
    })
    .filter((row) => row.heuristic.strongestTopicHitCount > 0);

  const blockedTopicDocs = topicDocs
    .filter((row) => !row.searchableAt && !row.heuristic.worthReprocessing)
    .sort((a, b) => b.heuristic.score - a.heuristic.score)
    .slice(0, 12)
    .map((row) => ({
      id: row.id,
      title: row.title,
      citation: row.citation,
      sourceR2Key: row.sourceR2Key,
      heuristic: row.heuristic
    }));

  const unmatchedImportCandidates = queryUnmatchedR2Objects()
    .map((row) => ({
      key: row.key,
      importHeuristic: scoreSourceImportCandidate(row)
    }))
    .filter((row) => !isLikelyFixtureSourceKey(row.key))
    .sort((a, b) => b.importHeuristic.score - a.importHeuristic.score)
    .slice(0, 12);

  const searchableTopicBearingCount = topicDocs.filter((row) => Boolean(row.searchableAt)).length;
  const worthReprocessingTopicCount = topicDocs.filter((row) => row.heuristic.worthReprocessing).length;

  const report = {
    generatedAt: new Date().toISOString(),
    summary: {
      searchableTopicBearingCount,
      blockedTopicBearingCount: blockedTopicDocs.length,
      unmatchedDecisionObjectCount: queryUnmatchedR2Objects().length,
      viableUnmatchedImportCount: unmatchedImportCandidates.filter((row) => row.importHeuristic.worthImporting).length,
      worthReprocessingTopicCount,
      acquisitionNeeded:
        worthReprocessingTopicCount === 0 &&
        unmatchedImportCandidates.filter((row) => row.importHeuristic.worthImporting).length === 0
    },
    blockedTopicDocs,
    unmatchedImportCandidates
  };

  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(markdownPath, toMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Provisional topic source acquisition JSON report written to ${reportPath}`);
  console.log(`Provisional topic source acquisition Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
