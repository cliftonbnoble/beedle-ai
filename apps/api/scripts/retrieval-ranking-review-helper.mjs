import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const sourceName = process.env.RETRIEVAL_RANKING_REVIEW_SOURCE || "retrieval-golden-query-eval-report.json";
const jsonName = process.env.RETRIEVAL_RANKING_REVIEW_JSON_NAME || "retrieval-ranking-review-helper-report.json";
const markdownName = process.env.RETRIEVAL_RANKING_REVIEW_MARKDOWN_NAME || "retrieval-ranking-review-helper-report.md";
const csvName = process.env.RETRIEVAL_RANKING_REVIEW_CSV_NAME || "retrieval-ranking-review-helper-report.csv";
const topN = Number.parseInt(process.env.RETRIEVAL_RANKING_REVIEW_TOP_N || "3", 10);

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function buildCsv(rows) {
  const header = [
    "query_id",
    "family",
    "query",
    "expected",
    "rank",
    "document_id",
    "title",
    "chunk_id",
    "chunk_type",
    "score",
    "vector_score",
    "lexical_score",
    "citation_anchor",
    "snippet",
    "review_label",
    "review_notes"
  ];
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.queryId,
        row.family,
        row.query,
        row.expected,
        row.rank,
        row.documentId,
        row.title,
        row.chunkId,
        row.chunkType,
        row.score,
        row.vectorScore,
        row.lexicalScore,
        row.citationAnchor,
        row.snippet,
        "",
        ""
      ].map(csvEscape).join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Retrieval Ranking Review Helper",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Source report: \`${report.sourceReport}\``,
    `- Queries: \`${report.summary.queryCount}\``,
    `- Review rows: \`${report.summary.reviewRowCount}\``,
    `- Top results/query: \`${report.summary.topN}\``,
    "",
    "Use the CSV for quick labeling with `good`, `acceptable`, or `bad`.",
    ""
  ];

  for (const query of report.queries) {
    lines.push(`## ${query.query}`);
    lines.push("");
    lines.push(`- family: \`${query.family}\``);
    lines.push(`- expected: ${query.expected}`);
    lines.push(`- totalResultsSeen: \`${query.totalResults}\``);
    lines.push(`- uniqueDecisionCount: \`${query.uniqueDecisionCount}\``);
    if (query.error) {
      lines.push(`- error: ${query.error}`);
    }
    lines.push("");
    for (const row of query.reviewRows) {
      lines.push(
        `- #${row.rank} \`${row.title || row.documentId}\` | chunkType=\`${row.chunkType}\` | score=\`${row.score}\` | vector=\`${row.vectorScore}\` | lexical=\`${row.lexicalScore}\``
      );
      lines.push(`  - citation: \`${row.citationAnchor || "<none>"}\``);
      lines.push(`  - snippet: ${row.snippet || "<none>"}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const sourcePath = path.resolve(reportsDir, sourceName);
  const source = JSON.parse(await fs.readFile(sourcePath, "utf8"));
  const queries = Array.isArray(source?.queries) ? source.queries : [];

  const reviewQueries = queries.map((query) => {
    const reviewRows = (Array.isArray(query?.topResults) ? query.topResults : []).slice(0, topN).map((row) => ({
      rank: Number(row.rank || 0),
      documentId: row.documentId || null,
      title: row.title || null,
      chunkId: row.chunkId || null,
      chunkType: row.chunkType || "<none>",
      score: Number(row.score || 0),
      vectorScore: Number(row.vectorScore || 0),
      lexicalScore: Number(row.lexicalScore || 0),
      citationAnchor: row.citationAnchor || null,
      snippet: row.snippet || null
    }));
    return {
      id: query.id,
      family: query.family,
      query: query.query,
      expected: query.expected,
      totalResults: Number(query.totalResults || 0),
      uniqueDecisionCount: Number(query.uniqueDecisionCount || 0),
      reviewRows
    };
  });

  const csvRows = reviewQueries.flatMap((query) =>
    query.reviewRows.map((row) => ({
      queryId: query.id,
      family: query.family,
      query: query.query,
      expected: query.expected,
      ...row
    }))
  );

  const report = {
    generatedAt: new Date().toISOString(),
    sourceReport: sourcePath,
    summary: {
      queryCount: reviewQueries.length,
      reviewRowCount: csvRows.length,
      topN
    },
    queries: reviewQueries
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(csvRows));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval ranking-review helper JSON report written to ${jsonPath}`);
  console.log(`Retrieval ranking-review helper Markdown report written to ${markdownPath}`);
  console.log(`Retrieval ranking-review helper CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
