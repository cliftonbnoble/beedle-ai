import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const sourceName = process.env.JUDGE_SEARCH_WORKSHEET_SOURCE || "judge-search-smoke-test-report.json";
const jsonName = process.env.JUDGE_SEARCH_WORKSHEET_JSON_NAME || "judge-search-eval-worksheet.json";
const csvName = process.env.JUDGE_SEARCH_WORKSHEET_CSV_NAME || "judge-search-eval-worksheet.csv";
const markdownName = process.env.JUDGE_SEARCH_WORKSHEET_MARKDOWN_NAME || "judge-search-eval-worksheet.md";
const topN = Number.parseInt(process.env.JUDGE_SEARCH_WORKSHEET_TOP_N || "3", 10);

const WORKSHEET_COLUMNS = [
  "queryId",
  "lane",
  "query",
  "expectation",
  "rank",
  "documentId",
  "title",
  "chunkType",
  "corpusTier",
  "score",
  "vectorScore",
  "lexicalScore",
  "citationAnchor",
  "snippet",
  "topResultRelevant",
  "top3OverallQuality",
  "snippetHelpfulness",
  "trustworthiness",
  "obviousJunkInTop3",
  "reviewLabel",
  "reviewNotes",
  "reviewedBy",
  "reviewedAt"
];

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function formatCsv(rows) {
  const lines = [WORKSHEET_COLUMNS.join(",")];
  for (const row of rows) {
    lines.push(WORKSHEET_COLUMNS.map((column) => csvEscape(row[column])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Judge Search Evaluation Worksheet",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Source report: \`${report.sourceReport}\``,
    `- Worksheet row count: \`${report.summary.worksheetRowCount}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Top rows per query: \`${report.summary.topN}\``,
    "",
    "## Scoring Rubric",
    "",
    "- `topResultRelevant`: `yes`, `mostly`, or `no`",
    "- `top3OverallQuality`: `strong`, `mixed`, or `weak`",
    "- `snippetHelpfulness`: `strong`, `adequate`, or `weak`",
    "- `trustworthiness`: `high`, `medium`, or `low`",
    "- `obviousJunkInTop3`: `none`, `some`, or `severe`",
    "- `reviewLabel`: overall `good`, `acceptable`, or `bad`",
    "",
    "## Tester Instructions",
    "",
    "1. Review each query as a single judge-style task.",
    "2. Look at the top 3 results together, not just the first row.",
    "3. Score the query once per row if needed, but use `reviewNotes` to explain the main issue succinctly.",
    "4. Prefer specific legal or factual criticism over generic comments.",
    "",
    "## Query Worksheet",
    ""
  ];

  for (const query of report.queries) {
    lines.push(`### ${query.query}`);
    lines.push("");
    lines.push(`- lane: \`${query.lane}\``);
    lines.push(`- expectation: ${query.expectation}`);
    lines.push("");
    for (const row of query.rows) {
      lines.push(`- #${row.rank} \`${row.title || row.documentId}\` | chunkType=\`${row.chunkType}\` | tier=\`${row.corpusTier || "<none>"}\` | score=\`${row.score}\``);
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

  const rows = queries.flatMap((query) =>
    (Array.isArray(query.topResults) ? query.topResults : []).slice(0, topN).map((row) => ({
      queryId: query.id,
      lane: query.lane,
      query: query.query,
      expectation: query.expectation,
      rank: row.rank,
      documentId: row.documentId || "",
      title: row.title || "",
      chunkType: row.chunkType || "",
      corpusTier: row.corpusTier || "",
      score: row.score ?? "",
      vectorScore: row.vectorScore ?? "",
      lexicalScore: row.lexicalScore ?? "",
      citationAnchor: row.citationAnchor || "",
      snippet: row.snippet || "",
      topResultRelevant: "",
      top3OverallQuality: "",
      snippetHelpfulness: "",
      trustworthiness: "",
      obviousJunkInTop3: "",
      reviewLabel: "",
      reviewNotes: "",
      reviewedBy: "",
      reviewedAt: ""
    }))
  );

  const report = {
    generatedAt: new Date().toISOString(),
    sourceReport: sourcePath,
    summary: {
      worksheetRowCount: rows.length,
      queryCount: queries.length,
      topN
    },
    queries: queries.map((query) => ({
      id: query.id,
      lane: query.lane,
      query: query.query,
      expectation: query.expectation,
      rows: rows.filter((row) => row.queryId === query.id)
    })),
    rows
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const csvPath = path.resolve(reportsDir, csvName);
  const markdownPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(csvPath, formatCsv(rows));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Judge search worksheet JSON written to ${jsonPath}`);
  console.log(`Judge search worksheet CSV written to ${csvPath}`);
  console.log(`Judge search worksheet Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
