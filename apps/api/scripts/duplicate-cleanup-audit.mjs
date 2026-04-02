import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.DUPLICATE_CLEANUP_AUDIT_REPORT_NAME || "duplicate-cleanup-audit-report.json";
const markdownName = process.env.DUPLICATE_CLEANUP_AUDIT_MARKDOWN_NAME || "duplicate-cleanup-audit-report.md";

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 20 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function formatMarkdown(report) {
  const lines = [
    "# Duplicate Cleanup Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    `- Duplicate citation groups: \`${report.summary.duplicateCitationGroups}\``,
    `- Duplicate rows total: \`${report.summary.duplicateRows}\``,
    `- Rows to keep: \`${report.summary.keepRowCount}\``,
    `- Rows to remove: \`${report.summary.removeRowCount}\``,
    `- Removable chunk rows: \`${report.summary.removableChunkRows}\``,
    `- Removable retrieval rows: \`${report.summary.removableRetrievalRows}\``,
    "",
    "## Safe Cleanup Rule",
    "",
    "- Keep the newest `decision_docx` row per citation using `updated_at`, then `created_at`, then `id` as the tie-break order.",
    "- Remove older duplicate rows only after confirming their dependent `document_chunks` and `retrieval_search_chunks` rows are scoped to those duplicate document ids.",
    "",
    "## Top Duplicate Groups",
    ""
  ];

  for (const row of report.topDuplicateGroups) {
    lines.push(
      `- \`${row.citation}\` | rows=${row.rowCount} | keep=\`${row.keepDocumentId}\` | remove=${row.removeDocumentIds.join(", ") || "<none>"} | removableChunks=${row.removableChunkRows} | removableRetrievalRows=${row.removableRetrievalRows}`
    );
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const duplicateGroups = await runSqlJson(`
    WITH ranked AS (
      SELECT
        id,
        citation,
        title,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
          PARTITION BY citation
          ORDER BY updated_at DESC, created_at DESC, id DESC
        ) AS rank_in_group,
        COUNT(*) OVER (PARTITION BY citation) AS row_count
      FROM documents
      WHERE file_type = 'decision_docx'
    )
    SELECT
      citation,
      row_count,
      json_group_array(
        json_object(
          'documentId', id,
          'title', title,
          'createdAt', created_at,
          'updatedAt', updated_at,
          'rankInGroup', rank_in_group
        )
      ) AS ranked_rows
    FROM ranked
    WHERE row_count > 1
    GROUP BY citation, row_count
    ORDER BY row_count DESC, citation ASC;
  `);

  const parsedGroups = duplicateGroups.map((row) => {
    const rankedRows = JSON.parse(row.ranked_rows || "[]");
    const keep = rankedRows.find((item) => Number(item.rankInGroup) === 1) || rankedRows[0] || null;
    const removeRows = rankedRows.filter((item) => Number(item.rankInGroup) !== 1);
    return {
      citation: row.citation,
      rowCount: Number(row.row_count || 0),
      keepDocumentId: keep?.documentId || null,
      keepTitle: keep?.title || null,
      removeDocumentIds: removeRows.map((item) => item.documentId),
      rankedRows
    };
  });

  const removableIds = parsedGroups.flatMap((row) => row.removeDocumentIds);
  const removableIdList = removableIds.length ? removableIds.map((id) => `'${String(id).replace(/'/g, "''")}'`).join(", ") : "''";

  const chunkCounts = removableIds.length
    ? await runSqlJson(`
        SELECT document_id AS documentId, COUNT(*) AS chunkCount
        FROM document_chunks
        WHERE document_id IN (${removableIdList})
        GROUP BY document_id
      `)
    : [];
  const retrievalCounts = removableIds.length
    ? await runSqlJson(`
        SELECT document_id AS documentId, COUNT(*) AS retrievalCount
        FROM retrieval_search_chunks
        WHERE document_id IN (${removableIdList})
        GROUP BY document_id
      `)
    : [];

  const chunkMap = new Map(chunkCounts.map((row) => [row.documentId, Number(row.chunkCount || 0)]));
  const retrievalMap = new Map(retrievalCounts.map((row) => [row.documentId, Number(row.retrievalCount || 0)]));

  const topDuplicateGroups = parsedGroups.slice(0, 25).map((row) => ({
    ...row,
    removableChunkRows: row.removeDocumentIds.reduce((sum, id) => sum + (chunkMap.get(id) || 0), 0),
    removableRetrievalRows: row.removeDocumentIds.reduce((sum, id) => sum + (retrievalMap.get(id) || 0), 0)
  }));

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    summary: {
      duplicateCitationGroups: parsedGroups.length,
      duplicateRows: removableIds.length,
      keepRowCount: parsedGroups.length,
      removeRowCount: removableIds.length,
      removableChunkRows: removableIds.reduce((sum, id) => sum + (chunkMap.get(id) || 0), 0),
      removableRetrievalRows: removableIds.reduce((sum, id) => sum + (retrievalMap.get(id) || 0), 0)
    },
    topDuplicateGroups
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Duplicate cleanup audit JSON report written to ${jsonPath}`);
  console.log(`Duplicate cleanup audit Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
