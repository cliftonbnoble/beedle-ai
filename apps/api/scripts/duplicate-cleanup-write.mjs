import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.DUPLICATE_CLEANUP_WRITE_REPORT_NAME || "duplicate-cleanup-write-report.json";
const markdownName = process.env.DUPLICATE_CLEANUP_WRITE_MARKDOWN_NAME || "duplicate-cleanup-write-report.md";
const apply = (process.env.DUPLICATE_CLEANUP_APPLY || "0") === "1";
const batchSize = Math.max(1, Number.parseInt(process.env.DUPLICATE_CLEANUP_BATCH_SIZE || "25", 10));
const busyTimeoutMs = Math.max(0, Number.parseInt(process.env.DUPLICATE_CLEANUP_BUSY_TIMEOUT_MS || "5000", 10));

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 50 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function runSql(sql) {
  await execFileAsync("sqlite3", [dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 50 * 1024 * 1024
  });
}

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function chunkArray(items, size) {
  const out = [];
  for (let index = 0; index < items.length; index += size) out.push(items.slice(index, index + size));
  return out;
}

function formatMarkdown(report) {
  const lines = [
    "# Duplicate Cleanup Write Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Database: \`${report.dbPath}\``,
    `- Duplicate citation groups: \`${report.summary.duplicateCitationGroups}\``,
    `- Duplicate rows removable: \`${report.summary.removableDuplicateRows}\``,
    `- Rows actually removed: \`${report.summary.removedDocumentRows}\``,
    `- Retrieval rows removed: \`${report.summary.removedRetrievalRows}\``,
    "",
    "## Keep Rule",
    "",
    "- Prefer rows with `approved_at`, then `searchable_at`, then `qc_passed`, then `qc_required_confirmed`, then newest `updated_at`/`created_at`/`id`.",
    "",
    "## Sample Decisions",
    ""
  ];

  for (const row of report.sampleGroups) {
    lines.push(
      `- \`${row.citation}\` | keep=\`${row.keepDocumentId}\` | remove=${(row.removeDocumentIds || []).join(", ") || "<none>"}`
    );
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function loadDuplicatePlan() {
  const rows = await runSqlJson(`
    WITH ranked AS (
      SELECT
        id,
        citation,
        title,
        approved_at,
        searchable_at,
        qc_passed,
        qc_required_confirmed,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
          PARTITION BY citation
          ORDER BY
            CASE WHEN approved_at IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN searchable_at IS NOT NULL THEN 1 ELSE 0 END DESC,
            qc_passed DESC,
            qc_required_confirmed DESC,
            updated_at DESC,
            created_at DESC,
            id DESC
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
          'approvedAt', approved_at,
          'searchableAt', searchable_at,
          'qcPassed', qc_passed,
          'qcRequiredConfirmed', qc_required_confirmed,
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

  return rows.map((row) => {
    const rankedRows = JSON.parse(row.ranked_rows || "[]");
    const keep = rankedRows.find((item) => Number(item.rankInGroup) === 1) || rankedRows[0] || null;
    const removeRows = rankedRows.filter((item) => Number(item.rankInGroup) !== 1);
    return {
      citation: row.citation,
      rowCount: Number(row.row_count || 0),
      keepRow: keep,
      removeRows
    };
  });
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  console.log(`Loading duplicate cleanup plan from ${dbPath}`);
  const groups = await loadDuplicatePlan();
  const removableIds = groups.flatMap((group) => group.removeRows.map((row) => row.documentId));

  let removedRetrievalRows = 0;
  let removedDocumentRows = 0;
  let removedChunkRows = 0;
  let removedSectionRows = 0;
  let removedParagraphRows = 0;

  console.log(
    JSON.stringify(
      {
        apply,
        batchSize,
        busyTimeoutMs,
        duplicateCitationGroups: groups.length,
        removableDuplicateRows: removableIds.length
      },
      null,
      2
    )
  );

  if (apply && removableIds.length > 0) {
    const batches = chunkArray(removableIds, batchSize);
    console.log(`Applying duplicate cleanup in ${batches.length} batches.`);

    for (const [batchIndex, ids] of batches.entries()) {
      const idList = ids.map(sqlQuote).join(", ");
      const [counts] = await runSqlJson(`
        SELECT
          (SELECT COUNT(*) FROM documents WHERE id IN (${idList})) AS documentCount,
          (SELECT COUNT(*) FROM retrieval_search_chunks WHERE document_id IN (${idList})) AS retrievalCount,
          (SELECT COUNT(*) FROM document_chunks WHERE document_id IN (${idList})) AS chunkCount,
          (SELECT COUNT(*) FROM document_sections WHERE document_id IN (${idList})) AS sectionCount,
          (SELECT COUNT(*) FROM section_paragraphs WHERE section_id IN (
            SELECT id FROM document_sections WHERE document_id IN (${idList})
          )) AS paragraphCount;
      `);

      console.log(
        `Batch ${batchIndex + 1}/${batches.length}: docs=${Number(counts?.documentCount || 0)} chunks=${Number(
          counts?.chunkCount || 0
        )} sections=${Number(counts?.sectionCount || 0)} paragraphs=${Number(counts?.paragraphCount || 0)} retrieval=${Number(
          counts?.retrievalCount || 0
        )}`
      );

      await runSql(`
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = ${busyTimeoutMs};
        BEGIN IMMEDIATE;
        DELETE FROM retrieval_search_chunks WHERE document_id IN (${idList});
        DELETE FROM document_chunks WHERE document_id IN (${idList});
        DELETE FROM section_paragraphs WHERE section_id IN (
          SELECT id FROM document_sections WHERE document_id IN (${idList})
        );
        DELETE FROM document_sections WHERE document_id IN (${idList});
        DELETE FROM documents WHERE id IN (${idList});
        COMMIT;
      `);

      removedDocumentRows += Number(counts?.documentCount || 0);
      removedRetrievalRows += Number(counts?.retrievalCount || 0);
      removedChunkRows += Number(counts?.chunkCount || 0);
      removedSectionRows += Number(counts?.sectionCount || 0);
      removedParagraphRows += Number(counts?.paragraphCount || 0);
      console.log(`Batch ${batchIndex + 1}/${batches.length}: complete.`);
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    dbPath,
    summary: {
      duplicateCitationGroups: groups.length,
      removableDuplicateRows: removableIds.length,
      removedDocumentRows,
      removedRetrievalRows,
      removedChunkRows,
      removedSectionRows,
      removedParagraphRows,
      batchSize,
      busyTimeoutMs
    },
    sampleGroups: groups.slice(0, 25).map((group) => ({
      citation: group.citation,
      keepDocumentId: group.keepRow?.documentId || null,
      removeDocumentIds: group.removeRows.map((row) => row.documentId)
    }))
  };

  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Duplicate cleanup write JSON report written to ${jsonPath}`);
  console.log(`Duplicate cleanup write Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
