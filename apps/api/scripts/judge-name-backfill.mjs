import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.JUDGE_NAME_BACKFILL_JSON_NAME || "judge-name-backfill-report.json";
const markdownName = process.env.JUDGE_NAME_BACKFILL_MARKDOWN_NAME || "judge-name-backfill-report.md";
const apply = (process.env.JUDGE_NAME_BACKFILL_APPLY || "0") === "1";

const CANONICAL_JUDGE_NAMES = [
  "René Juárez",
  "Andrew Yick",
  "Connie Brandon",
  "Deborah K. Lim",
  "Dorothy Chou Proudfoot",
  "Erin E. Katayama",
  "Harrison Nam",
  "Jeffrey Eckber",
  "Jill Figg Dayal",
  "Joseph Koomas",
  "Michael J. Berg",
  "Peter Kearns"
];

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function stripDiacritics(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function normalizeJudgeLookupKey(value) {
  return stripDiacritics(value)
    .toLowerCase()
    .replace(/^(judge|hon\.?|honorable|administrative law judge|alj|hearing officer|dated|date)\s+/i, "")
    .replace(/^\/?s\/?\s*/i, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalizeJudgeName(value) {
  const raw = normalizeWhitespace(value);
  if (!raw) return null;
  const normalized = normalizeJudgeLookupKey(raw);
  if (!normalized) return null;
  for (const judge of CANONICAL_JUDGE_NAMES) {
    const judgeKey = normalizeJudgeLookupKey(judge);
    if (normalized === judgeKey || normalized.includes(judgeKey) || judgeKey.includes(normalized)) {
      return judge;
    }
  }
  return raw;
}

function isInvalidAuthorName(value) {
  const raw = normalizeWhitespace(value);
  if (!raw) return false;
  const normalized = normalizeJudgeLookupKey(raw);
  if (!normalized) return true;
  return (
    raw === "an Francisco, CA" ||
    raw === "San Francisco, CA" ||
    /san francisco|francisco ca|city and county|california/.test(normalized)
  );
}

function extractCanonicalJudgesFromText(text) {
  const lookup = normalizeJudgeLookupKey(text);
  if (!lookup) return [];
  const matched = [];
  for (const judge of CANONICAL_JUDGE_NAMES) {
    const judgeKey = normalizeJudgeLookupKey(judge);
    if (judgeKey && lookup.includes(judgeKey)) matched.push(judge);
  }
  return Array.from(new Set(matched));
}

function inferJudgeFromTailText(tailText) {
  const raw = String(tailText || "");
  if (!raw.trim()) {
    return { inferredJudgeName: null, matchType: "no_tail_text", matchedJudges: [] };
  }

  const signatureMatch = raw.match(/\n\/?s\/?\s*([A-Z][A-Za-z .,'-]{3,90})\b/i);
  if (signatureMatch?.[1]) {
    const canonical = canonicalizeJudgeName(signatureMatch[1]);
    if (canonical && CANONICAL_JUDGE_NAMES.includes(canonical)) {
      return { inferredJudgeName: canonical, matchType: "signature_match", matchedJudges: [canonical] };
    }
  }

  const datedWindow = raw.match(/dated[\s\S]{0,240}$/i);
  const datedMatches = extractCanonicalJudgesFromText(datedWindow?.[0] || raw);
  if (datedMatches.length === 1) {
    return { inferredJudgeName: datedMatches[0], matchType: "dated_block_match", matchedJudges: datedMatches };
  }
  if (datedMatches.length > 1) {
    return { inferredJudgeName: null, matchType: "ambiguous_dated_block", matchedJudges: datedMatches };
  }

  const fullMatches = extractCanonicalJudgesFromText(raw);
  if (fullMatches.length === 1) {
    return { inferredJudgeName: fullMatches[0], matchType: "tail_text_match", matchedJudges: fullMatches };
  }
  if (fullMatches.length > 1) {
    return { inferredJudgeName: null, matchType: "ambiguous_tail_text", matchedJudges: fullMatches };
  }

  return { inferredJudgeName: null, matchType: "no_match", matchedJudges: [] };
}

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function runSql(sql) {
  await execFileAsync("sqlite3", [dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

function formatMarkdown(report) {
  const lines = [
    "# Judge Name Backfill Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Database: \`${report.dbPath}\``,
    `- Decision docs inspected: \`${report.summary.totalDecisionDocs}\``,
    `- Canonical author names already present: \`${report.summary.canonicalAlreadyPresent}\``,
    `- Missing author names: \`${report.summary.missingAuthorNameCount}\``,
    `- Invalid author names: \`${report.summary.invalidAuthorNameCount}\``,
    `- Candidate repairs: \`${report.summary.candidateRepairCount}\``,
    `- Applied updates: \`${report.summary.appliedUpdateCount}\``,
    `- Ambiguous candidates: \`${report.summary.ambiguousCount}\``,
    "",
    "## Canonical Counts",
    ""
  ];

  for (const row of report.summary.canonicalCounts) {
    lines.push(`- \`${row.judgeName}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Candidate Repairs");
  lines.push("");
  for (const row of report.candidateRepairs.slice(0, 50)) {
    lines.push(
      `- \`${row.citation}\` | before=\`${row.currentAuthorName || "<none>"}\` | after=\`${row.nextAuthorName || "<null>"}\` | reason=\`${row.repairReason}\` | match=\`${row.matchType}\``
    );
  }

  lines.push("");
  lines.push("## Ambiguous Candidates");
  lines.push("");
  for (const row of report.ambiguousCandidates.slice(0, 25)) {
    lines.push(`- \`${row.citation}\` | matches=\`${(row.matchedJudges || []).join(", ")}\``);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const rows = await runSqlJson(`
    SELECT
      d.id,
      d.title,
      d.citation,
      d.author_name AS currentAuthorName,
      d.decision_date AS decisionDate,
      (
        SELECT group_concat(chunk_text, char(10) || char(10))
        FROM (
          SELECT c.chunk_text
          FROM document_chunks c
          WHERE c.document_id = d.id
          ORDER BY c.chunk_order DESC, c.created_at DESC
          LIMIT 8
        )
      ) AS tailText
    FROM documents d
    WHERE d.file_type = 'decision_docx'
    ORDER BY d.created_at DESC;
  `);

  const inspected = rows.map((row) => {
    const canonicalCurrent = canonicalizeJudgeName(row.currentAuthorName);
    const currentIsCanonical = canonicalCurrent && CANONICAL_JUDGE_NAMES.includes(canonicalCurrent);
    const invalidCurrentAuthor = isInvalidAuthorName(row.currentAuthorName);
    const inferred = inferJudgeFromTailText(row.tailText);
    let nextAuthorName = null;
    let repairReason = null;

    if (inferred.inferredJudgeName) {
      const sameAsCurrent =
        currentIsCanonical && normalizeJudgeLookupKey(canonicalCurrent) === normalizeJudgeLookupKey(inferred.inferredJudgeName);
      if (!sameAsCurrent) {
        nextAuthorName = inferred.inferredJudgeName;
        repairReason = invalidCurrentAuthor
          ? "replace_invalid_author_with_inferred_judge"
          : normalizeWhitespace(row.currentAuthorName)
            ? "replace_noncanonical_author_with_inferred_judge"
            : "fill_missing_author_with_inferred_judge";
      }
    } else if (invalidCurrentAuthor) {
      nextAuthorName = null;
      repairReason = "clear_invalid_author_name";
    }

    const shouldUpdate = repairReason !== null;

    return {
      id: row.id,
      title: row.title,
      citation: row.citation,
      decisionDate: row.decisionDate,
      currentAuthorName: row.currentAuthorName || null,
      canonicalCurrentAuthorName: currentIsCanonical ? canonicalCurrent : null,
      invalidCurrentAuthor,
      inferredJudgeName: inferred.inferredJudgeName,
      nextAuthorName,
      repairReason,
      matchType: inferred.matchType,
      matchedJudges: inferred.matchedJudges,
      shouldUpdate,
      tailPreview: normalizeWhitespace(String(row.tailText || "").slice(-320))
    };
  });

  const candidateRepairs = inspected.filter((row) => row.shouldUpdate);
  const ambiguousCandidates = inspected.filter((row) => row.matchType.startsWith("ambiguous"));
  const missingAuthorNameCount = inspected.filter((row) => !normalizeWhitespace(row.currentAuthorName)).length;
  const invalidAuthorNameCount = inspected.filter((row) => row.invalidCurrentAuthor).length;
  const canonicalAlreadyPresent = inspected.filter((row) => row.canonicalCurrentAuthorName).length;

  if (apply && candidateRepairs.length > 0) {
    const statements = ["BEGIN IMMEDIATE;"];
    for (const row of candidateRepairs) {
      statements.push(
        `UPDATE documents SET author_name = ${row.nextAuthorName ? sqlQuote(row.nextAuthorName) : "NULL"}, updated_at = datetime('now') WHERE id = ${sqlQuote(row.id)};`
      );
    }
    statements.push("COMMIT;");
    await runSql(statements.join("\n"));
  }

  const canonicalCounts = CANONICAL_JUDGE_NAMES.map((judgeName) => ({
    judgeName,
    count: inspected.filter((row) => {
      const finalName = apply && row.shouldUpdate ? row.inferredJudgeName : row.canonicalCurrentAuthorName || row.currentAuthorName;
      return normalizeJudgeLookupKey(finalName || "") === normalizeJudgeLookupKey(judgeName);
    }).length
  })).sort((a, b) => b.count - a.count || a.judgeName.localeCompare(b.judgeName));

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    dbPath,
    summary: {
      totalDecisionDocs: inspected.length,
      missingAuthorNameCount,
      invalidAuthorNameCount,
      canonicalAlreadyPresent,
      candidateRepairCount: candidateRepairs.length,
      ambiguousCount: ambiguousCandidates.length,
      appliedUpdateCount: apply ? candidateRepairs.length : 0,
      canonicalCounts
    },
    candidateRepairs,
    ambiguousCandidates
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Judge name backfill JSON report written to ${jsonPath}`);
  console.log(`Judge name backfill Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
