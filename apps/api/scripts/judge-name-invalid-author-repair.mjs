import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { strFromU8, unzipSync } from "fflate";

const execFileAsync = promisify(execFile);

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

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.JUDGE_INVALID_AUTHOR_JSON_NAME || "judge-name-invalid-author-repair-report.json";
const markdownName = process.env.JUDGE_INVALID_AUTHOR_MARKDOWN_NAME || "judge-name-invalid-author-repair-report.md";
const csvName = process.env.JUDGE_INVALID_AUTHOR_CSV_NAME || "judge-name-invalid-author-repair-report.csv";
const apply = (process.env.JUDGE_INVALID_AUTHOR_APPLY || "0") === "1";
const limit = Number(process.env.JUDGE_INVALID_AUTHOR_LIMIT || "250");
const busyTimeoutMs = Number(process.env.JUDGE_INVALID_AUTHOR_BUSY_TIMEOUT_MS || "5000");
const curlTimeoutSeconds = Number(process.env.JUDGE_INVALID_AUTHOR_CURL_TIMEOUT_SECONDS || "20");

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

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function runSql(sql) {
  await execFileAsync("sqlite3", ["-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

async function fetchSourceBytes(documentId) {
  const { stdout } = await execFileAsync(
    "curl",
    ["-fsS", "--max-time", String(curlTimeoutSeconds), `${apiBase}/source/${documentId}`],
    {
      cwd: process.cwd(),
      maxBuffer: 50 * 1024 * 1024,
      encoding: "buffer"
    }
  );
  return stdout;
}

async function assertApiHealthy() {
  await execFileAsync("curl", ["-fsS", "--max-time", String(curlTimeoutSeconds), `${apiBase}/health`], {
    cwd: process.cwd(),
    maxBuffer: 1024 * 1024
  });
}

function decodeXmlEntities(text) {
  return text
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#160;/g, " ")
    .replace(/&#xA0;/gi, " ");
}

function xmlToText(xml) {
  return decodeXmlEntities(
    xml
      .replace(/<w:tab[^>]*\/>/g, "\t")
      .replace(/<w:br[^>]*\/>/g, "\n")
      .replace(/<\/w:p>/g, "\n")
      .replace(/<\/w:tr>/g, "\n")
      .replace(/<\/w:tc>/g, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[ \t]{2,}/g, " ")
  );
}

function extractSourceText(bytes) {
  const files = unzipSync(new Uint8Array(bytes));
  const names = Object.keys(files)
    .filter((name) => /^word\/(document|header\d+|footer\d+)\.xml$/.test(name))
    .sort((a, b) => a.localeCompare(b));
  const parts = [];
  for (const name of names) {
    parts.push(xmlToText(strFromU8(files[name])));
  }
  return parts.join("\n\n");
}

async function extractSourceTextWithTextutil(bytes) {
  const tempPath = path.join(os.tmpdir(), `judge-invalid-author-${crypto.randomUUID()}.docx`);
  await fs.writeFile(tempPath, bytes);
  try {
    const { stdout } = await execFileAsync("/usr/bin/textutil", ["-convert", "txt", "-stdout", tempPath], {
      cwd: process.cwd(),
      maxBuffer: 50 * 1024 * 1024
    });
    return normalizeWhitespace(stdout);
  } finally {
    await fs.rm(tempPath, { force: true });
  }
}

async function extractRecoverableSourceText(bytes) {
  try {
    return extractSourceText(bytes);
  } catch (primaryError) {
    try {
      const fallbackText = await extractSourceTextWithTextutil(bytes);
      if (fallbackText) return fallbackText;
    } catch {
      // fall through
    }
    throw primaryError;
  }
}

function tailWindow(text, maxChars = 12000) {
  return text.length <= maxChars ? text : text.slice(-maxChars);
}

function inferJudgeFromSourceText(text) {
  const raw = String(text || "");
  if (!raw.trim()) {
    return { inferredJudgeName: null, matchType: "empty_source_text", matchedJudges: [], evidenceSnippet: "" };
  }

  const tail = tailWindow(raw);
  const datedWindow = tail.match(/dated[\s\S]{0,600}$/i)?.[0] || tail;
  const datedMatches = extractCanonicalJudgesFromText(datedWindow);
  if (datedMatches.length === 1) {
    return {
      inferredJudgeName: datedMatches[0],
      matchType: "source_dated_block_match",
      matchedJudges: datedMatches,
      evidenceSnippet: normalizeWhitespace(datedWindow.slice(0, 400))
    };
  }
  if (datedMatches.length > 1) {
    return {
      inferredJudgeName: null,
      matchType: "ambiguous_source_dated_block",
      matchedJudges: datedMatches,
      evidenceSnippet: normalizeWhitespace(datedWindow.slice(0, 400))
    };
  }

  const titleMatch = tail.match(/([A-Z][A-Za-z .,'’-]{3,90})\s+Administrative Law Judge/i);
  if (titleMatch?.[1]) {
    const canonical = canonicalizeJudgeName(titleMatch[1]);
    if (canonical && CANONICAL_JUDGE_NAMES.includes(canonical)) {
      return {
        inferredJudgeName: canonical,
        matchType: "source_title_line_match",
        matchedJudges: [canonical],
        evidenceSnippet: normalizeWhitespace(titleMatch[0])
      };
    }
  }

  const signatureMatch = tail.match(/\/?s\/?\s*([A-Z][A-Za-z .,'’-]{3,90})/i);
  if (signatureMatch?.[1]) {
    const canonical = canonicalizeJudgeName(signatureMatch[1]);
    if (canonical && CANONICAL_JUDGE_NAMES.includes(canonical)) {
      return {
        inferredJudgeName: canonical,
        matchType: "source_signature_match",
        matchedJudges: [canonical],
        evidenceSnippet: normalizeWhitespace(signatureMatch[0])
      };
    }
  }

  const tailMatches = extractCanonicalJudgesFromText(tail);
  if (tailMatches.length === 1) {
    return {
      inferredJudgeName: tailMatches[0],
      matchType: "source_tail_match",
      matchedJudges: tailMatches,
      evidenceSnippet: normalizeWhitespace(tail.slice(0, 400))
    };
  }
  if (tailMatches.length > 1) {
    return {
      inferredJudgeName: null,
      matchType: "ambiguous_source_tail",
      matchedJudges: tailMatches,
      evidenceSnippet: normalizeWhitespace(tail.slice(0, 400))
    };
  }

  return { inferredJudgeName: null, matchType: "no_source_match", matchedJudges: [], evidenceSnippet: "" };
}

function isClearlyInvalidNonblankAuthor(value) {
  const raw = normalizeWhitespace(value);
  if (!raw) return false;
  if (CANONICAL_JUDGE_NAMES.includes(raw)) return false;
  if (/san francisco|francisco|california/i.test(raw)) return true;
  if (/[.,]{2,}/.test(raw)) return true;
  if (/\b(llc|trust|properties|management)\b/i.test(raw)) return true;
  if (/\b(seismic|scaffolding|security|gate|retrofit|subtenant|smoke|sidewalk|roof|camera|petition|minute order|decision)\b/i.test(raw))
    return true;
  if (/[0-9]/.test(raw)) return true;
  if (raw.length < 4) return true;
  const looksLikeName = /^[A-Z][A-Za-z'’. -]+(?:\s+[A-Z][A-Za-z'’. -]+){1,4}$/.test(raw);
  if (!looksLikeName) return true;
  return false;
}

function countBy(values, labelKey = "key") {
  const counts = new Map();
  for (const value of values) {
    const key = normalizeWhitespace(value || "<unknown>");
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([key, count]) => ({ [labelKey]: key, count }))
    .sort((a, b) => b.count - a.count || String(a[labelKey]).localeCompare(String(b[labelKey])));
}

function buildCsv(report) {
  const header = [
    "citation",
    "title",
    "decision_date",
    "current_author_name",
    "next_author_name",
    "match_type",
    "matched_judges",
    "evidence_snippet"
  ];
  const lines = [header.join(",")];
  for (const row of report.candidateRepairs) {
    lines.push(
      [
        row.citation,
        row.title || "",
        row.decisionDate || "",
        row.currentAuthorName || "",
        row.nextAuthorName || "",
        row.matchType,
        (row.matchedJudges || []).join("; "),
        row.evidenceSnippet || ""
      ]
        .map((value) => {
          const text = String(value ?? "");
          return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
        })
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Judge Invalid Author Repair Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- API base: \`${report.apiBase}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- inspected docs: \`${report.summary.inspectedDocCount}\``,
    `- fetched docs: \`${report.summary.fetchedDocCount}\``,
    `- candidate repairs: \`${report.summary.candidateRepairCount}\``,
    `- applied updates: \`${report.summary.appliedUpdateCount}\``,
    `- fetch failures: \`${report.summary.fetchFailureCount}\``,
    `- ambiguous matches: \`${report.summary.ambiguousCount}\``,
    "",
    "## Top Invalid Current Author Values",
    ""
  ];

  for (const row of report.summary.invalidAuthorTopValues.slice(0, 25)) {
    lines.push(`- \`${row.authorName}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Recovery By Judge");
  lines.push("");

  for (const row of report.summary.byJudge) {
    lines.push(`- \`${row.judgeName}\`: \`${row.count}\``);
  }

  if (report.fetchFailures.length > 0) {
    lines.push("");
    lines.push("## Fetch Failures");
    lines.push("");
    for (const row of report.fetchFailures.slice(0, 25)) {
      lines.push(`- \`${row.citation}\`: \`${row.error}\``);
    }
  }

  lines.push("");
  lines.push("## Candidate Repairs");
  lines.push("");
  for (const row of report.candidateRepairs.slice(0, 50)) {
    lines.push(
      `- \`${row.citation}\` | before=\`${row.currentAuthorName}\` | after=\`${row.nextAuthorName}\` | match=\`${row.matchType}\``
    );
    if (row.evidenceSnippet) lines.push(`  - evidence: ${row.evidenceSnippet}`);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  await assertApiHealthy();

  const rows = await runSqlJson(`
    SELECT
      id,
      title,
      citation,
      decision_date AS decisionDate,
      author_name AS currentAuthorName,
      source_r2_key AS sourceKey
    FROM documents
    WHERE file_type = 'decision_docx'
      AND COALESCE(source_r2_key, '') != ''
      AND author_name IS NOT NULL
      AND trim(author_name) != ''
      AND (citation IS NULL OR citation NOT LIKE 'BEE-%')
      AND (citation IS NULL OR citation NOT LIKE 'KNOWN-REF-%')
      AND (citation IS NULL OR citation NOT LIKE 'PILOT-%')
      AND (citation IS NULL OR citation NOT LIKE 'HISTORICAL-%')
    ORDER BY decision_date DESC, citation ASC
    LIMIT ${Math.max(1, Math.floor(limit * 8))};
  `);

  const suspiciousRows = rows.filter((row) => isClearlyInvalidNonblankAuthor(row.currentAuthorName)).slice(0, limit);

  const candidateRepairs = [];
  const ambiguousCandidates = [];
  const fetchFailures = [];

  for (const row of suspiciousRows) {
    let sourceText = "";
    try {
      const bytes = await fetchSourceBytes(row.id);
      sourceText = await extractRecoverableSourceText(bytes);
    } catch (error) {
      fetchFailures.push({
        citation: row.citation,
        sourceKey: row.sourceKey,
        error: error instanceof Error ? error.message : String(error)
      });
      continue;
    }

    const inferred = inferJudgeFromSourceText(sourceText);
    if (inferred.matchType.startsWith("ambiguous")) {
      ambiguousCandidates.push({
        ...row,
        matchedJudges: inferred.matchedJudges,
        matchType: inferred.matchType,
        evidenceSnippet: inferred.evidenceSnippet
      });
      continue;
    }

    if (inferred.inferredJudgeName && inferred.inferredJudgeName !== row.currentAuthorName) {
      candidateRepairs.push({
        ...row,
        nextAuthorName: inferred.inferredJudgeName,
        matchedJudges: inferred.matchedJudges,
        matchType: inferred.matchType,
        evidenceSnippet: inferred.evidenceSnippet
      });
    }
  }

  if (apply && candidateRepairs.length > 0) {
    const statements = ["BEGIN IMMEDIATE;"];
    for (const row of candidateRepairs) {
      statements.push(
        `UPDATE documents
         SET author_name = ${sqlQuote(row.nextAuthorName)},
             updated_at = datetime('now')
         WHERE id = ${sqlQuote(row.id)};`
      );
    }
    statements.push("COMMIT;");
    await runSql(statements.join("\n"));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    apiBase,
    dbPath,
    candidateRepairs,
    ambiguousCandidates,
    fetchFailures,
    summary: {
      inspectedDocCount: suspiciousRows.length,
      fetchedDocCount: suspiciousRows.length - fetchFailures.length,
      candidateRepairCount: candidateRepairs.length,
      appliedUpdateCount: apply ? candidateRepairs.length : 0,
      fetchFailureCount: fetchFailures.length,
      ambiguousCount: ambiguousCandidates.length,
      invalidAuthorTopValues: countBy(suspiciousRows.map((row) => row.currentAuthorName), "authorName"),
      byJudge: countBy(candidateRepairs.map((row) => row.nextAuthorName), "judgeName")
    }
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Judge invalid-author repair JSON report written to ${jsonPath}`);
  console.log(`Judge invalid-author repair Markdown report written to ${markdownPath}`);
  console.log(`Judge invalid-author repair CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
