import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const apiBase = (process.env.API_BASE_URL || "https://beedle-api.clifton23.workers.dev").replace(/\/$/, "");
const databaseName = process.env.D1_REMOTE_DATABASE || "beedle";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.D1_REMOTE_INDEX_CODE_QA_JSON_NAME || "d1-remote-index-code-qa-report.json";
const markdownName = process.env.D1_REMOTE_INDEX_CODE_QA_MARKDOWN_NAME || "d1-remote-index-code-qa-report.md";
const requestTimeoutMs = Number(process.env.D1_REMOTE_INDEX_CODE_QA_TIMEOUT_MS || "30000");

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function wranglerSql(sql) {
  const { stdout } = await execFileAsync("npx", ["wrangler", "d1", "execute", databaseName, "--remote", "--json", "--command", sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  const parsed = JSON.parse(stdout || "[]");
  const failed = parsed.find((item) => item && item.success === false);
  if (failed) throw new Error(`Remote D1 SQL failed: ${JSON.stringify(failed)}`);
  return parsed.flatMap((item) => item.results || []);
}

async function fetchSearch(payload) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    const response = await fetch(`${apiBase}/search`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal
    });
    const raw = await response.text();
    let body;
    try {
      body = JSON.parse(raw || "{}");
    } catch {
      throw new Error(`Search returned non-JSON status=${response.status}: ${raw.slice(0, 300)}`);
    }
    if (!response.ok) throw new Error(`Search failed status=${response.status}: ${JSON.stringify(body).slice(0, 500)}`);
    return body;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataIntersectionSql(codes) {
  const aliases = codes.map((code, index) => `l${index}`);
  const joins = aliases
    .map((alias, index) => {
      const code = codes[index];
      const base = `document_reference_links ${alias}`;
      const conditions = `${alias}.reference_type='index_code' AND ${alias}.canonical_value=${sqlQuote(code)} AND ${alias}.is_valid=1`;
      if (index === 0) return `FROM ${base}`;
      return `JOIN ${base} ON ${alias}.document_id = l0.document_id AND ${conditions}`;
    })
    .join("\n");
  return `SELECT COUNT(DISTINCT l0.document_id) AS count ${joins}\nWHERE l0.reference_type='index_code' AND l0.canonical_value=${sqlQuote(codes[0])} AND l0.is_valid=1;`;
}

function markdown(report) {
  const lines = [
    "# D1 Remote Index-Code QA Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API: \`${report.apiBase}\``,
    `- Remote database: \`${report.databaseName}\``,
    `- Passed: \`${report.passed}\``,
    "",
    "## Metadata",
    "",
    `- J32 docs: \`${report.metadata.j32Docs}\``,
    `- A19.1 docs: \`${report.metadata.a191Docs}\``,
    `- J32 + A19.1 docs: \`${report.metadata.j32AndA191Docs}\``,
    "",
    "## Search Probes",
    ""
  ];
  for (const probe of report.searchProbes) {
    lines.push(`- \`${probe.id}\`: total=\`${probe.total}\`, returned=\`${probe.returned}\`, pass=\`${probe.pass}\``);
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [metadataCounts] = await wranglerSql(`SELECT
    (SELECT COUNT(DISTINCT document_id) FROM document_reference_links WHERE reference_type='index_code' AND canonical_value='J32' AND is_valid=1) AS j32Docs,
    (SELECT COUNT(DISTINCT document_id) FROM document_reference_links WHERE reference_type='index_code' AND canonical_value='A19.1' AND is_valid=1) AS a191Docs,
    (${metadataIntersectionSql(["J32", "A19.1"]).replace(/;$/, "")}) AS j32AndA191Docs;`);

  const specs = [
    {
      id: "j32_filter",
      expectMinTotal: 1,
      payload: { query: "J32 hardship tenant", limit: 5, snippetMaxLength: 260, corpusMode: "trusted_plus_provisional", filters: { approvedOnly: false, indexCodes: ["J32"] } }
    },
    {
      id: "a191_filter",
      expectMinTotal: 1,
      payload: { query: "A19.1 no license rent increase", limit: 5, snippetMaxLength: 260, corpusMode: "trusted_plus_provisional", filters: { approvedOnly: false, indexCodes: ["A19.1"] } }
    },
    {
      id: "j32_a191_intersection_filter",
      expectExactTotal: Number(metadataCounts.j32AndA191Docs || 0),
      payload: { query: "J32 A19.1", limit: 5, snippetMaxLength: 260, corpusMode: "trusted_plus_provisional", filters: { approvedOnly: false, indexCodes: ["J32", "A19.1"] } }
    }
  ];

  const searchProbes = [];
  for (const spec of specs) {
    const response = await fetchSearch(spec.payload);
    const total = Number(response.total || 0);
    const returned = Array.isArray(response.results) ? response.results.length : 0;
    const pass = spec.expectExactTotal === undefined ? total >= spec.expectMinTotal : total === spec.expectExactTotal;
    searchProbes.push({ id: spec.id, total, returned, pass, topCitations: (response.results || []).slice(0, 5).map((row) => row.citation) });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    databaseName,
    metadata: metadataCounts,
    searchProbes,
    passed: searchProbes.every((probe) => probe.pass) && Number(metadataCounts.j32Docs || 0) > 0 && Number(metadataCounts.a191Docs || 0) > 0
  };

  await Promise.all([
    fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(report, null, 2)),
    fs.writeFile(path.join(reportsDir, markdownName), markdown(report))
  ]);
  console.log(JSON.stringify(report, null, 2));
  if (!report.passed) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
