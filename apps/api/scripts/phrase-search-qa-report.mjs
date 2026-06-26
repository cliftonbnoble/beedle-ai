import fs from "node:fs/promises";
import path from "node:path";

const apiBase = (process.env.PHRASE_SEARCH_QA_API_BASE || process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.PHRASE_SEARCH_QA_JSON_NAME || "phrase-search-qa-report.json";
const markdownName = process.env.PHRASE_SEARCH_QA_MARKDOWN_NAME || "phrase-search-qa-report.md";
const csvName = process.env.PHRASE_SEARCH_QA_CSV_NAME || "phrase-search-qa-report.csv";
const limit = Math.max(4, Number(process.env.PHRASE_SEARCH_QA_LIMIT || "6"));
const timeoutMs = Math.max(1000, Number(process.env.PHRASE_SEARCH_QA_TIMEOUT_MS || "12000"));
const corpusMode = process.env.PHRASE_SEARCH_QA_CORPUS_MODE || "trusted_only";

const TASKS = [
  {
    id: "pipe_noise",
    query: "pipe noise",
    topK: 4,
    top1ExpectedAny: ["pipe", "pipes", "plumbing", "radiator", "heating system", "boiler"],
    expectedAny: ["pipe noise", "pipes below", "banging pipe", "plumbing noise", "boiler noise", "radiator noise", "heating system noise"],
    expectedAllConcepts: [["pipe", "pipes", "plumbing", "boiler", "radiator", "heating system"], ["noise", "noises", "banging", "gurgling", "hissing"]],
    disallowedTopAny: ["pipe replacement cost", "certified capital improvement"],
    maxLexicalMs: 750,
    maxTotalMs: 6000
  },
  {
    id: "mold_kitchen",
    query: "mold in the kitchen",
    topK: 4,
    top1ExpectedAny: ["mold in the kitchen", "mold in kitchen", "kitchen sink", "kitchen cabinet", "under the kitchen sink"],
    expectedAny: ["mold in the kitchen", "kitchen sink", "kitchen cabinet", "under the kitchen sink", "kitchen wall"],
    expectedAllConcepts: [["mold", "mildew"], ["kitchen"]],
    disallowedTopAny: ["molding", "bathroom only"],
    maxLexicalMs: 750,
    maxTotalMs: 6000
  },
  {
    id: "heater_malfunction_winter",
    query: "heater malfunctioning in the winter",
    topK: 4,
    top1ExpectedAny: ["heater was still not working", "heater was not working", "heating system was not working", "boiler malfunctioned", "insufficient heat"],
    expectedAny: ["heater was still not working", "heating system was not working", "heaters apart but did not repair", "insufficient heat", "broken", "not working", "not repair"],
    expectedAllConcepts: [["heat", "heater", "heating", "boiler", "radiator"], ["not working", "broken", "not repair", "failed", "insufficient"], ["winter", "cold", "november", "december", "january", "february", "room temperature"]],
    disallowedTopAny: ["oven", "stove", "range", "water heater"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "leaky_bathroom_window",
    query: "leaky bathroom window",
    topK: 4,
    top1ExpectedAny: ["leaking window", "bathroom window", "windows rattle and leak", "windows leak"],
    expectedAny: ["bathroom window", "windows rattle and leak", "windows leak", "leaking window", "window frame", "window latch"],
    expectedAllConcepts: [["leak", "leaks", "leaking", "leaky", "leakage"], ["bathroom"], ["window", "windows"]],
    disallowedTopAny: ["leaky faucet", "capital improvement", "new windows passthrough"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "bathroom_window_leak",
    query: "bathroom window leak",
    topK: 4,
    top1ExpectedAny: ["leaking window", "bathroom window", "windows rattle and leak", "windows leak"],
    expectedAny: ["bathroom window", "windows rattle and leak", "window frame", "windows leak", "leaking window"],
    expectedAllConcepts: [["bathroom"], ["window", "windows"], ["leak", "leaks", "leaking", "leaky", "leakage"]],
    disallowedTopAny: ["leaky faucet", "capital improvement"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "kitchen_mold",
    query: "kitchen mold",
    topK: 4,
    top1ExpectedAny: ["kitchen mold", "mold in kitchen", "kitchen sink", "kitchen cabinet", "under the kitchen sink"],
    expectedAny: ["kitchen mold", "mold in kitchen", "kitchen sink", "kitchen cabinet", "under the kitchen sink"],
    expectedAllConcepts: [["kitchen"], ["mold", "mildew"]],
    disallowedTopAny: ["molding"],
    maxLexicalMs: 750,
    maxTotalMs: 6000
  },
  {
    id: "radiator_noise",
    query: "radiator noise",
    topK: 4,
    top1ExpectedAny: ["radiator noise"],
    expectedAny: ["radiator noise", "radiators", "unreasonably loud noises", "hissing", "banging noises"],
    expectedAllConcepts: [["radiator", "radiators", "heating system"], ["noise", "noises", "hissing", "banging"]],
    disallowedTopAny: ["pipe replacement cost", "capital improvement"],
    maxLexicalMs: 750,
    maxTotalMs: 6000
  },
  {
    id: "noisy_pipes",
    query: "noisy pipes",
    topK: 4,
    top1ExpectedAny: ["pipe", "pipes", "boiler noise", "radiator noise", "plumbing noise"],
    expectedAny: ["boiler noise", "radiator noise", "plumbing noise", "banging pipe", "pipes"],
    expectedAllConcepts: [["pipe", "pipes", "plumbing", "boiler", "radiator", "heating system"], ["noise", "noisy", "banging", "gurgling", "hissing", "clanging"]],
    disallowedTopAny: ["pipe replacement cost", "capital improvement"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "banging_pipe",
    query: "banging pipe",
    topK: 4,
    top1ExpectedAny: ["banging pipe", "noise from the banging pipe"],
    expectedAny: ["banging pipe", "noise from the banging pipe", "pipes below", "boiler noise"],
    expectedAllConcepts: [["pipe", "pipes", "plumbing", "boiler", "radiator"], ["banging", "noise", "noisy", "clanging"]],
    disallowedTopAny: ["pipe replacement cost", "capital improvement"],
    maxLexicalMs: 750,
    maxTotalMs: 6000
  },
  {
    id: "plumbing_noise",
    query: "plumbing noise",
    topK: 4,
    top1ExpectedAny: ["plumbing noise", "gurgling", "tapping"],
    expectedAny: ["plumbing noise", "gurgling", "tapping", "bathroom plumbing", "sink drain"],
    expectedAllConcepts: [["plumbing", "pipe", "pipes", "drain"], ["noise", "noisy", "gurgling", "tapping", "banging"]],
    disallowedTopAny: ["capital improvement"],
    maxLexicalMs: 750,
    maxTotalMs: 6000
  },
  {
    id: "mold_kitchen_cabinet",
    query: "mold kitchen cabinet",
    topK: 4,
    top1ExpectedAny: ["mold in the kitchen above the cabinet", "kitchen cabinet", "kitchen cabinets"],
    expectedAny: ["mold in the kitchen above the cabinet", "kitchen cabinet", "kitchen cabinets", "mold and mildew"],
    expectedAllConcepts: [["mold", "mildew"], ["kitchen"], ["cabinet", "cabinets"]],
    disallowedTopAny: ["molding"],
    maxLexicalMs: 750,
    maxTotalMs: 7000
  },
  {
    id: "bathroom_mold",
    query: "bathroom mold",
    topK: 4,
    top1ExpectedAny: ["bathroom mold", "mold in the bathroom"],
    expectedAny: ["bathroom mold", "mold in the bathroom", "mold/mildew", "mold mildew"],
    expectedAllConcepts: [["bathroom", "bath"], ["mold", "mildew"]],
    disallowedTopAny: ["molding"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "heater_not_working",
    query: "heater not working",
    topK: 4,
    top1ExpectedAny: ["heater was not working", "heating system was not working", "not repair the central heater"],
    expectedAny: ["heater was not working", "heating system was not working", "not repair the central heater", "nonfunctioning heater"],
    expectedAllConcepts: [["heat", "heater", "heating", "boiler", "radiator"], ["not working", "broken", "not functioning", "failed", "repair"]],
    disallowedTopAny: ["water heater", "oven", "stove", "range"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "insufficient_heat_bedroom",
    query: "insufficient heat in bedroom",
    topK: 4,
    top1ExpectedAny: ["insufficient heat", "bedroom", "heat in the bedrooms", "inadequate"],
    expectedAny: ["insufficient heat", "bedroom", "heat in the bedrooms", "inadequate heat"],
    expectedAllConcepts: [["heat", "heating", "heater", "boiler", "radiator"], ["insufficient", "inadequate", "not working", "cold"], ["bedroom", "room"]],
    disallowedTopAny: ["water heater"],
    maxLexicalMs: 750,
    maxTotalMs: 7000
  },
  {
    id: "kitchen_sink_leak",
    query: "kitchen sink leak",
    topK: 4,
    top1ExpectedAny: ["kitchen sink leak", "leak-free kitchen sink", "leak at kitchen sink"],
    expectedAny: ["kitchen sink leak", "leak-free kitchen sink", "leak at kitchen sink", "kitchen sink drain"],
    expectedAllConcepts: [["kitchen"], ["sink"], ["leak", "leaking", "leaks"]],
    disallowedTopAny: ["capital improvement"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "ceiling_leak_bedroom",
    query: "ceiling leak in bedroom",
    topK: 4,
    top1ExpectedAny: ["ceiling leaks in the bedroom", "bedroom ceiling", "leaking bedroom ceiling", "water leaking from roof", "water leaked into the master bedroom"],
    expectedAny: ["ceiling leaks in the bedroom", "bedroom ceiling", "leaking bedroom ceiling", "ceiling leak"],
    expectedAllConcepts: [["ceiling", "roof"], ["leak", "leaking", "water"], ["bedroom", "room"]],
    disallowedTopAny: ["capital improvement"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "leaking_roof_bedroom",
    query: "leaking roof over bedroom",
    topK: 4,
    top1ExpectedAny: ["roof", "ceiling", "water intrusion", "leaking bedroom"],
    expectedAny: ["roof repairs", "ceiling repair", "leaking bedroom ceiling", "water intrusion", "roof"],
    expectedAllConcepts: [["roof", "ceiling"], ["leak", "leaking", "water intrusion", "water"], ["bedroom", "room"]],
    disallowedTopAny: ["capital improvement"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "electrical_outlets_not_working",
    query: "electrical outlets not working",
    topK: 4,
    top1ExpectedAny: ["non-working electrical outlets", "malfunctioning electrical outlets", "broken electrical outlets", "working electrical outlet"],
    expectedAny: ["non-working electrical outlets", "malfunctioning electrical outlets", "broken electrical outlets", "working electrical outlets"],
    expectedAllConcepts: [["electrical", "outlet", "outlets"], ["not working", "non-working", "malfunctioning", "broken", "working order"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "rotten_floor_boards",
    query: "rotten floor boards",
    topK: 4,
    top1ExpectedAny: ["rotten floor", "rotted floor", "floor boards", "floorboards", "dry rot"],
    expectedAny: ["rotten floor", "rotted floor", "floor boards", "floorboards", "dry rot", "soft floor"],
    expectedAllConcepts: [["rotten", "rotted", "dry rot", "soft", "damaged"], ["floor", "flooring", "floor boards", "floorboards", "boards"]],
    disallowedTopAny: ["capital improvement"],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "broken_front_door_lock",
    query: "broken front door lock",
    topK: 4,
    top1ExpectedAny: ["broken front door lock", "front door lock", "door lock was not broken", "lock on the front door"],
    expectedAny: ["broken front door lock", "front door lock", "entry door", "lock on the front door"],
    expectedAllConcepts: [["front door", "entry door", "door"], ["lock", "locks", "locking"], ["broken", "not working", "repair"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "security_gate_not_locking",
    query: "security gate not locking",
    topK: 4,
    top1ExpectedAny: ["security gate", "front gate locking", "properly locking front gate"],
    expectedAny: ["security gate", "front gate locking", "properly locking front gate", "locked iron gate"],
    expectedAllConcepts: [["security", "gate", "front gate"], ["lock", "locking", "locked"], ["not", "inoperative", "broken", "repair"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "low_water_pressure_kitchen_sink",
    query: "low water pressure kitchen sink",
    topK: 4,
    top1ExpectedAny: ["hot water pressure to the kitchen sink", "low hot water pressure at the kitchen sink", "water pressure for the kitchen sink"],
    expectedAny: ["hot water pressure to the kitchen sink", "low hot water pressure at the kitchen sink", "water pressure for the kitchen sink"],
    expectedAllConcepts: [["water pressure", "pressure"], ["kitchen"], ["sink"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "trash_chute_odor",
    query: "trash chute odor",
    topK: 4,
    top1ExpectedAny: ["trash chute", "garbage chute", "refuse chute", "odor", "smell", "stench"],
    expectedAny: ["trash chute", "garbage chute", "refuse chute", "odor", "smell", "stench", "foul odor", "offensive odor"],
    expectedAllConcepts: [["trash", "garbage", "refuse", "waste"], ["chute", "trash chute", "garbage chute", "refuse chute"], ["odor", "smell", "stench"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "garbage_smell_hallway",
    query: "garbage smell hallway",
    topK: 4,
    top1ExpectedAny: ["garbage smell", "trash smell", "refuse smell", "odor", "hallway", "common area"],
    expectedAny: ["garbage smell", "trash smell", "refuse smell", "odor", "smell", "hallway", "common area", "corridor"],
    expectedAllConcepts: [["garbage", "trash", "refuse", "waste"], ["smell", "odor", "stench"], ["hallway", "hall", "common area", "corridor"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  },
  {
    id: "shower_drain_backing_up",
    query: "shower drain backing up",
    topK: 4,
    top1ExpectedAny: ["shower", "drain", "backing up", "backed up", "sewage backing up", "tub was backing up"],
    expectedAny: ["shower", "drain", "backing up", "backed up", "sewage backing up", "tub was backing up", "bathroom facilities"],
    expectedAllConcepts: [["shower", "bathtub", "tub", "bath"], ["drain", "drains", "sewer", "plumbing"], ["backing up", "backed up", "backup", "overflow", "sewage"]],
    disallowedTopAny: [],
    maxLexicalMs: 1000,
    maxTotalMs: 7000
  }
];

function normalize(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s.-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function passageText(result) {
  return [
    result?.title,
    result?.citation,
    result?.sectionLabel,
    result?.sectionHeading,
    result?.snippet,
    result?.matchedPassage?.snippet,
    result?.primaryAuthorityPassage?.snippet,
    result?.supportingFactPassage?.snippet,
    Array.isArray(result?.retrievalReason) ? result.retrievalReason.join(" ") : ""
  ].join(" ");
}

function evidenceText(result) {
  return [
    result?.snippet,
    result?.matchedPassage?.snippet,
    result?.primaryAuthorityPassage?.snippet,
    result?.supportingFactPassage?.snippet
  ].join(" ");
}

function includesAny(text, values = []) {
  const haystack = normalize(text);
  return values.filter((value) => haystack.includes(normalize(value)));
}

function conceptCoverage(text, groups = []) {
  const haystack = normalize(text);
  return groups.map((group) => group.filter((value) => haystack.includes(normalize(value))));
}

async function fetchDebug(task) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      signal: controller.signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: task.query,
        queryType: "keyword",
        limit,
        snippetMaxLength: 360,
        corpusMode,
        filters: { approvedOnly: true }
      })
    });
    const raw = await response.text();
    const body = JSON.parse(raw);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${JSON.stringify(body).slice(0, 500)}`);
    }
    return body;
  } finally {
    clearTimeout(timer);
  }
}

function evaluateTask(task, response, wallMs) {
  const results = Array.isArray(response?.results) ? response.results : [];
  const topK = results.slice(0, task.topK || 4);
  const topText = topK.map(evidenceText).join(" ");
  const top1Text = evidenceText(topK[0]);
  const expectedHits = includesAny(topText, task.expectedAny);
  const top1ExpectedHits = includesAny(top1Text, task.top1ExpectedAny);
  const disallowedTopHits = includesAny(top1Text, task.disallowedTopAny);
  const coverage = conceptCoverage(topText, task.expectedAllConcepts);
  const missingConceptGroups = coverage
    .map((hits, index) => ({ index, hits }))
    .filter((item) => item.hits.length === 0)
    .map((item) => task.expectedAllConcepts[item.index]);
  const timings = response?.runtimeDiagnostics?.stageTimingsMs || {};
  const lexicalMs = Number(timings.lexicalSearch || 0);
  const totalMs = Number(timings.total || wallMs || 0);
  const failures = [];

  if (results.length === 0) failures.push("no_results");
  if (expectedHits.length === 0) failures.push("missing_expected_phrase_or_signal");
  if (task.top1ExpectedAny?.length && top1ExpectedHits.length === 0) failures.push("top_result_missing_expected_phrase_or_signal");
  if (missingConceptGroups.length > 0) failures.push("missing_required_concept_group");
  if (disallowedTopHits.length > 0) failures.push("top_result_has_drift_signal");
  if (task.maxLexicalMs && lexicalMs > task.maxLexicalMs) failures.push("lexical_search_too_slow");
  if (task.maxTotalMs && totalMs > task.maxTotalMs) failures.push("total_search_too_slow");

  return {
    id: task.id,
    query: task.query,
    passed: failures.length === 0,
    failures,
    totalResults: Number(response?.total || results.length || 0),
    wallMs,
    lexicalMs,
    totalMs,
    expectedHits,
    top1ExpectedHits,
    conceptHits: coverage,
    missingConceptGroups,
    disallowedTopHits,
    topResults: topK.map((result, index) => ({
      rank: index + 1,
      title: result.title || "",
      documentId: result.documentId || "",
      score: Number(result.score || 0),
      sectionLabel: result.sectionLabel || "",
      snippet: String(result.snippet || "").replace(/\s+/g, " ").trim()
    }))
  };
}

function buildMarkdown(report) {
  const lines = [
    "# Phrase Search QA Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Corpus mode: \`${report.corpusMode}\``,
    `- Passed: \`${report.summary.passed}/${report.summary.queryCount}\``,
    `- Failed: \`${report.summary.failed}\``,
    ""
  ];

  for (const row of report.results) {
    lines.push(`## ${row.passed ? "PASS" : "FAIL"} ${row.query}`);
    lines.push("");
    lines.push(`- lexicalMs: \`${row.lexicalMs}\`, totalMs: \`${row.totalMs}\`, wallMs: \`${row.wallMs}\``);
    lines.push(`- failures: ${row.failures.length ? row.failures.map((item) => `\`${item}\``).join(", ") : "`none`"}`);
    lines.push(`- top1ExpectedHits: ${row.top1ExpectedHits.length ? row.top1ExpectedHits.map((item) => `\`${item}\``).join(", ") : "`none`"}`);
    lines.push(`- expectedHits: ${row.expectedHits.length ? row.expectedHits.map((item) => `\`${item}\``).join(", ") : "`none`"}`);
    for (const result of row.topResults) {
      lines.push(`- #${result.rank} ${result.title} | score=\`${result.score.toFixed(3)}\` | ${result.snippet.slice(0, 220)}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function buildCsv(report) {
  const header = ["id", "query", "passed", "failures", "top1_expected_hits", "wall_ms", "lexical_ms", "total_ms", "rank", "title", "score", "snippet"];
  const lines = [header.join(",")];
  for (const row of report.results) {
    for (const result of row.topResults) {
      lines.push(
        [
          row.id,
          row.query,
          row.passed,
          row.failures.join("|"),
          row.top1ExpectedHits.join("|"),
          row.wallMs,
          row.lexicalMs,
          row.totalMs,
          result.rank,
          result.title,
          result.score,
          result.snippet
        ].map(csvEscape).join(",")
      );
    }
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const results = [];
  for (const task of TASKS) {
    const started = Date.now();
    try {
      const response = await fetchDebug(task);
      results.push(evaluateTask(task, response, Date.now() - started));
    } catch (error) {
      results.push({
        id: task.id,
        query: task.query,
        passed: false,
        failures: ["request_failed"],
        error: error instanceof Error ? error.message : String(error),
        totalResults: 0,
        wallMs: Date.now() - started,
        lexicalMs: 0,
        totalMs: Date.now() - started,
        expectedHits: [],
        top1ExpectedHits: [],
        conceptHits: [],
        missingConceptGroups: task.expectedAllConcepts || [],
        disallowedTopHits: [],
        topResults: []
      });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    limit,
    summary: {
      queryCount: results.length,
      passed: results.filter((row) => row.passed).length,
      failed: results.filter((row) => !row.passed).length,
      maxLexicalMs: Math.max(...results.map((row) => row.lexicalMs || 0)),
      maxTotalMs: Math.max(...results.map((row) => row.totalMs || 0))
    },
    results
  };

  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, buildMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Phrase search QA JSON report written to ${jsonPath}`);
  console.log(`Phrase search QA Markdown report written to ${markdownPath}`);
  console.log(`Phrase search QA CSV report written to ${csvPath}`);

  if (report.summary.failed > 0 && process.env.PHRASE_SEARCH_QA_ALLOW_FAILURES !== "1") {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
