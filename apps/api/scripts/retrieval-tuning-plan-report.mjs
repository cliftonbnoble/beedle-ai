import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const goldenName = process.env.RETRIEVAL_TUNING_GOLDEN_NAME || "retrieval-golden-query-eval-report.json";
const issueName = process.env.RETRIEVAL_TUNING_ISSUE_NAME || "retrieval-issue-quality-audit-report.json";
const healthName = process.env.RETRIEVAL_TUNING_HEALTH_NAME || "retrieval-health-report.json";
const jsonName = process.env.RETRIEVAL_TUNING_PLAN_JSON_NAME || "retrieval-tuning-plan-report.json";
const markdownName = process.env.RETRIEVAL_TUNING_PLAN_MARKDOWN_NAME || "retrieval-tuning-plan-report.md";

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(4));
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function topWords(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function analyzeQuery(goldenQuery, issueQuery) {
  const topResults = Array.isArray(goldenQuery?.topResults) ? goldenQuery.topResults : [];
  const snippets = topResults.map((row) => String(row.snippet || ""));
  const titles = topResults.map((row) => String(row.title || ""));
  const chunkTypes = unique(topResults.map((row) => row.chunkType || "<none>"));
  const vectorScores = topResults.map((row) => Number(row.vectorScore || 0));
  const lexicalScores = topResults.map((row) => Number(row.lexicalScore || 0));
  const combinedText = [...snippets, ...titles].join(" ").toLowerCase();

  const signals = {
    queryId: goldenQuery.id,
    family: goldenQuery.family,
    query: goldenQuery.query,
    uniqueDecisionCount: Number(goldenQuery.uniqueDecisionCount || 0),
    totalResults: Number(goldenQuery.totalResults || 0),
    chunkTypeConcentration: chunkTypes.length === 1 ? chunkTypes[0] : null,
    avgVectorScoreTop3: avg(vectorScores.slice(0, 3)),
    avgLexicalScoreTop3: avg(lexicalScores.slice(0, 3)),
    vectorMatchCount: Number(issueQuery?.vectorMatchCount || 0),
    collisionTerms: [],
    notes: []
  };

  if (signals.chunkTypeConcentration) {
    signals.notes.push(`Top results are concentrated in \`${signals.chunkTypeConcentration}\` chunks.`);
  }
  if (signals.avgVectorScoreTop3 < 0.1 && signals.avgLexicalScoreTop3 > 0.8) {
    signals.notes.push("Top results are mostly lexical rather than semantic.");
  }
  if (signals.uniqueDecisionCount < 5) {
    signals.notes.push("Decision diversity is narrower than expected.");
  }

  if (goldenQuery.id === "mold" && /\bmolding\b/.test(combinedText)) {
    signals.collisionTerms.push("molding");
    signals.notes.push("Lower-quality lexical collision with `molding` appears in top results.");
  }

  if (["ventilation", "cooling"].includes(goldenQuery.id)) {
    const thermalProxyTerms = ["fan", "hvac", "air conditioner", "air-conditioning", "capital improvement", "passthrough"];
    const found = thermalProxyTerms.filter((term) => combinedText.includes(term));
    if (found.length) {
      signals.collisionTerms.push(...found);
      signals.notes.push(`Top results lean toward equipment/passthrough proxy terms: ${unique(found).join(", ")}.`);
    }
  }

  if (goldenQuery.id === "owner_move_in" && !/owner move-in|omi|relative move-in/.test(combinedText)) {
    signals.notes.push("Top snippets do not strongly echo owner move-in terminology.");
  }

  if (goldenQuery.id === "repair_notice" && !/notice|repair|request/.test(combinedText)) {
    signals.notes.push("Top snippets do not strongly surface procedural notice language.");
  }

  return signals;
}

function buildPlan(golden, issue, health) {
  const goldenQueries = Array.isArray(golden?.queries) ? golden.queries : [];
  const issueQueries = new Map((Array.isArray(issue?.queries) ? issue.queries : []).map((row) => [row.id, row]));
  const analyzed = goldenQueries.map((query) => analyzeQuery(query, issueQueries.get(query.id)));

  const lexicalCollisionQueries = analyzed.filter((row) => row.collisionTerms.length > 0);
  const lexicalDominantQueries = analyzed.filter((row) => row.avgVectorScoreTop3 < 0.1 && row.avgLexicalScoreTop3 > 0.8);
  const monoChunkQueries = analyzed.filter((row) => row.chunkTypeConcentration);
  const weakPrecisionQueries = analyzed.filter((row) => ["ventilation", "cooling", "owner_move_in"].includes(row.queryId));

  const planItems = [
    {
      priority: 1,
      title: "Suppress obvious lexical collisions before scoring ties are accepted",
      why: lexicalCollisionQueries.length
        ? `Current evidence shows direct collisions in top results for: ${lexicalCollisionQueries.map((row) => row.query).join(", ")}.`
        : "No major lexical collisions were detected in the current sample.",
      actions: [
        "Add a query-normalization pass for high-risk tokens like `mold` so `molding` can be demoted or excluded when semantic evidence is weak.",
        "Add a rank-time penalty when the only match is a substring collision and the vector score is near zero.",
        "Re-run the golden query review after each collision rule so we do not over-prune legitimate hits."
      ],
      affectedQueries: lexicalCollisionQueries.map((row) => ({
        query: row.query,
        collisionTerms: unique(row.collisionTerms)
      }))
    },
    {
      priority: 2,
      title: "Improve semantic intent handling for housing-condition proxy queries",
      why: weakPrecisionQueries.length
        ? "Cooling and ventilation still look vulnerable to proxy-term matches like fan, HVAC, or passthrough language."
        : "Proxy-term drift was not detected strongly enough to prioritize.",
      actions: [
        "Expand query rewriting for `ventilation` and `cooling` to include habitability-oriented variants like `air flow`, `overheating`, `heat`, and `temperature control`.",
        "Add a down-weight for passthrough/capital-improvement style hits when the query family is `housing_conditions`.",
        "Prefer results that also mention tenant conditions, service reduction, heat, or habitability in the snippet."
      ],
      affectedQueries: weakPrecisionQueries.map((row) => ({
        query: row.query,
        notes: row.notes
      }))
    },
    {
      priority: 3,
      title: "Reduce single-section dominance in top results",
      why: monoChunkQueries.length
        ? `${monoChunkQueries.length} queries currently surface only one chunk type in the top results, usually findings-of-fact.`
        : "Chunk-type diversity already looks balanced enough.",
      actions: [
        "Add a small diversity preference so conclusions/orders can enter the top ranks when relevance is comparable.",
        "Tune section weighting per family instead of globally; eviction and notice queries often benefit from procedural/conclusion sections.",
        "Keep findings-of-fact strong, but stop them from crowding out clearer legal holdings in close-score cases."
      ],
      affectedQueries: monoChunkQueries.slice(0, 8).map((row) => ({
        query: row.query,
        chunkType: row.chunkTypeConcentration
      }))
    },
    {
      priority: 4,
      title: "Tune lexical/vector balance only where the report says we need it",
      why: lexicalDominantQueries.length
        ? `${lexicalDominantQueries.length} queries are currently top-heavy on lexical scoring.`
        : "Current top results are not overly lexical.",
      actions: [
        "Start with family-specific weighting rather than a global rebalance.",
        "Use the golden-query CSV labels as the guardrail before changing score mixing.",
        "If a query is already good, leave it alone."
      ],
      affectedQueries: lexicalDominantQueries.map((row) => ({
        query: row.query,
        avgVectorScoreTop3: row.avgVectorScoreTop3,
        avgLexicalScoreTop3: row.avgLexicalScoreTop3
      }))
    }
  ];

  return {
    generatedAt: new Date().toISOString(),
    inputs: {
      goldenReport: path.resolve(reportsDir, goldenName),
      issueReport: path.resolve(reportsDir, issueName),
      healthReport: path.resolve(reportsDir, healthName)
    },
    currentState: {
      goldenSummary: golden?.summary || null,
      issueSummary: issue?.summary || null,
      healthSummary: health?.summary || null
    },
    diagnostics: analyzed,
    planItems
  };
}

function formatMarkdown(report) {
  const lines = [
    "# Retrieval Tuning Plan",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Golden queries returned: \`${report.currentState.goldenSummary?.returnedQueryCount ?? "<unknown>"}\` / \`${report.currentState.goldenSummary?.queryCount ?? "<unknown>"}\``,
    `- Issue-quality hit rate: \`${report.currentState.issueSummary?.overallHitRate ?? "<unknown>"}\``,
    `- Retrieval-health hit rate: \`${report.currentState.healthSummary?.overallHitRate ?? "<unknown>"}\``,
    "",
    "## Priority Order",
    ""
  ];

  for (const item of report.planItems) {
    lines.push(`### P${item.priority}: ${item.title}`);
    lines.push("");
    lines.push(`- why: ${item.why}`);
    lines.push("- actions:");
    for (const action of item.actions) {
      lines.push(`  - ${action}`);
    }
    if (item.affectedQueries?.length) {
      lines.push("- affected queries:");
      for (const query of item.affectedQueries) {
        lines.push(`  - \`${query.query}\`${query.chunkType ? ` | chunkType=${query.chunkType}` : ""}${query.collisionTerms ? ` | collisions=${query.collisionTerms.join(", ")}` : ""}`);
      }
    }
    lines.push("");
  }

  lines.push("## Query Diagnostics");
  lines.push("");
  for (const row of report.diagnostics) {
    lines.push(`- \`${row.query}\` | family=\`${row.family}\` | uniqueDecisionCount=\`${row.uniqueDecisionCount}\` | avgVectorTop3=\`${row.avgVectorScoreTop3}\` | avgLexicalTop3=\`${row.avgLexicalScoreTop3}\``);
    for (const note of row.notes) {
      lines.push(`  - ${note}`);
    }
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [golden, issue, health] = await Promise.all([
    fs.readFile(path.resolve(reportsDir, goldenName), "utf8").then(JSON.parse),
    fs.readFile(path.resolve(reportsDir, issueName), "utf8").then(JSON.parse),
    fs.readFile(path.resolve(reportsDir, healthName), "utf8").then(JSON.parse)
  ]);

  const report = buildPlan(golden, issue, health);
  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify({
    diagnosticsCount: report.diagnostics.length,
    planItemCount: report.planItems.length
  }, null, 2));
  console.log(`Retrieval tuning-plan JSON report written to ${jsonPath}`);
  console.log(`Retrieval tuning-plan Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
