import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

function extractKeywordList(src) {
  const m = src.match(/const DECISION_LAYER_SECTION_LABEL_KEYWORDS = \[([\s\S]*?)\];/);
  assert.ok(m, "DECISION_LAYER_SECTION_LABEL_KEYWORDS array must exist");
  return Array.from(m[1].matchAll(/"([^"]+)"/g)).map((entry) => entry[1]);
}

function classifierBody(src, name) {
  const m = src.match(new RegExp(`function ${name}\\(sectionLabel: string\\): boolean \\{([\\s\\S]*?)\\n\\}`));
  assert.ok(m, `${name} must exist`);
  return m[1];
}

test("decision-layer fallback fetches apply the section-label SQL prefilter", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");

  // The clause helper builds a lowercased LIKE superset per keyword.
  assert.match(searchQueryAnalysisSrc, /lower\(\$\{column\}\) LIKE '%\$\{keyword\}%'/);

  // fetchChunksByDocumentIds accepts the opt-in flag and injects the clause into all branches.
  assert.match(src, /decisionLayerSectionsOnly = false/);
  assert.match(
    src,
    /const documentSectionClause = decisionLayerSectionsOnly \? decisionLayerSectionLabelClause\("c\.section_label"\) : "";/
  );
  assert.match(
    src,
    /const retrievalSectionClause = decisionLayerSectionsOnly \? decisionLayerSectionLabelClause\("rs\.section_label"\) : "";/
  );
  assert.match(src, /AND d\.id IN \(\$\{placeholders\}\)\$\{documentSectionClause\}/);
  assert.match(src, /AND d\.id IN \(\$\{placeholders\}\)\$\{retrievalSectionClause\}/);
  // The recursive batch call forwards the flag so large id sets stay filtered.
  assert.match(src, /fetchChunksByDocumentIds\(env, batch, where, params, decisionLayerSectionsOnly\)/);

  // Both decision-layer fallbacks opt in; nothing else should.
  const authFn = src.match(/async function fetchAuthorityChunksByDocumentIds\([\s\S]*?\n\}/)?.[0] || "";
  const supFn =
    src.match(/async function fetchSupportingFactChunksByDocumentIds\([\s\S]*?const supportRows =/)?.[0] || "";
  assert.match(authFn, /fetchChunksByDocumentIds\(env, documentIds, where, params, true\)/);
  assert.match(supFn, /fetchChunksByDocumentIds\(env, documentIds, where, params, true\)/);
});

test("section-label keyword set is a superset of every decision-layer classifier label", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const keywords = extractKeywordList(src);
  assert.ok(keywords.length >= 8, "expected a non-trivial keyword superset");

  // Auto-extract the normalized label forms each classifier accepts so that adding a new
  // category to a classifier without extending the keyword set fails this test (instead of
  // silently dropping rows the JS filter would have kept).
  const classifierForms = [];
  for (const name of [
    "isConclusionsLikeSectionLabel",
    "isFindingsLikeSectionLabel",
    "isSupportingFactSectionLabel"
  ]) {
    const body = classifierBody(src, name);
    for (const m of body.matchAll(/normalized === "([^"]+)"/g)) classifierForms.push(m[1]);
  }

  // The human-readable phrases the classifier regexes match. Pinned below so a regex change
  // forces this list (and the superset proof) to be re-reviewed.
  const regexPhrases = [
    "conclusions of law",
    "findings of fact",
    "summary of the evidence",
    "factual background",
    "background",
    "history",
    "evidence",
    "testimony"
  ];
  assert.ok(
    src.includes("summary\\s+of\\s+the\\s+evidence|factual\\s+background|background|history|evidence|testimony"),
    "supporting-fact classifier regex changed — re-verify the keyword superset and update regexPhrases"
  );

  const allForms = [...new Set([...classifierForms, ...regexPhrases])];
  assert.ok(allForms.length >= 8);

  for (const form of allForms) {
    const lower = form.toLowerCase();
    const covered = keywords.some((keyword) => lower.includes(keyword));
    assert.ok(
      covered,
      `classifier label form "${form}" is not covered by any DECISION_LAYER_SECTION_LABEL_KEYWORDS entry — ` +
        "the SQL prefilter would drop rows the JS classifier keeps"
    );
  }
});
