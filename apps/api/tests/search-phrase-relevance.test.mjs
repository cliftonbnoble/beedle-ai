import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");
const sharedConceptsPath = path.resolve(process.cwd(), "../../packages/shared/src/search-concepts.ts");
const searchFtsMigrationPath = path.resolve(process.cwd(), "migrations/0008_search_fts.sql");

test("phrase searches use concept coverage instead of isolated substring matches", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");
  const concepts = await fs.readFile(sharedConceptsPath, "utf8");

  assert.match(src, /function phraseConceptVariantsForToken\(token: string\): string\[]/);
  assert.match(src, /conceptVariantsForToken\(normalized, "search"\)/);
  assert.match(concepts, /pattern: \/\^pipes\?\$\//);
  assert.match(concepts, /pattern: \/\^nois\(\?:e\|es\|y\)\$\//);
  assert.match(concepts, /pattern: \/\^roofs\?\$\//);
  assert.match(concepts, /pattern: \/\^ceilings\?\$\//);
  assert.ok(concepts.includes("pattern: /^electrical$|^electric$/"));
  assert.ok(concepts.includes("pattern: /^outlets?$/"));
  assert.ok(concepts.includes("pattern: /^working$/"));
  assert.ok(concepts.includes("pattern: /^rotten$|^rotted$/"));
  assert.ok(concepts.includes("pattern: /^floors?$|^flooring$|^boards?$/"));
  assert.ok(concepts.includes("pattern: /^trash$|^garbage$|^rubbish$|^refuse$/"));
  assert.ok(concepts.includes("pattern: /^odou?rs?$|^smells?$|^smelly$|^stench$/"));
  assert.ok(concepts.includes("pattern: /^drains?$|^drainage$/"));
  assert.ok(concepts.includes("pattern: /^back(?:ing|ed)?$|^backup$|^backups$|^overflow(?:ed|ing)?$/"));
  assert.ok(concepts.includes("pattern: /^hallways?$|^halls?$|^corridors?$/"));
  assert.match(src, /function phraseConceptCoverage\(query: string, text: string\):/);
  assert.match(src, /function wholePhraseIndexInNormalizedText\(normalizedText: string, normalizedTerm: string\): number/);
  assert.match(src, /wholePhraseIndexInNormalizedText\(normalizedText, normalizedVariant\)/);
  assert.doesNotMatch(
    src,
    /function phraseConceptCoverage\(query: string, text: string\):[\s\S]*normalizedText\.indexOf\(normalizedVariant\)/,
    "Phrase concept coverage should not count substrings inside larger words"
  );
  assert.match(src, /function phraseConceptGuardPasses\(row: ChunkRow, query: string\): boolean/);
  assert.match(src, /if \(!phraseConceptGuardPasses\(row, query\)\) return false/);
  assert.match(src, /phrase_concept_undercoverage_penalty/);
  assert.match(src, /multiword_phrase_match_boost/);
  assert.match(src, /wholePhraseIndexInNormalizedText\(normalizedText, normalizedPhrase\) >= 0\) return 0\.68/);
});

test("phrase execution terms keep SQL scans focused while ranking handles concept expansion", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /function keywordExecutionTerms\(query: string\): string\[] \{[\s\S]*phraseConceptGroups\(query\)\.length >= 2/);
  assert.match(src, /const tokens = meaningfulLexicalTokens\(query\)\.slice\(0, 4\)/);
  assert.match(src, /return uniq\(\[normalized, \.\.\.tokens\]\.filter\(Boolean\)\)\.slice\(0, 5\)/);
  assert.doesNotMatch(src, /phrase_concept_scope_fetch/, "Known-slow broad phrase pre-scope should not be in the runtime path");
});

test("phrase searches use FTS before falling back to broad LIKE scans", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");
  const migration = await fs.readFile(searchFtsMigrationPath, "utf8");

  assert.match(migration, /CREATE VIRTUAL TABLE IF NOT EXISTS search_chunks_fts USING fts5/);
  assert.match(migration, /CREATE TRIGGER IF NOT EXISTS document_chunks_ai_search_fts/);
  assert.match(migration, /CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_ai_search_fts/);
  assert.match(src, /async function ensureSearchFts\(env: Env\): Promise<boolean>/);
  assert.match(src, /function phraseSearchFtsQuery\(query: string\): string/);
  assert.match(src, /async function ftsSearch\(/);
  assert.match(src, /const phraseFtsCandidateSearch =[\s\S]*isPhraseEvidenceQuery\(effectiveQuery\)[\s\S]*!activeStructuredFilterKinds\(parsed\.filters\)\.length/);
  assert.match(src, /: phraseFtsCandidateSearch\s*\?\s*\[\]/);
  assert.match(src, /phraseFtsEligible[\s\S]*await ftsSearch/);
  assert.match(src, /const phraseFtsHasEnoughEvidence =[\s\S]*phraseFtsEligible[\s\S]*lexicalRows\.length >= Math\.min\(Math\.max\(parsed\.limit, 8\), 18\)/);
  assert.match(src, /shouldSkipVectorSearch\(effectiveQuery, parsed\.filters, queryType\) \|\| phraseFtsHasEnoughEvidence/);
  assert.match(src, /if \(!skipLexicalForVectorFirstIssueSearch && lexicalRows\.length === 0\)[\s\S]*await lexicalSearch/);
});

test("phrase snippets prefer phrase evidence and avoid common drift cases", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /const authorityPhraseCoverage = authoritySnippet \? phraseConceptCoverage/);
  assert.match(src, /const factPhraseCoverage = factSnippet \? phraseConceptCoverage/);
  assert.match(src, /factPhraseCoverage\.matchedCount > authorityPhraseCoverage\.matchedCount/);
  assert.match(src, /function hasWaterHeaterDrift\(query: string, text: string\): boolean/);
  assert.match(src, /room_heat_water_heater_drift_penalty/);
  assert.match(src, /function hasCapitalImprovementCostDrift\(query: string, text: string\): boolean/);
  assert.match(src, /phrase_capital_improvement_cost_drift_penalty/);
  assert.match(src, /function hasLeakWindowContext\(text: string\): boolean/);
  assert.match(src, /function leakWindowContextAdjustment\(query: string, text: string\): \{ score: number; reason: string \| null \}/);
  assert.match(src, /leak_window_split_evidence_penalty/);
  assert.match(src, /leak_window_bathroom_context_boost/);
  assert.match(src, /leak_window_missing_bathroom_penalty/);
  assert.match(src, /Math\.max\(recallConfig\.lexicalSearchLimit, 360\)/);
  assert.match(src, /function hasConcretePhraseFactSignal\(text: string\): boolean/);
  assert.match(src, /function isGenericHousingServiceStandard\(text: string\): boolean/);
  assert.match(src, /phrase_exact_fact_evidence_boost/);
  assert.match(src, /document_phrase_exact_fact_boost/);
  assert.match(src, /decision_layer_exact_phrase_evidence_boost/);
  assert.match(src, /isPhraseEvidenceQuery\(effectiveQuery\)[\s\S]*Math\.min\(0\.04, Math\.max\(0, docHitCount - 1\) \* 0\.01\)/);
  assert.match(src, /isPhraseEvidenceQuery\(context\.query\)[\s\S]*Math\.min\(0\.04, Math\.max\(0, docHitCount - 1\) \* 0\.01\)/);
  assert.match(src, /phrase_generic_legal_standard_penalty/);
});

test("search scoring uses per-search derived query context in hot row scoring", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /interface QueryDerivedContext/);
  assert.match(src, /function buildQueryDerivedContext\(context: SearchContext\): QueryDerivedContext/);
  assert.match(src, /context\.derived = buildQueryDerivedContext\(context\)/);
  assert.match(src, /const queryDerived = context\.derived \?\? buildQueryDerivedContext\(context\)/);
  assert.match(src, /const issueTerms = queryDerived\.issueTerms/);
  assert.match(src, /const referencedJudges = queryDerived\.referencedJudges/);
  assert.match(src, /queryDerived\.phraseEvidenceQuery/);
});
