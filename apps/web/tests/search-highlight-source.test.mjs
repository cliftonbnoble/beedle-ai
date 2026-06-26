import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchPagePath = path.resolve(process.cwd(), "src/app/search/page.tsx");
const decisionDetailPath = path.resolve(process.cwd(), "src/app/search/decision/[documentId]/decision-detail-client.tsx");
const highlightingPath = path.resolve(process.cwd(), "src/app/search/highlighting.tsx");
const globalsPath = path.resolve(process.cwd(), "src/app/globals.css");

test("shared search highlighter prefers phrases and concept evidence without substring matches", async () => {
  const src = await fs.readFile(highlightingPath, "utf8");

  assert.match(src, /const HIGHLIGHT_STOPWORDS = new Set\(/);
  assert.match(src, /!HIGHLIGHT_STOPWORDS\.has\(term\.toLowerCase\(\)\)/);
  assert.match(src, /function highlightConceptVariantsForToken\(token: string\)/);
  assert.match(src, /add\("pipe", "pipes", "plumbing", "boiler", "radiator", "radiators", "steam heat", "heating system"\)/);
  assert.match(src, /add\("noise", "noises", "noisy", "humming", "banging", "clanging", "gurgling", "hissing", "tapping"\)/);
  assert.match(src, /add\("leak", "leaks", "leaky", "leaking", "leakage", "water intrusion", "water damage", "water"\)/);
  assert.match(src, /add\("roof", "roofs", "ceiling", "ceilings", "exterior wall", "water intrusion"\)/);
  assert.match(src, /add\("outlet", "outlets", "electrical outlet", "electrical outlets", "working electrical outlet", "working electrical outlets"\)/);
  assert.match(src, /add\("rotten", "rotted", "rot", "dry rot", "soft", "damaged", "deteriorated"\)/);
  assert.match(src, /function conceptPhrasePatterns\(groups: string\[\]\[\]\)/);
  assert.match(src, /\(\?<!\[A-Za-z0-9\]\)/);
  assert.match(src, /\(\?!\[A-Za-z0-9\]\)/);
  assert.match(src, /new RegExp\(patterns\.join\("\|"\), "gi"\)/);

  const phrasePatternIndex = src.indexOf("if (rawTerms.length >= 2) patterns.push(boundedPhrasePattern(rawTerms));");
  const conceptPatternIndex = src.indexOf("patterns.push(...conceptPhrasePatterns(conceptGroups));");
  const termPatternIndex = src.indexOf("for (const term of conceptTerms)");
  assert.ok(phrasePatternIndex > -1, "Expected phrase highlight pattern to be built from raw query terms");
  assert.ok(conceptPatternIndex > -1, "Expected concept phrase pattern fallback");
  assert.ok(termPatternIndex > -1, "Expected individual concept term fallback");
  assert.ok(phrasePatternIndex < conceptPatternIndex, "Literal phrase highlights should be registered first");
  assert.ok(conceptPatternIndex < termPatternIndex, "Concept phrase highlights should be registered before individual terms");
});

test("search result snippets and decision reader share concept-aware highlighting", async () => {
  const searchSrc = await fs.readFile(searchPagePath, "utf8");
  const decisionSrc = await fs.readFile(decisionDetailPath, "utf8");

  assert.match(searchSrc, /import \{ renderHighlightedSearchText \} from "\.\/highlighting"/);
  assert.match(searchSrc, /renderHighlightedSearchText\(editorialPreview\.previewText, query,/);
  assert.match(searchSrc, /renderHighlightedSearchText\(result\.primaryAuthorityPassage\?\.snippet \|\| result\.matchedPassage\?\.snippet \|\| result\.snippet, query,/);
  assert.match(decisionSrc, /import \{ renderHighlightedSearchText \} from "\.\.\/\.\.\/highlighting"/);
  assert.match(decisionSrc, /renderHighlightedSearchText\(paragraph\.text, query,/);
});

test("decision reader highlights phrase evidence without noisy substring matches", async () => {
  const src = await fs.readFile(highlightingPath, "utf8");

  assert.match(src, /function meaningfulHighlightTerms\(query: string\)/);
  assert.match(src, /\(\?<!\[A-Za-z0-9\]\)/);
  assert.match(src, /\(\?!\[A-Za-z0-9\]\)/);
  assert.match(src, /\.sort\(\(a, b\) => b\.length - a\.length\)/);
  assert.doesNotMatch(src, /\bindexOf\(/, "Highlighter should not rely on substring matching for evidence marks");
});

test("full decision reader uses reader-friendly Arial typography", async () => {
  const css = await fs.readFile(globalsPath, "utf8");

  assert.match(css, /\.decision-reader__paragraph\s*\{[\s\S]*font-family: Arial, Helvetica, sans-serif;/);
  assert.match(css, /\.decision-reader__paragraph\s*\{[\s\S]*line-height: 1\.78;/);
  assert.match(css, /\.decision-reader__paragraph\s*\{[\s\S]*max-width: 78ch;/);
  assert.match(css, /\.decision-reader__paragraph\s*\{[\s\S]*text-rendering: optimizeLegibility;/);
});
