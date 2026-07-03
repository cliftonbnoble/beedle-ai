// Pure text primitives extracted from search.ts (SEARCH-02c module split, step 2).
//
// These are the foundational, dependency-free string helpers the rest of the search service is built on:
// Unicode/whitespace normalization, tokenization, stopword filtering, FTS quoting, regex escaping, and
// whole-word matching. They depend only on each other (never on the rest of search.ts), which is why they
// are a safe leaf extraction. Relocating them here is behavior-neutral — the golden ranking net stays
// byte-identical.

export function normalize(input: string): string {
  return input
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeWhitespace(value: string): string {
  return String(value || "").replace(/\s+/g, " ").trim();
}

export function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function uniq<T>(values: T[]): T[] {
  return Array.from(new Set(values));
}

export function tokenize(input: string): string[] {
  return normalize(input)
    .split(/[^a-z0-9_:-]+/)
    .map((item) => item.trim())
    .filter((item) => item.length > 1);
}

// Quoted-phrase intent (NS-03): legal users quote a phrase to mean exact match, but tokenize/normalize
// strip quotes before any stage sees them. When the ENTIRE query is one balanced double-quoted span
// (straight or curly quotes) containing at least two tokens, return the inner phrase so the caller can
// route it down the exact_phrase path. Anything else — mixed quoted/unquoted text, unbalanced or
// nested quotes, single-token spans — returns "" and keeps today's keyword behavior.
export function wholeQueryQuotedPhrase(input: string): string {
  const trimmed = String(input || "")
    .replace(/[“”„‟]/g, '"')
    .trim();
  if (trimmed.length < 4 || !trimmed.startsWith('"') || !trimmed.endsWith('"')) return "";
  const inner = trimmed.slice(1, -1).trim();
  if (!inner || inner.includes('"')) return "";
  if (tokenize(inner).length < 2) return "";
  return inner;
}

export const STOPWORD_TOKENS = new Set([
  "a",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "but",
  "by",
  "for",
  "from",
  "if",
  "in",
  "into",
  "is",
  "it",
  "no",
  "not",
  "of",
  "on",
  "or",
  "such",
  "that",
  "the",
  "their",
  "then",
  "there",
  "these",
  "they",
  "this",
  "to",
  "was",
  "will",
  "with",
  // NS-07: natural-language question scaffolding. Interrogatives, modals, auxiliaries, and pronouns
  // survived the original list, so "can my landlord raise rent twice in one year" spent term slots on
  // "can"/"my"/"about" and overflowed the 6-token phrase-engine ceiling (NS-04). None of these appear
  // in any golden query (verified against the fixture before adding).
  "about",
  "am",
  "been",
  "being",
  "can",
  "could",
  "did",
  "do",
  "does",
  "done",
  "had",
  "has",
  "have",
  "having",
  "her",
  "him",
  "his",
  "how",
  "its",
  "may",
  "might",
  "must",
  "my",
  "our",
  "shall",
  "she",
  "should",
  "what",
  "when",
  "where",
  "which",
  "who",
  "whom",
  "whose",
  "why",
  "without",
  "would",
  "your"
]);

export function meaningfulLexicalTokens(query: string): string[] {
  const tokens = uniq(tokenize(query)).filter((token) => token.length >= 2 && !STOPWORD_TOKENS.has(token));
  const hasLongToken = tokens.some((token) => token.length >= 4 && !/^\d+$/.test(token));
  return tokens
    .filter((token) => token.length >= 4 || !hasLongToken || /\d/.test(token))
    .slice(0, 8);
}

export function ftsQuote(value: string): string {
  const normalized = normalizeWhitespace(normalize(value || "").replace(/[^a-z0-9\s]/g, " "));
  return normalized ? `"${normalized.replace(/"/g, "\"\"")}"` : "";
}

// Word-boundary regexes are built from a small vocabulary of query-derived terms but were previously
// compiled fresh on EVERY call — and containsWholeWord runs per row × per term in the scoring hot loop
// (34 call sites). The patterns are pure functions of the term, so they are memoized per isolate. The
// cap guards against unbounded growth across many distinct queries; clearing resets cheaply.
const WHOLE_WORD_REGEX_CACHE_MAX = 5000;

const wholeWordRegexCache = new Map<string, RegExp>();
export function wholeWordRegex(term: string): RegExp {
  let regex = wholeWordRegexCache.get(term);
  if (!regex) {
    if (wholeWordRegexCache.size >= WHOLE_WORD_REGEX_CACHE_MAX) wholeWordRegexCache.clear();
    regex = new RegExp(`(^|[^a-z0-9])${escapeRegex(term)}([^a-z0-9]|$)`, "i");
    wholeWordRegexCache.set(term, regex);
  }
  return regex;
}

// Global-flag sibling for occurrence counting. A shared /g/ regex is only safe with
// String.prototype.match (which resets lastIndex itself) — never call .test/.exec on these.
const wholeWordCountRegexCache = new Map<string, RegExp>();
export function wholeWordCountRegex(term: string): RegExp {
  let regex = wholeWordCountRegexCache.get(term);
  if (!regex) {
    if (wholeWordCountRegexCache.size >= WHOLE_WORD_REGEX_CACHE_MAX) wholeWordCountRegexCache.clear();
    regex = new RegExp(`(^|[^a-z0-9])${escapeRegex(term)}([^a-z0-9]|$)`, "gi");
    wholeWordCountRegexCache.set(term, regex);
  }
  return regex;
}

export function containsWholeWord(text: string, term: string, precomputed?: { normalizedText?: string }): boolean {
  const normalizedText = precomputed?.normalizedText ?? normalize(text);
  const normalizedTerm = normalize(term);
  if (!normalizedText || !normalizedTerm) return false;
  return wholeWordRegex(normalizedTerm).test(normalizedText);
}
