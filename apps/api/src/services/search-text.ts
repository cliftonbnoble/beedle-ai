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
  "with"
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

export function containsWholeWord(text: string, term: string, precomputed?: { normalizedText?: string }): boolean {
  const normalizedText = precomputed?.normalizedText ?? normalize(text);
  const normalizedTerm = normalize(term);
  if (!normalizedText || !normalizedTerm) return false;
  const regex = new RegExp(`(^|[^a-z0-9])${escapeRegex(normalizedTerm)}([^a-z0-9]|$)`, "i");
  return regex.test(normalizedText);
}
