import type { CSSProperties, ReactNode } from "react";
import { conceptVariantsForToken } from "@beedle/shared";
import { repairDisplayText } from "./text-cleanup";

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const HIGHLIGHT_STOPWORDS = new Set(["a", "an", "and", "are", "as", "at", "by", "for", "from", "in", "into", "is", "of", "on", "or", "the", "to", "with"]);

function tokenizeHighlightQuery(query: string) {
  return String(query || "")
    .split(/[\s,.;:()/"'-]+/)
    .map((term) => term.trim())
    .filter(Boolean);
}

function tokenSurfaceVariants(token: string) {
  const normalized = token.toLowerCase();
  const variants = new Set([normalized]);
  if (normalized.endsWith("y")) variants.add(`${normalized.slice(0, -1)}ies`);
  if (!normalized.endsWith("s")) variants.add(`${normalized}s`);
  return Array.from(variants).filter(Boolean);
}

function highlightConceptVariantsForToken(token: string) {
  const normalized = token.toLowerCase();
  const variants = new Set<string>(tokenSurfaceVariants(normalized));
  for (const value of conceptVariantsForToken(normalized, "highlight")) {
    const item = value.toLowerCase().replace(/\s+/g, " ").trim();
    if (item) variants.add(item);
  }

  return Array.from(variants).filter(Boolean);
}

function meaningfulHighlightTerms(query: string) {
  const rawTerms = tokenizeHighlightQuery(query);
  const hasLongTerm = rawTerms.some((term) => term.length >= 4 && !/^\d+$/.test(term));
  return Array.from(
    new Set(
      rawTerms.filter(
        (term) =>
          term.length >= 3 &&
          !HIGHLIGHT_STOPWORDS.has(term.toLowerCase()) &&
          (term.length >= 4 || !hasLongTerm || /\d/.test(term))
      )
    )
  );
}

function boundedPhrasePattern(terms: string[]) {
  return `(?<![A-Za-z0-9])${terms.map((term) => escapeRegExp(term)).join(`[\\s,.;:()/"'-]+`)}(?![A-Za-z0-9])`;
}

function conceptPhrasePatterns(groups: string[][]) {
  const patterns: string[] = [];
  for (let index = 0; index < groups.length - 1; index += 1) {
    const left = groups[index]?.slice(0, 8) || [];
    const right = groups[index + 1]?.slice(0, 8) || [];
    if (!left.length || !right.length) continue;
    const leftPattern = `(?:${left.map(escapeRegExp).join("|")})`;
    const rightPattern = `(?:${right.map(escapeRegExp).join("|")})`;
    patterns.push(`(?<![A-Za-z0-9])${leftPattern}[\\s,.;:()/"'-]+${rightPattern}(?![A-Za-z0-9])`);
    patterns.push(`(?<![A-Za-z0-9])${rightPattern}[\\s,.;:()/"'-]+${leftPattern}(?![A-Za-z0-9])`);
  }
  return patterns;
}

function buildHighlightPattern(query: string) {
  const rawTerms = tokenizeHighlightQuery(query);
  const meaningfulTerms = meaningfulHighlightTerms(query);
  const conceptGroups = meaningfulTerms.map(highlightConceptVariantsForToken).filter((group) => group.length > 0);
  const conceptTerms = Array.from(new Set(conceptGroups.flat()))
    .filter((term) => term.length >= 3 && !HIGHLIGHT_STOPWORDS.has(term))
    .sort((a, b) => b.length - a.length);
  const patterns: string[] = [];

  if (rawTerms.length >= 2) patterns.push(boundedPhrasePattern(rawTerms));
  patterns.push(...conceptPhrasePatterns(conceptGroups));
  for (const term of conceptTerms) {
    patterns.push(`(?<![A-Za-z0-9])${escapeRegExp(term)}(?![A-Za-z0-9])`);
  }

  return patterns.length > 0 ? new RegExp(patterns.join("|"), "gi") : null;
}

export function renderHighlightedSearchText(text: string, query: string, options: { markStyle?: CSSProperties } = {}): ReactNode {
  const cleanedText = repairDisplayText(text, query);
  const pattern = buildHighlightPattern(query);
  if (!cleanedText || !pattern) return cleanedText;

  const parts: ReactNode[] = [];
  let lastIndex = 0;
  for (const match of cleanedText.matchAll(pattern)) {
    const index = match.index ?? -1;
    const matchedText = match[0] || "";
    if (index < 0 || !matchedText) continue;
    if (index > lastIndex) parts.push(<span key={`text-${lastIndex}`}>{cleanedText.slice(lastIndex, index)}</span>);
    parts.push(
      <mark key={`mark-${index}`} style={options.markStyle}>
        {matchedText}
      </mark>
    );
    lastIndex = index + matchedText.length;
  }
  if (lastIndex < cleanedText.length) parts.push(<span key={`text-${lastIndex}`}>{cleanedText.slice(lastIndex)}</span>);
  return parts.length > 0 ? parts : cleanedText;
}
