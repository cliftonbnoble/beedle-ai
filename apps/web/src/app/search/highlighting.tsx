import type { CSSProperties, ReactNode } from "react";
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
  const add = (...values: string[]) => {
    for (const value of values) {
      const item = value.toLowerCase().replace(/\s+/g, " ").trim();
      if (item) variants.add(item);
    }
  };

  if (/^pipes?$/.test(normalized)) add("pipe", "pipes", "plumbing", "boiler", "radiator", "radiators", "steam heat", "heating system");
  if (/^nois(?:e|es|y)$/.test(normalized)) add("noise", "noises", "noisy", "humming", "banging", "clanging", "gurgling", "hissing", "tapping");
  if (/^(heat|heater|heaters|heating)$/.test(normalized)) add("heat", "heater", "heaters", "heating", "boiler", "radiator", "radiators", "steam heat", "heating system");
  if (/^boilers?$/.test(normalized)) add("boiler", "boilers", "heat", "heating", "heating system", "steam heat");
  if (/^radiators?$/.test(normalized)) add("radiator", "radiators", "heat", "heating", "heating system", "steam heat");
  if (/^malfunction(?:ing|ed)?$/.test(normalized)) add("malfunction", "malfunctioning", "malfunctioned", "broken", "not working", "not functioning", "failed", "failure", "repair");
  if (normalized === "winter") add("winter", "cold", "cold weather", "minimum room temperature", "room temperature");
  if (normalized === "mold") add("mold", "mildew");
  if (normalized === "mildew") add("mildew", "mold");
  if (/^leak(?:s|y|ing|age)?$/.test(normalized)) add("leak", "leaks", "leaky", "leaking", "leakage", "water intrusion", "water damage", "water");
  if (/^roofs?$/.test(normalized)) add("roof", "roofs", "ceiling", "ceilings", "exterior wall", "water intrusion");
  if (/^ceilings?$/.test(normalized)) add("ceiling", "ceilings", "roof", "roofs", "overhead", "water intrusion");
  if (/^bedrooms?$/.test(normalized)) add("bedroom", "bedrooms", "room", "rooms");
  if (/^locks?$|^locking$|^locked$/.test(normalized)) add("lock", "locks", "locking", "locked", "latch", "deadbolt");
  if (/^doors?$/.test(normalized)) add("door", "doors", "front door", "entry door");
  if (/^electrical$|^electric$/.test(normalized)) add("electrical", "electric", "outlet", "outlets", "wiring");
  if (/^outlets?$/.test(normalized)) add("outlet", "outlets", "electrical outlet", "electrical outlets", "working electrical outlet", "working electrical outlets");
  if (/^working$/.test(normalized)) add("working", "not working", "non working", "non-working", "not functioning", "properly functioning", "good working order");
  if (/^broken$/.test(normalized)) add("broken", "not working", "non working", "non-working", "not functioning", "malfunctioning", "repair", "replace");
  if (/^rotten$|^rotted$/.test(normalized)) add("rotten", "rotted", "rot", "dry rot", "soft", "damaged", "deteriorated");
  if (/^floors?$|^flooring$|^boards?$/.test(normalized)) add("floor", "floors", "flooring", "floor boards", "floorboards", "boards");
  if (/^trash$|^garbage$|^rubbish$|^refuse$/.test(normalized)) add("trash", "garbage", "rubbish", "refuse", "waste", "debris");
  if (/^chutes?$/.test(normalized)) add("chute", "chutes", "trash chute", "garbage chute", "refuse chute");
  if (/^odou?rs?$|^smells?$|^smelly$|^stench$/.test(normalized)) add("odor", "odors", "odour", "odours", "smell", "smells", "smelly", "stench", "foul odor", "offensive odor", "noxious odor");
  if (/^sewers?$|^sewage$/.test(normalized)) add("sewer", "sewers", "sewage", "waste line", "waste pipe", "plumbing");
  if (/^drains?$|^drainage$/.test(normalized)) add("drain", "drains", "drainage", "plumbing", "sewer", "waste line", "waste pipe");
  if (/^clogg(?:ed|ing)?$|^clogs?$|^blocked$|^blockage$/.test(normalized)) add("clog", "clogs", "clogged", "clogging", "blocked", "blockage", "stoppage", "obstructed");
  if (/^back(?:ing|ed)?$|^backup$|^backups$|^overflow(?:ed|ing)?$/.test(normalized)) add("backing", "backing up", "backed up", "backup", "backups", "overflow", "overflowed", "overflowing", "sewage backing up");
  if (/^hallways?$|^halls?$|^corridors?$/.test(normalized)) add("hallway", "hallways", "hall", "halls", "corridor", "corridors", "common area", "common areas");
  if (normalized === "bathroom") add("bathroom", "bath", "shower", "toilet");
  if (/^showers?$|^bathtubs?$|^tubs?$/.test(normalized)) add("shower", "showers", "bathtub", "bathtubs", "tub", "tubs", "bath");
  if (/^windows?$/.test(normalized)) add("window", "windows", "window sash", "window frame", "window latch", "window lock", "weatherstrip");
  if (normalized === "kitchen") add("kitchen");

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
