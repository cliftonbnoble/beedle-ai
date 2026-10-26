export type CitationContext = "rule" | "ordinance";
export interface ParsedCitation {
  raw: string;
  context: CitationContext | null;
  canonical: string | null;
  parent: string | null;
  part: string | null;
  // Parsing identifies a provision; only catalog resolution can validate it.
  resolution: "unresolved" | "ambiguous" | "invalid";
  parserVersion: "citations/2";
}

const body = String.raw`(?:(?:part\s+[a-z0-9.]+\s*-\s*)|(?:[ivxlcdm]+\s*-\s*))?\d+(?:[.-]\d+)+(?:\s*\(\s*[a-z0-9]+\s*\))*`;
const prefix = String.raw`(?:rules?(?:\s+and\s+regulations?)?|(?:rent\s+)?ordinance)`;
// A word boundary after ')' permits backtracking to the parent. Explicitly
// disallow a continuation of the number or another subsection instead.
const end = String.raw`(?![\p{L}\p{N}_]|\s*\(|\.\d)`;

export function parseCitation(raw: string, context?: CitationContext): ParsedCitation {
  let value = raw.trim().replace(/[.,;:]+$/, "").trim();
  const explicit = value.match(new RegExp(`^(${prefix})\\b\\s*`, "i"));
  const detected = explicit ? (/ordinance/i.test(explicit[1] || "") ? "ordinance" : "rule") : null;
  if (explicit) value = value.slice(explicit[0].length);
  value = value.replace(/^(?:sections?\b|sec\.?\b|no\.?\b|#|§)\s*[.:]?\s*/i, "");
  const part = value.match(/^(?:part\s+([a-z0-9.]+)|([ivxlcdm]+))\s*-\s*/i);
  if (part) value = value.slice(part[0].length);
  const canonical = value.replace(/\s+/g, "").toLowerCase();
  const romanValid = !part?.[2] || /^(?:m{0,4}(?:cm|cd|d?c{0,3})(?:xc|xl|l?x{0,3})(?:ix|iv|v?i{0,3}))$/i.test(part[2]);
  const valid = romanValid && /^\d+(?:[.-]\d+)+(?:\([a-z0-9]+\))*$/.test(canonical);
  const conflict = detected && context && detected !== context;
  const resolvedContext = conflict ? null : detected ?? context ?? null;
  return {
    raw, context: resolvedContext, canonical: valid ? canonical : null,
    parent: valid && canonical.includes("(") ? canonical.replace(/\([^()]+\)$/, "") : null,
    part: part ? (part[1] || part[2] || "").toUpperCase() : null,
    resolution: !valid ? "invalid" : !resolvedContext ? "ambiguous" : "unresolved",
    parserVersion: "citations/2"
  };
}

export function extractCitations(text: string): ParsedCitation[] {
  const matches: Array<{ index: number; end: number; value: ParsedCitation }> = [];
  const explicit = new RegExp(String.raw`\b${prefix}\s*(?:sections?\b|sec\.?|no\.?|#|§)?\s*${body}${end}`, "giu");
  for (const match of text.matchAll(explicit)) {
    matches.push({ index: match.index!, end: match.index! + match[0].length, value: parseCitation(match[0]) });
  }
  const generic = new RegExp(String.raw`(?:\bsections?\b|\bsec\.?|§)\s*${body}${end}`, "giu");
  for (const match of text.matchAll(generic)) {
    const at = match.index!;
    if (matches.some((found) => at >= found.index && at < found.end)) continue;
    const preceding = text.slice(Math.max(0, at - 160), at);
    // Context must be in the same sentence or an immediately preceding heading.
    // If both kinds occur, preserve ambiguity instead of guessing by number.
    const scope = preceding.split(/\.(?:\s|$)|[;!?]/).pop() || "";
    const rule = /\brules?\b|\bregulations?\b/i.test(scope);
    const ordinance = /\bordinance\b/i.test(scope);
    const context = rule !== ordinance ? (rule ? "rule" : "ordinance") : undefined;
    matches.push({ index: at, end: at + match[0].length, value: parseCitation(match[0], context) });
  }
  return matches.sort((a, b) => a.index - b.index).map((match) => match.value);
}

export function citationKey(raw: string, context?: CitationContext): string {
  const parsed = parseCitation(raw, context);
  return parsed.resolution !== "invalid" && (!context || parsed.context === context) ? parsed.canonical || "" : "";
}

// Section numbers can repeat between Parts. Bare keys are only suitable for
// cross-Part candidate lookup (or the conservative critical-citation detector).
export function citationIdentity(raw: string, context?: CitationContext): string {
  const parsed = parseCitation(raw, context);
  const key = citationKey(raw, context);
  return key && parsed.part ? `part:${parsed.part.toLowerCase()}:${key}` : key;
}

export function citationLabel(citation: ParsedCitation): string {
  return `${citation.context === "ordinance" ? "Ordinance" : "Rule"} ${citation.part ? `Part ${citation.part} - ` : ""}${citation.canonical}`;
}
