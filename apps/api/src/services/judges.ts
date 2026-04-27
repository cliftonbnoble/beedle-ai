const CANONICAL_JUDGE_NAMES = [
  "René Juárez",
  "Andrew Yick",
  "Connie Brandon",
  "Deborah K. Lim",
  "Dorothy Chou Proudfoot",
  "Erin E. Katayama",
  "Harrison Nam",
  "Jeffrey Eckber",
  "Jill Figg Dayal",
  "Joseph Koomas",
  "Michael J. Berg",
  "Peter Kearns"
] as const;

const HONORIFIC_PREFIX = /^(judge|hon\.?|honorable|administrative law judge|alj|hearing officer|dated|date)\s+/i;
const SIGNATURE_PREFIX = /^\/?s\/?\s*/i;

function normalizeWhitespace(value: string): string {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function stripDiacritics(value: string): string {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

export function normalizeJudgeLookupKey(value: string): string {
  return stripDiacritics(value)
    .toLowerCase()
    .replace(HONORIFIC_PREFIX, "")
    .replace(SIGNATURE_PREFIX, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function judgeAliases(judge: string): string[] {
  const key = normalizeJudgeLookupKey(judge);
  const parts = key.split(" ").filter(Boolean);
  if (!parts.length) return [];
  const surname = parts.slice(-1).join(" ");
  const firstLast = parts.length >= 2 ? `${parts[0]} ${surname}` : key;
  return Array.from(new Set([key, surname, firstLast].filter(Boolean)));
}

function toDisplayJudgeCase(value: string): string {
  return normalizeWhitespace(value)
    .split(/\s+/)
    .map((part) => {
      if (!part) return part;
      if (/^[A-Z]\.$/.test(part)) return part;
      if (/^[A-Z]{1,3}$/.test(part)) return part;
      const pieces = part.split(/([.'’-])/);
      return pieces
        .map((piece) => {
          if (!piece) return piece;
          if (/^[.'’-]$/.test(piece)) return piece;
          const lower = piece.toLowerCase();
          return lower.charAt(0).toUpperCase() + lower.slice(1);
        })
        .join("");
    })
    .join(" ");
}

export function canonicalJudgeNames(): readonly string[] {
  return CANONICAL_JUDGE_NAMES;
}

export function canonicalizeJudgeName(value: string | null | undefined): string | null {
  const raw = normalizeWhitespace(value || "");
  if (!raw) return null;
  const normalized = normalizeJudgeLookupKey(raw);
  if (!normalized) return null;

  for (const judge of CANONICAL_JUDGE_NAMES) {
    for (const alias of judgeAliases(judge)) {
      if (normalized === alias) return judge;
      if (normalized.includes(alias) || alias.includes(normalized)) return judge;
    }
  }

  return raw;
}

function isClearlyInvalidNonblankJudge(value: string): boolean {
  const raw = normalizeWhitespace(value);
  if (!raw) return false;
  if (CANONICAL_JUDGE_NAMES.includes(raw as (typeof CANONICAL_JUDGE_NAMES)[number])) return false;
  if (/^unknown$|^judge unknown$|^<unknown>$/i.test(raw)) return true;
  if (/san francisco|francisco|california/i.test(raw)) return true;
  if (/[.,]{2,}/.test(raw)) return true;
  if (/\b(llc|trust|properties|management)\b/i.test(raw)) return true;
  if (/\b(storage|space|denied|seismic|scaffolding|security|gate|retrofit|subtenant|smoke|sidewalk|roof|camera|petition|minute order|decision)\b/i.test(raw)) {
    return true;
  }
  if (/[0-9]/.test(raw)) return true;
  if (raw.length < 4) return true;
  const displayCase = toDisplayJudgeCase(raw);
  const looksLikeName = /^[A-Z][A-Za-z'’. -]+(?:\s+[A-Z][A-Za-z'’. -]+){1,4}$/.test(displayCase);
  return !looksLikeName;
}

export function sanitizeDisplayJudgeName(value: string | null | undefined): string | null {
  const canonical = canonicalizeJudgeName(value);
  if (!canonical) return null;
  if (isClearlyInvalidNonblankJudge(canonical)) return null;
  if (CANONICAL_JUDGE_NAMES.includes(canonical as (typeof CANONICAL_JUDGE_NAMES)[number])) return canonical;
  return toDisplayJudgeCase(canonical);
}

export function extractCanonicalJudgeNamesFromText(text: string): string[] {
  const lookup = normalizeJudgeLookupKey(text || "");
  if (!lookup) return [];
  const matched: string[] = [];
  for (const judge of CANONICAL_JUDGE_NAMES) {
    for (const alias of judgeAliases(judge)) {
      if (!alias) continue;
      if (lookup.includes(alias)) {
        matched.push(judge);
        break;
      }
    }
  }
  return Array.from(new Set(matched));
}

export function inferJudgeFromTextFragments(fragments: Array<string | null | undefined>): string | null {
  const signatureMatches = new Set<string>();
  const canonicalMatches = new Set<string>();

  for (const fragment of fragments) {
    const raw = String(fragment || "");
    if (!raw.trim()) continue;

    const signatureMatch = raw.match(/\n\/?s\/?\s*([A-Z][A-Za-z .,'-]{3,90})\b/i);
    if (signatureMatch?.[1]) {
      const canonical = canonicalizeJudgeName(normalizeWhitespace(signatureMatch[1]));
      if (canonical && CANONICAL_JUDGE_NAMES.includes(canonical as (typeof CANONICAL_JUDGE_NAMES)[number])) {
        signatureMatches.add(canonical);
      }
    }

    for (const judge of extractCanonicalJudgeNamesFromText(raw)) {
      canonicalMatches.add(judge);
    }
  }

  if (signatureMatches.size === 1) return Array.from(signatureMatches)[0] || null;
  if (signatureMatches.size > 1) return null;
  if (canonicalMatches.size === 1) return Array.from(canonicalMatches)[0] || null;
  return null;
}

export function judgeSearchTerms(authorName: string | null | undefined): string[] {
  const canonical = canonicalizeJudgeName(authorName);
  if (!canonical) return [];
  const key = normalizeJudgeLookupKey(canonical);
  return Array.from(
    new Set(
      [
        canonical,
        key,
        ...judgeAliases(canonical)
      ].filter(Boolean)
    )
  );
}

export function queryReferencesJudge(query: string): string[] {
  return extractCanonicalJudgeNamesFromText(query || "");
}
