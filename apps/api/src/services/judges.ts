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
