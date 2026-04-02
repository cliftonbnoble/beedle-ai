function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function preserveTokenCase(source: string, replacement: string) {
  const compactSource = source.replace(/\s+/g, "");
  if (!compactSource) return replacement;
  if (compactSource === compactSource.toUpperCase()) return replacement.toUpperCase();
  if (
    compactSource[0] === compactSource[0]?.toUpperCase() &&
    compactSource.slice(1) === compactSource.slice(1).toLowerCase()
  ) {
    return `${replacement[0]?.toUpperCase() || ""}${replacement.slice(1).toLowerCase()}`;
  }
  return replacement.toLowerCase();
}

function queryRepairTerms(query: string) {
  return Array.from(
    new Set(
      String(query || "")
        .split(/[\s,.;:()/"'-]+/)
        .map((term) => term.trim())
        .filter((term) => term.length >= 3)
    )
  ).sort((a, b) => b.length - a.length);
}

function repairQuerySpacing(text: string, query: string) {
  let repaired = String(text || "");
  for (const term of queryRepairTerms(query)) {
    const compactTerm = term.replace(/[^A-Za-z0-9]/g, "");
    if (compactTerm.length < 3) continue;
    const loosePattern = new RegExp(
      `(?<![A-Za-z0-9])${compactTerm.split("").map(escapeRegExp).join("\\s*")}(?![A-Za-z0-9])`,
      "gi"
    );
    repaired = repaired.replace(loosePattern, (match) => preserveTokenCase(match, compactTerm));
  }
  return repaired;
}

function repairSuspiciousIntraWordSpacing(text: string) {
  let repaired = String(text || "").replace(/\b(?:[A-Za-z]\s+){1,4}[A-Za-z]{2,}\b/g, (match) => {
    const parts = match.trim().split(/\s+/).filter(Boolean);
    if (parts.length < 2) return match;
    const merged = parts.join("");
    if (merged.length < 4) return match;
    if (!/^[A-Za-z]+$/.test(merged)) return match;
    if (!/^[A-Z]?[a-z]+$/.test(merged)) return match;

    const first = parts[0] || "";
    const singleLetterPrefixCount = parts.slice(0, -1).length;
    const ambiguousStandalone = /^(?:A|a|I)$/.test(first);
    if (ambiguousStandalone && singleLetterPrefixCount < 2) return match;

    return preserveTokenCase(match, merged);
  });

  repaired = repaired.replace(/\b[A-Za-z](?:\s+[A-Za-z]{1,4}){1,3}\b/g, (match) => {
    const parts = match.trim().split(/\s+/).filter(Boolean);
    if (parts.length < 2) return match;
    if ((parts[0] || "").length !== 1) return match;
    if (parts.some((part) => !/^[A-Za-z]{1,4}$/.test(part))) return match;

    const merged = parts.join("");
    if (!/^[A-Za-z]+$/.test(merged)) return match;

    const lowerMerged = merged.toLowerCase();
    const allowShortWord =
      merged.length >= 4 ||
      new Set(["the", "and", "not", "was", "are", "for"]).has(lowerMerged);
    if (!allowShortWord) return match;

    const first = parts[0] || "";
    const ambiguousStandalone = /^(?:A|a|I)$/.test(first);
    if (ambiguousStandalone && parts.length < 3) return match;

    return preserveTokenCase(match, merged);
  });

  return repaired;
}

export function repairDisplayText(text: string, query?: string) {
  const withNormalizedSpaces = String(text || "")
    .replace(/[\u00AD\u200B-\u200D\u2060\uFEFF]/g, "")
    .replace(/[\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000]/g, " ");
  const queryRepaired = query ? repairQuerySpacing(withNormalizedSpaces, query) : withNormalizedSpaces;
  return repairSuspiciousIntraWordSpacing(queryRepaired);
}
