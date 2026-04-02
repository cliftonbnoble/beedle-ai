function cleanWhitespace(input) {
  return String(input || "").replace(/\s+/g, " ").trim();
}

function cleanTextForProbe(input) {
  return cleanWhitespace(String(input || "").replace(/<[^>]+>/g, " "));
}

function isLowSignalSentence(input) {
  const text = cleanTextForProbe(input);
  if (!text || text.length < 30) return true;
  const lower = text.toLowerCase();
  if (/(see attached proof of service|proof of service|attached proof|service list)/i.test(lower)) return true;
  if (/(hearing|calendar|notice|date)\s*[:\-]/i.test(lower) && text.length < 95) return true;
  if (/^\(?case\s*no\.?/i.test(lower) && text.length < 95) return true;
  if (/^(before|present|re:|regarding)\b/i.test(lower) && text.length < 50) return true;
  if (/^[A-Z][A-Z\s,.'-]{0,80}$/.test(text)) return true;
  return false;
}

function sentenceScore(input) {
  const text = cleanTextForProbe(input);
  const lower = text.toLowerCase();
  const legalSignals = [
    "service reduction",
    "rent",
    "tenant",
    "landlord",
    "petition",
    "finding",
    "findings",
    "evidence",
    "analysis",
    "conclusion",
    "ordinance",
    "rule",
    "index code",
    "housing",
    "unit",
    "harassment",
    "notice",
    "violation"
  ];
  let score = 0;
  for (const signal of legalSignals) {
    if (lower.includes(signal)) score += 2;
  }
  const tokenCount = text.split(/\s+/).filter(Boolean).length;
  if (tokenCount >= 8) score += 1;
  if (tokenCount >= 14) score += 1;
  if (/[.;:]/.test(text)) score += 1;
  if (/\b\d+(?:\.\d+)+(?:\([a-z0-9]+\))?/i.test(text)) score += 1;
  return score;
}

export function buildSelfQueryVariants(input) {
  const title = cleanWhitespace(input?.title || "");
  const citation = cleanWhitespace(input?.citation || "");
  const variants = [];
  if (title) variants.push(title);
  if (title) variants.push(title.replace(/\s*-\s*/g, " ").replace(/\s+/g, " ").trim());
  if (citation) variants.push(citation);
  if (citation) variants.push(citation.replace(/[-_]/g, " "));
  return Array.from(new Set(variants.filter(Boolean)));
}

export function buildContentProbe(detail, fallback) {
  const chunks = Array.isArray(detail?.chunks) ? detail.chunks : [];
  const candidates = [];
  for (const chunk of chunks) {
    const chunkText = cleanTextForProbe(chunk?.chunkText || "");
    if (!chunkText || chunkText.length < 40) continue;
    const sentences = chunkText
      .split(/(?<=[.?!])\s+/)
      .map((item) => cleanTextForProbe(item))
      .filter(Boolean);
    for (const sentence of sentences) {
      if (isLowSignalSentence(sentence)) continue;
      const score = sentenceScore(sentence);
      candidates.push({ query: sentence.slice(0, 160), score });
    }
  }

  candidates.sort((a, b) => b.score - a.score || b.query.length - a.query.length);
  if (candidates[0] && candidates[0].score >= 3) {
    return { query: candidates[0].query, skipped: false, skippedReason: null, score: candidates[0].score };
  }

  const fallbackText = cleanWhitespace(fallback || "");
  if (fallbackText) {
    return { query: fallbackText, skipped: true, skippedReason: "low_signal_chunks_fallback_to_title", score: 0 };
  }
  return { query: "", skipped: true, skippedReason: "no_content_probe_available", score: 0 };
}

function splitRuleCitation(input) {
  const clean = cleanWhitespace(input).replace(/^rule\s+/i, "");
  const match = clean.match(/^([IVXLC]+-)?(\d+(?:\.\d+)+(?:\([a-z0-9]+\))*)$/i);
  if (!match) return null;
  const display = clean;
  const bare = match[2];
  return { display, bare };
}

export function buildLegalQueryVariants(detail) {
  const refs = detail?.validReferences || {};
  const rules = Array.isArray(refs.rulesSections) ? refs.rulesSections : [];
  const ordinance = Array.isArray(refs.ordinanceSections) ? refs.ordinanceSections : [];
  const indexCodes = Array.isArray(refs.indexCodes) ? refs.indexCodes : [];

  const variants = [];
  for (const rule of rules) {
    const parsed = splitRuleCitation(rule);
    if (!parsed) continue;
    variants.push(parsed.display);
    variants.push(parsed.bare);
    variants.push(`Rule ${parsed.bare}`);
    variants.push(`Part ${parsed.display}`);
  }
  for (const ord of ordinance) {
    const clean = cleanWhitespace(ord).replace(/^ordinance\s+/i, "");
    if (!clean) continue;
    variants.push(clean);
    variants.push(`Ordinance ${clean}`);
  }
  for (const code of indexCodes) {
    const clean = cleanWhitespace(code);
    if (clean) variants.push(clean);
  }

  const deduped = Array.from(new Set(variants.map((item) => cleanWhitespace(item)).filter(Boolean)));
  if (deduped.length === 0) {
    return { variants: [], skipped: true, skippedReason: "no_validated_legal_references" };
  }
  return { variants: deduped.slice(0, 8), skipped: false, skippedReason: null };
}

