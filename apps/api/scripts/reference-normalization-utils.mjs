export function chooseParserPath({ pdfCount, textCount, minThreshold }) {
  if (typeof textCount === "number" && (pdfCount < minThreshold || textCount > pdfCount)) {
    return "text_export";
  }
  return "pdf";
}

export function choosePreferredSource({
  trueText,
  layoutText,
  pdf,
  minThreshold
}) {
  const candidates = [];
  if (trueText) candidates.push({ ...trueText, source_type: "true_text" });
  if (layoutText) candidates.push({ ...layoutText, source_type: "layout_text" });
  if (pdf) candidates.push({ ...pdf, source_type: "pdf" });
  if (candidates.length === 0) {
    throw new Error("no source candidates available");
  }

  const trueCandidate = candidates.find((item) => item.source_type === "true_text");
  if (!trueCandidate) {
    return candidates[0];
  }

  let selected = trueCandidate;
  const layoutCandidate = candidates.find((item) => item.source_type === "layout_text");
  const pdfCandidate = candidates.find((item) => item.source_type === "pdf");

  if (
    layoutCandidate &&
    (selected.section_count === 0 || (selected.section_count < minThreshold && layoutCandidate.section_count > selected.section_count))
  ) {
    selected = { ...layoutCandidate, fallback_reason: "true_text_low_coverage" };
  }

  if (
    pdfCandidate &&
    (selected.section_count === 0 || (selected.section_count < minThreshold && pdfCandidate.section_count > selected.section_count))
  ) {
    selected = { ...pdfCandidate, fallback_reason: "text_sources_low_coverage" };
  }

  return selected;
}

export function normalizeSectionRef(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section|^sec|^§|^rule/g, "")
    .replace(/^part[0-9a-z.\-]+\-/g, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

export function citationMatch(normalizedQuery, normalizedCandidate) {
  if (!normalizedQuery || !normalizedCandidate) return false;
  if (normalizedQuery === normalizedCandidate) return true;
  if (normalizedCandidate.startsWith(`${normalizedQuery}(`)) return true;
  return false;
}
