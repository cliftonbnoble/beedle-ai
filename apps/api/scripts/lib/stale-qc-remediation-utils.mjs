function parseJsonArray(input) {
  if (!input) return [];
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed.map((value) => String(value)) : [];
  } catch {
    return [];
  }
}

function parseJsonObject(input) {
  if (!input) return {};
  try {
    const parsed = JSON.parse(input);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((value) => String(value).trim()).filter(Boolean)));
}

function normalizeUnknownList(values) {
  return unique(values).sort((a, b) => a.localeCompare(b));
}

function extractUnknownRefsFromWarning(prefix, warning) {
  if (!warning.startsWith(prefix)) return [];
  return unique(
    warning
      .slice(prefix.length)
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
  );
}

export const DEFAULT_MIN_EXTRACTION_CONFIDENCE = 0.7;
export const DEFAULT_ALLOW_DUAL_CONTEXT_3715 = true;

export const DEFAULT_ALLOWED_UNKNOWN_RULES = [
  "Rule 37.8",
  "Rule 37.2",
  "Rule 1954.53",
  "Rule 1954.52",
  "Rule 11.16",
  "Rule 37.1",
  "Rule 1954.50",
  "Rule 4.1",
  "Rule 10.1",
  "Rule 6.1",
  "Rule 954.53",
  "Rule 6.12",
  "Rule 919.1",
  "Rule 37.73",
  "Rule 37.5",
  "Rule 1954.51",
  "Rule 11.8",
  "Rule 1.40",
  "Rule 1.25"
];

export const DEFAULT_ALLOWED_UNKNOWN_ORDINANCE = [
  "Ordinance 37.3",
  "Ordinance 37.15",
  "Ordinance 37.7",
  "Ordinance 37.9",
  "Ordinance 37.14",
  "Ordinance 37.12",
  "Ordinance 6.14",
  "Ordinance 5.10",
  "Ordinance 37.9(a)",
  "Ordinance 37.73",
  "Ordinance 37.5",
  "Ordinance 37.4",
  "Ordinance 37.10",
  "Ordinance 32.3"
];

export function classifyWarning(warning) {
  const text = String(warning || "");
  if (!text) return "empty";
  if (text.startsWith("Recovered ")) return "recovered_heading";
  if (text.startsWith("Split long merged paragraph")) return "split_long_paragraph";
  if (text === "Index Codes not detected") return "stale_index_warning";
  if (text === "Taxonomy suggestion is low-confidence; review case type during QC") return "low_confidence_taxonomy";
  if (text.startsWith("Unknown rules references (manual review):")) return "unknown_rules";
  if (text.startsWith("Unknown ordinance references (manual review):")) return "unknown_ordinance";
  if (text.startsWith("Extraction noise filtered")) return "extraction_noise_filtered";
  if (text.startsWith("Critical reference exception:")) return "critical_reference_exception";
  if (text.startsWith("Unknown index codes (manual review):")) return "unknown_index_codes";
  return "other";
}

function extractReferenceValidation(metadataJson, warnings) {
  const metadata = parseJsonObject(metadataJson);
  const referenceValidation =
    metadata.referenceValidation && typeof metadata.referenceValidation === "object"
      ? metadata.referenceValidation
      : {};

  const warningList = parseJsonArray(warnings);
  const warningUnknownRules = warningList.flatMap((warning) =>
    extractUnknownRefsFromWarning("Unknown rules references (manual review):", warning)
  );
  const warningUnknownOrdinance = warningList.flatMap((warning) =>
    extractUnknownRefsFromWarning("Unknown ordinance references (manual review):", warning)
  );
  const warningUnknownIndexCodes = warningList.flatMap((warning) =>
    extractUnknownRefsFromWarning("Unknown index codes (manual review):", warning)
  );

  return {
    unknownRules: normalizeUnknownList([
      ...(Array.isArray(referenceValidation.unknownRules) ? referenceValidation.unknownRules : []),
      ...warningUnknownRules
    ]),
    unknownOrdinance: normalizeUnknownList([
      ...(Array.isArray(referenceValidation.unknownOrdinance) ? referenceValidation.unknownOrdinance : []),
      ...warningUnknownOrdinance
    ]),
    unknownIndexCodes: normalizeUnknownList([
      ...(Array.isArray(referenceValidation.unknownIndexCodes) ? referenceValidation.unknownIndexCodes : []),
      ...warningUnknownIndexCodes
    ])
  };
}

export function evaluateStaleQcCandidate(
  row,
  {
    minExtractionConfidence = DEFAULT_MIN_EXTRACTION_CONFIDENCE,
    allowedUnknownRules = DEFAULT_ALLOWED_UNKNOWN_RULES,
    allowedUnknownOrdinance = DEFAULT_ALLOWED_UNKNOWN_ORDINANCE,
    allowDualContext3715 = DEFAULT_ALLOW_DUAL_CONTEXT_3715
  } = {}
) {
  const warnings = parseJsonArray(row.extractionWarningsJson);
  const categories = warnings.map(classifyWarning);
  const indexCodes = parseJsonArray(row.indexCodesJson);
  const rulesSections = parseJsonArray(row.rulesSectionsJson);
  const ordinanceSections = parseJsonArray(row.ordinanceSectionsJson);
  const referenceValidation = extractReferenceValidation(row.metadataJson, row.extractionWarningsJson);
  const criticalWarnings = warnings.filter((warning) => classifyWarning(warning) === "critical_reference_exception");
  const hasDualContext3715 =
    rulesSections.includes("Rule 37.15") &&
    ordinanceSections.includes("Ordinance 37.15") &&
    criticalWarnings.length > 0 &&
    criticalWarnings.every((warning) =>
      String(warning).includes("37.15 may be cross-context ambiguous")
    );

  if (Number(row.qcPassed || 0) > 0) {
    return { eligible: false, reason: "already_qc_passed" };
  }
  if (Number(row.qcRequiredConfirmed || 0) > 0) {
    return { eligible: false, reason: "already_confirmed" };
  }
  if (indexCodes.length === 0 || rulesSections.length === 0 || ordinanceSections.length === 0) {
    return { eligible: false, reason: "missing_required_arrays" };
  }
  if (Number(row.extractionConfidence || 0) < minExtractionConfidence) {
    return { eligible: false, reason: "extraction_confidence_below_threshold" };
  }
  if (referenceValidation.unknownIndexCodes.length > 0) {
    return {
      eligible: false,
      reason: "unknown_index_codes_present",
      unknownIndexCodes: referenceValidation.unknownIndexCodes
    };
  }
  if (categories.includes("critical_reference_exception") && !(allowDualContext3715 && hasDualContext3715)) {
    return { eligible: false, reason: "critical_reference_exception_present" };
  }

  const disallowedWarnings = warnings.filter((warning) => {
    const category = classifyWarning(warning);
    if (category === "critical_reference_exception" && allowDualContext3715 && hasDualContext3715) {
      return false;
    }
    return ![
      "recovered_heading",
      "split_long_paragraph",
      "stale_index_warning",
      "low_confidence_taxonomy",
      "unknown_rules",
      "unknown_ordinance",
      "extraction_noise_filtered"
    ].includes(category);
  });
  if (disallowedWarnings.length > 0) {
    return {
      eligible: false,
      reason: "disallowed_warning_shape",
      disallowedWarnings
    };
  }

  const allowedRulesSet = new Set(allowedUnknownRules.map(String));
  const allowedOrdinanceSet = new Set(allowedUnknownOrdinance.map(String));
  const disallowedUnknownRules = referenceValidation.unknownRules.filter((value) => !allowedRulesSet.has(value));
  const disallowedUnknownOrdinance = referenceValidation.unknownOrdinance.filter((value) => !allowedOrdinanceSet.has(value));

  if (disallowedUnknownRules.length > 0) {
    return {
      eligible: false,
      reason: "disallowed_unknown_rules",
      disallowedUnknownRules
    };
  }
  if (disallowedUnknownOrdinance.length > 0) {
    return {
      eligible: false,
      reason: "disallowed_unknown_ordinance",
      disallowedUnknownOrdinance
    };
  }

  return {
    eligible: true,
    reason: "stale_qc_safe_autoconfirm",
    warningCategories: Array.from(new Set(categories)).sort(),
    unknownRules: referenceValidation.unknownRules,
    unknownOrdinance: referenceValidation.unknownOrdinance,
    payload: {
      index_codes: indexCodes,
      rules_sections: rulesSections,
      ordinance_sections: ordinanceSections,
      confirm_required_metadata: true
    }
  };
}

export function selectStaleQcCandidates(rows = [], options = {}) {
  const selected = [];
  const skipped = [];

  for (const row of rows) {
    const evaluation = evaluateStaleQcCandidate(row, options);
    if (evaluation.eligible) {
      selected.push({ row, evaluation });
    } else {
      skipped.push({ row, evaluation });
    }
  }

  selected.sort((a, b) => {
    const confidenceDelta = Number(b.row.extractionConfidence || 0) - Number(a.row.extractionConfidence || 0);
    if (confidenceDelta !== 0) return confidenceDelta;
    const unknownRefDelta =
      (a.evaluation.unknownRules?.length || 0) +
      (a.evaluation.unknownOrdinance?.length || 0) -
      ((b.evaluation.unknownRules?.length || 0) + (b.evaluation.unknownOrdinance?.length || 0));
    if (unknownRefDelta !== 0) return unknownRefDelta;
    const dateDelta = String(b.row.decisionDate || "").localeCompare(String(a.row.decisionDate || ""));
    if (dateDelta !== 0) return dateDelta;
    return String(a.row.citation || "").localeCompare(String(b.row.citation || ""));
  });

  skipped.sort((a, b) => {
    const reasonDelta = String(a.evaluation.reason || "").localeCompare(String(b.evaluation.reason || ""));
    if (reasonDelta !== 0) return reasonDelta;
    return String(a.row.citation || "").localeCompare(String(b.row.citation || ""));
  });

  return { selected, skipped };
}
