function parseJsonArray(input) {
  if (!input) return [];
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed.map((value) => String(value).trim()).filter(Boolean) : [];
  } catch {
    return [];
  }
}

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((value) => String(value).trim()).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  );
}

function serializeArray(values) {
  return JSON.stringify(uniqueSorted(values));
}

export function deriveCompanionBaseKey(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/-POST-HEARING-ORDER/g, "")
    .replace(/-DECISION-TECH[.-]?\s*CORR-?\d*/g, "")
    .replace(/-TECH[.-]?\s*CORR-?\d*/g, "")
    .replace(/-DECISION-\d{1,2}-\d{1,2}-\d{2,4}/g, "")
    .replace(/-DECISION/g, "")
    .replace(/-ORDER/g, "")
    .replace(/\s+/g, "")
    .trim();
}

export function classifyCompanionFamily(row) {
  const text = `${row?.title || ""} ${row?.citation || ""}`.toUpperCase();
  if (text.includes("TECH") && text.includes("CORR")) return "tech_corr";
  if (text.includes("POST HEARING ORDER")) return "post_hearing_order";
  if (text.includes("MINUTE ORDER")) return "minute_order";
  if (text.includes("REMAND")) return "remand";
  if (text.includes("ORDER")) return "order_other";
  if (text.includes("DECISION")) return "decision_other";
  return "other";
}

export function buildCompanionRecoveryCandidates(targetRows = [], siblingRows = []) {
  const siblingsByBaseKey = new Map();

  for (const row of siblingRows) {
    const baseKey = deriveCompanionBaseKey(row.citation || row.title || "");
    if (!baseKey) continue;
    const list = siblingsByBaseKey.get(baseKey) || [];
    list.push({
      documentId: row.documentId,
      title: row.title,
      citation: row.citation,
      searchableAt: row.searchableAt,
      qcPassed: Number(row.qcPassed || 0),
      extractionConfidence: Number(row.extractionConfidence || 0),
      updatedAt: String(row.updatedAt || ""),
      indexCodes: parseJsonArray(row.indexCodesJson),
      rulesSections: parseJsonArray(row.rulesSectionsJson),
      ordinanceSections: parseJsonArray(row.ordinanceSectionsJson)
    });
    siblingsByBaseKey.set(baseKey, list);
  }

  const selected = [];
  const skipped = [];

  for (const row of targetRows) {
    const targetIndexCodes = parseJsonArray(row.indexCodesJson);
    const targetRulesSections = parseJsonArray(row.rulesSectionsJson);
    const targetOrdinanceSections = parseJsonArray(row.ordinanceSectionsJson);
    const missingIndexCodes = targetIndexCodes.length === 0;
    const missingRulesSections = targetRulesSections.length === 0;
    const missingOrdinanceSections = targetOrdinanceSections.length === 0;
    const baseKey = deriveCompanionBaseKey(row.citation || row.title || "");
    const family = classifyCompanionFamily(row);

    if (!baseKey) {
      skipped.push({ row, evaluation: { eligible: false, reason: "empty_base_key", family } });
      continue;
    }

    if (!(missingIndexCodes || missingRulesSections || missingOrdinanceSections)) {
      skipped.push({ row, evaluation: { eligible: false, reason: "no_missing_metadata", family } });
      continue;
    }

    const siblings = (siblingsByBaseKey.get(baseKey) || [])
      .filter((candidate) => candidate.documentId !== row.documentId)
      .sort((a, b) => {
        const searchableDelta = Number(Boolean(b.searchableAt)) - Number(Boolean(a.searchableAt));
        if (searchableDelta !== 0) return searchableDelta;
        const qcDelta = Number(Boolean(b.qcPassed)) - Number(Boolean(a.qcPassed));
        if (qcDelta !== 0) return qcDelta;
        const confidenceDelta = Number(b.extractionConfidence || 0) - Number(a.extractionConfidence || 0);
        if (confidenceDelta !== 0) return confidenceDelta;
        return String(b.updatedAt || "").localeCompare(String(a.updatedAt || ""));
      });

    if (!siblings.length) {
      skipped.push({ row, evaluation: { eligible: false, reason: "no_companion_sibling", family, baseKey } });
      continue;
    }

    const uniqueIndexPayloads = uniqueSorted(
      siblings.filter((sibling) => sibling.indexCodes.length > 0).map((sibling) => serializeArray(sibling.indexCodes))
    );
    const uniqueRulesPayloads = uniqueSorted(
      siblings.filter((sibling) => sibling.rulesSections.length > 0).map((sibling) => serializeArray(sibling.rulesSections))
    );
    const uniqueOrdinancePayloads = uniqueSorted(
      siblings.filter((sibling) => sibling.ordinanceSections.length > 0).map((sibling) => serializeArray(sibling.ordinanceSections))
    );

    if (missingIndexCodes && uniqueIndexPayloads.length !== 1) {
      skipped.push({
        row,
        evaluation: { eligible: false, reason: "index_payload_not_unanimous", family, baseKey, siblingCount: siblings.length }
      });
      continue;
    }
    if (missingRulesSections && uniqueRulesPayloads.length !== 1) {
      skipped.push({
        row,
        evaluation: { eligible: false, reason: "rules_payload_not_unanimous", family, baseKey, siblingCount: siblings.length }
      });
      continue;
    }
    if (missingOrdinanceSections && uniqueOrdinancePayloads.length !== 1) {
      skipped.push({
        row,
        evaluation: { eligible: false, reason: "ordinance_payload_not_unanimous", family, baseKey, siblingCount: siblings.length }
      });
      continue;
    }

    const selectedIndexCodes = missingIndexCodes ? JSON.parse(uniqueIndexPayloads[0] || "[]") : targetIndexCodes;
    const selectedRulesSections = missingRulesSections ? JSON.parse(uniqueRulesPayloads[0] || "[]") : targetRulesSections;
    const selectedOrdinanceSections = missingOrdinanceSections ? JSON.parse(uniqueOrdinancePayloads[0] || "[]") : targetOrdinanceSections;

    if (!(selectedIndexCodes.length > 0 && selectedRulesSections.length > 0 && selectedOrdinanceSections.length > 0)) {
      skipped.push({
        row,
        evaluation: { eligible: false, reason: "recovered_payload_incomplete", family, baseKey, siblingCount: siblings.length }
      });
      continue;
    }

    selected.push({
      row,
      evaluation: {
        eligible: true,
        reason: "companion_unanimous_payload",
        family,
        baseKey,
        siblingCount: siblings.length,
        selectedIndexCodes,
        selectedRulesSections,
        selectedOrdinanceSections,
        siblingSample: siblings.slice(0, 3).map((sibling) => ({
          documentId: sibling.documentId,
          citation: sibling.citation,
          title: sibling.title,
          searchable: Boolean(sibling.searchableAt),
          qcPassed: Boolean(sibling.qcPassed)
        })),
        payload: {
          index_codes: selectedIndexCodes,
          rules_sections: selectedRulesSections,
          ordinance_sections: selectedOrdinanceSections,
          confirm_required_metadata: true
        }
      }
    });
  }

  selected.sort((a, b) => {
    const familyRank = (value) =>
      ["tech_corr", "post_hearing_order", "remand", "minute_order", "order_other", "decision_other", "other"].indexOf(
        value
      );
    const familyDelta = familyRank(a.evaluation.family) - familyRank(b.evaluation.family);
    if (familyDelta !== 0) return familyDelta;
    const siblingDelta = Number(b.evaluation.siblingCount || 0) - Number(a.evaluation.siblingCount || 0);
    if (siblingDelta !== 0) return siblingDelta;
    return String(b.row?.decisionDate || "").localeCompare(String(a.row?.decisionDate || ""));
  });

  skipped.sort((a, b) => String(a.evaluation.reason || "").localeCompare(String(b.evaluation.reason || "")));

  return { selected, skipped };
}
