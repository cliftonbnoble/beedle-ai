function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function norm(value) {
  return String(value || "").toLowerCase().trim();
}

function hasHardMessage(message) {
  return /(malformed|invalid|unable to parse|unparseable|format error|unknown index)/i.test(String(message || ""));
}

export function classifyUnresolvedReferenceRisk(detail) {
  const issues = asArray(detail?.referenceIssues);
  const unresolvedCount = Number(detail?.unresolvedReferenceCount || issues.length || 0);
  const valid = detail?.validReferences || {};
  const validIndexCount = asArray(valid.indexCodes).length;
  const validRulesCount = asArray(valid.rulesSections).length;
  const validOrdinanceCount = asArray(valid.ordinanceSections).length;
  const criticalExceptionCount = asArray(detail?.criticalExceptionReferences).length;
  const missingRules = Boolean(detail?.missingRulesDetection);
  const missingOrdinance = Boolean(detail?.missingOrdinanceDetection);
  const failedQc = asArray(detail?.failedQcRequirements);
  const anyHardMessage = issues.some((issue) => hasHardMessage(issue?.message));
  const hasIndexIssues = issues.some((issue) => norm(issue?.referenceType) === "index_code");
  const nonWarningSeverity = issues.some((issue) => norm(issue?.severity) && norm(issue?.severity) !== "warning");
  const hasNonRulesOrdinanceIssue = issues.some((issue) => {
    const type = norm(issue?.referenceType);
    return type && type !== "rules_section" && type !== "ordinance_section";
  });

  const lowRiskResidue =
    unresolvedCount > 0 &&
    unresolvedCount <= 4 &&
    criticalExceptionCount === 0 &&
    !missingRules &&
    !missingOrdinance &&
    failedQc.length === 0 &&
    validIndexCount > 0 &&
    validRulesCount > 0 &&
    validOrdinanceCount > 0 &&
    !anyHardMessage &&
    !hasIndexIssues &&
    !nonWarningSeverity &&
    !hasNonRulesOrdinanceIssue;

  return {
    unresolvedCount,
    lowRiskResidue,
    category: lowRiskResidue ? "low_risk_residue" : "risky_unresolved",
    reasons: {
      criticalExceptionCount,
      validIndexCount,
      validRulesCount,
      validOrdinanceCount,
      missingRules,
      missingOrdinance,
      failedQcCount: failedQc.length,
      anyHardMessage,
      hasIndexIssues,
      nonWarningSeverity,
      hasNonRulesOrdinanceIssue
    }
  };
}

export function isConservativeRemediationCandidate(doc, detail) {
  if (doc?.isLikelyFixture) return { eligible: false, reason: "fixture" };
  const blockers = asArray(doc?.approvalReadiness?.blockers);
  const allowedBlockers = new Set(["metadata_not_confirmed", "qc_gate_not_passed", "unresolved_references_above_threshold"]);
  if (blockers.length === 0 || blockers.some((b) => !allowedBlockers.has(b))) {
    return { eligible: false, reason: "unsupported_blockers" };
  }
  const risk = classifyUnresolvedReferenceRisk(detail);
  if (risk.unresolvedCount > 0 && !risk.lowRiskResidue) {
    return { eligible: false, reason: "risky_unresolved", risk };
  }
  if ((detail?.extractionConfidence || 0) < 0.6) {
    return { eligible: false, reason: "low_confidence", risk };
  }
  return { eligible: true, reason: "eligible", risk };
}

