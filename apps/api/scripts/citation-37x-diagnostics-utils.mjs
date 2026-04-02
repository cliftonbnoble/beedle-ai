function norm(value) {
  return String(value || "").toLowerCase().trim();
}

function normalizeCitationLike(value) {
  return norm(value)
    .replace(/[\s_]+/g, "")
    .replace(/^ordinance/, "")
    .replace(/^rule/, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

export function extract37xKey(issue) {
  const raw = String(issue?.normalizedValue || issue?.rawValue || "");
  const normalized = normalizeCitationLike(raw);
  if (!normalized.startsWith("37.")) return null;
  const base = normalized.replace(/\([a-z0-9]+\)/g, "");
  return { normalized, base };
}

export function classify37xDiagnostic(input) {
  const {
    verifyCheck,
    ordinanceIssueCount,
    rulesIssueCount,
    experimentalAliasMode = false
  } = input;
  const diagnostic = verifyCheck?.diagnostic || "not_found";
  const ordMatches = Array.isArray(verifyCheck?.ordinance_matches) ? verifyCheck.ordinance_matches : [];
  const rulMatches = Array.isArray(verifyCheck?.rules_matches) ? verifyCheck.rules_matches : [];

  let classification = "truly_not_found";
  let rationale = "No exact or related candidate found in normalized references.";
  let experimentalWouldResolve = false;

  if (diagnostic === "exact_match") {
    classification = "resolvable_by_normalization";
    rationale = "Exact citation exists in committed normalized references.";
    experimentalWouldResolve = true;
  } else if (diagnostic === "parent_or_related_only") {
    const crossContextSkew =
      (rulesIssueCount > 0 && ordMatches.length > 0 && rulMatches.length === 0) ||
      (ordinanceIssueCount > 0 && rulMatches.length > 0 && ordMatches.length === 0);
    if (crossContextSkew) {
      classification = "cross_context_alias_candidate";
      rationale = "Citation appears in opposite context as parent/related; candidate for explicit alias mapping.";
      experimentalWouldResolve = experimentalAliasMode;
    } else {
      classification = "resolvable_by_normalization";
      rationale = "Related parent/subsection candidate exists in expected context.";
      experimentalWouldResolve = experimentalAliasMode;
    }
  } else if (diagnostic === "multiple_exact") {
    classification = "cross_context_alias_candidate";
    rationale = "Multiple exact matches across contexts; requires explicit contextual disambiguation.";
    experimentalWouldResolve = false;
  }

  return {
    classification,
    rationale,
    experimentalWouldResolve
  };
}

