const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function splitList(value) {
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function safeLower(value) {
  return normalizeText(value).toLowerCase();
}

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function parseUnresolvedReferences(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function parseRows(rows) {
  return (rows || []).map((row) => ({
    documentId: String(row.documentId || ""),
    title: String(row.title || ""),
    batchKey: String(row.batchKey || ""),
    blocked37xFamily: splitList(row.blocked37xFamily),
    unresolvedTriageBuckets: splitList(row.unresolvedTriageBuckets),
    recurringCitationFamily: splitList(row.recurringCitationFamily),
    blockers: splitList(row.blockers),
    unresolvedReferences: parseUnresolvedReferences(row.exactUnresolvedReferences).map((item) => ({
      referenceType: String(item.referenceType || ""),
      rawValue: String(item.rawValue || ""),
      normalizedValue: String(item.normalizedValue || ""),
      rootCause: String(item.rootCause || ""),
      message: String(item.message || "")
    }))
  }));
}

function classifyContextWords(snippet) {
  const lower = safeLower(snippet);
  const ordinanceSignals = ["ordinance", "rent ordinance", "section", "sec.", "code section", "chapter"];
  const rulesSignals = ["rule ", "rules", "regulation", "regulations", "part ", "article "];
  const ordinanceHits = ordinanceSignals.filter((token) => lower.includes(token));
  const rulesHits = rulesSignals.filter((token) => lower.includes(token));
  if (ordinanceHits.length > 0 && rulesHits.length === 0) return { contextClass: "likely_ordinance_wording", nearbyWords: ordinanceHits };
  if (rulesHits.length > 0 && ordinanceHits.length === 0) return { contextClass: "likely_rules_wording", nearbyWords: rulesHits };
  if (rulesHits.length > 0 && ordinanceHits.length > 0) return { contextClass: "mixed_ambiguous_wording", nearbyWords: [...ordinanceHits, ...rulesHits] };
  return { contextClass: "no_useful_context", nearbyWords: [] };
}

export function classifyEvidenceContext(snippet) {
  return classifyContextWords(snippet || "");
}

function findSnippetInText(text, rawCitation, normalizedValue) {
  const source = String(text || "");
  if (!source.trim()) return null;
  const lower = source.toLowerCase();
  const probes = [String(rawCitation || ""), String(normalizedValue || ""), String(rawCitation || "").replace(/^ordinance\s+/i, ""), String(rawCitation || "").replace(/^rule\s+/i, "")]
    .map((item) => item.trim())
    .filter(Boolean);
  let idx = -1;
  let matched = "";
  for (const probe of probes) {
    const at = lower.indexOf(probe.toLowerCase());
    if (at >= 0) {
      idx = at;
      matched = probe;
      break;
    }
  }
  if (idx < 0) return null;
  const start = Math.max(0, idx - 140);
  const end = Math.min(source.length, idx + matched.length + 140);
  return normalizeText(source.slice(start, end));
}

function extractSnippet(textBlocks, rawCitation, normalizedValue) {
  for (const block of textBlocks || []) {
    const snippet = findSnippetInText(block, rawCitation, normalizedValue);
    if (snippet) return snippet;
  }
  return "";
}

function makePatternKey(ref) {
  return `${ref.referenceType}|${ref.rawValue}|${ref.normalizedValue}|${ref.rootCause}`;
}

function issueAppearanceLikely(contextCounts, rootCauseCounts) {
  const ordinance = contextCounts.find((item) => item.key === "likely_ordinance_wording")?.count || 0;
  const rules = contextCounts.find((item) => item.key === "likely_rules_wording")?.count || 0;
  const ambiguous = contextCounts.find((item) => item.key === "mixed_ambiguous_wording")?.count || 0;
  const none = contextCounts.find((item) => item.key === "no_useful_context")?.count || 0;
  const rootTop = rootCauseCounts[0]?.key || "";
  if (ordinance > rules && ordinance > ambiguous && ordinance > none) return "ordinance citation ambiguity";
  if (rules > ordinance && rules > ambiguous) return "rules/ordinance cross-context confusion";
  if (rootTop === "not_found" && none >= Math.max(ordinance, rules)) return "true not-found";
  return "mixed";
}

function recommendedPosture(appearance) {
  if (appearance === "ordinance citation ambiguity") return "possible_manual_context_fix_but_no_auto_apply";
  if (appearance === "rules/ordinance cross-context confusion") return "escalate_to_legal_context_review";
  if (appearance === "true not-found") return "keep_blocked";
  return "escalate_to_legal_context_review";
}

export function buildReviewerLegalEvidencePackets(rows, docSourceById) {
  const normalizedRows = parseRows(rows);
  const blockedRows = normalizedRows.filter(
    (row) =>
      row.blocked37xFamily.some((family) => UNSAFE_37X.has(family)) ||
      row.unresolvedTriageBuckets.includes("unsafe_37x_structural_block") ||
      row.unresolvedTriageBuckets.includes("cross_context_ambiguous")
  );
  const groups = new Map();
  for (const row of blockedRows) {
    const key = row.batchKey || `doc:${row.documentId}`;
    const list = groups.get(key) || [];
    list.push(row);
    groups.set(key, list);
  }

  const packets = Array.from(groups.entries())
    .map(([batchKey, docs]) => {
      const refs = docs.flatMap((doc) => doc.unresolvedReferences);
      const patternCounts = countBy(refs.map(makePatternKey));
      const majorPatterns = patternCounts.slice(0, 6).map((patternRow) => {
        const [referenceType, rawValue, normalizedValue, rootCause] = String(patternRow.key).split("|");
        const snippets = [];
        for (const doc of docs) {
          const source = docSourceById.get(doc.documentId);
          if (!source) continue;
          const snippet = extractSnippet(source.textBlocks || [], rawValue, normalizedValue);
          if (!snippet) continue;
          const context = classifyEvidenceContext(snippet);
          snippets.push({
            documentId: doc.documentId,
            title: source.title || doc.title,
            rawCitation: rawValue,
            normalizedValue,
            referenceType,
            rootCause,
            localTextSnippet: snippet,
            nearbyWordsSuggestingContext: context.nearbyWords,
            contextClass: context.contextClass
          });
          if (snippets.length >= 6) break;
        }
        const dedupGroups = countBy(
          snippets.map((item) => `${item.contextClass}|${safeLower(item.localTextSnippet).slice(0, 100)}`)
        ).map((row) => ({
          key: row.key,
          count: row.count
        }));
        return {
          pattern: {
            referenceType,
            rawCitation: rawValue,
            normalizedValue,
            rootCause
          },
          count: patternRow.count,
          representativeSnippets: snippets,
          deduplicatedContextPatternGroups: dedupGroups
        };
      });
      const allSnippets = majorPatterns.flatMap((pattern) => pattern.representativeSnippets);
      const contextCounts = countBy(allSnippets.map((item) => item.contextClass));
      const rootCauseCounts = countBy(refs.map((item) => item.rootCause || "unknown"));
      const blocked37xFamily = Array.from(new Set(docs.flatMap((doc) => doc.blocked37xFamily))).sort();
      const unresolvedTriageBuckets = Array.from(new Set(docs.flatMap((doc) => doc.unresolvedTriageBuckets))).sort();
      const appearance = issueAppearanceLikely(contextCounts, rootCauseCounts);
      const posture = recommendedPosture(appearance);
      return {
        batchKey,
        docCount: docs.length,
        blocked37xFamily,
        unresolvedTriageBuckets,
        dominantBlockerPattern: countBy(docs.flatMap((doc) => doc.blockers))[0]?.key || "unresolved_references_above_threshold",
        dominantCitationFamily: countBy(docs.flatMap((doc) => doc.recurringCitationFamily))[0]?.key || blocked37xFamily[0] || "",
        sampleDocs: docs
          .slice()
          .sort((a, b) => a.title.localeCompare(b.title) || a.documentId.localeCompare(b.documentId))
          .slice(0, 8)
          .map((doc) => ({ documentId: doc.documentId, title: doc.title })),
        topRepeatedUnresolvedCitationPatterns: majorPatterns.map((pattern) => ({
          ...pattern.pattern,
          count: pattern.count
        })),
        patternEvidence: majorPatterns,
        topRawCitationStringsByCount: countBy(refs.map((item) => item.rawValue).filter(Boolean)).slice(0, 12),
        topNormalizedValuesByCount: countBy(refs.map((item) => item.normalizedValue).filter(Boolean)).slice(0, 12),
        topRootCausesByCount: rootCauseCounts.slice(0, 8),
        deduplicatedContextPatternGroups: countBy(allSnippets.map((item) => item.contextClass)).map((row) => ({
          contextClass: row.key,
          count: row.count
        })),
        contextSummary: {
          ordinanceLike: contextCounts.find((row) => row.key === "likely_ordinance_wording")?.count || 0,
          rulesLike: contextCounts.find((row) => row.key === "likely_rules_wording")?.count || 0,
          ambiguous: contextCounts.find((row) => row.key === "mixed_ambiguous_wording")?.count || 0,
          noUsefulContext: contextCounts.find((row) => row.key === "no_useful_context")?.count || 0
        },
        issueAppearanceLikely: appearance,
        recommendedReviewerPosture: posture,
        reviewerNotesTemplate: [
          "Context decision (ordinance/rules/ambiguous):",
          "Evidence snippet references reviewed:",
          "Decision: keep blocked / escalate / possible manual context fix (no auto-apply):",
          "Reviewer notes:"
        ],
        recommendedReviewerQuestions: [
          "Is this citation intended as ordinance or rules?",
          "Is there enough nearby context to disambiguate safely?",
          "Should this remain blocked?"
        ],
        reviewChecklist: [
          "Review representative snippets before making any manual recommendation.",
          "Document context decision with source-backed snippet references.",
          "Do not auto-resolve 37.3/37.7/37.9; remain read-only."
        ]
      };
    })
    .sort((a, b) => b.docCount - a.docCount || a.batchKey.localeCompare(b.batchKey));

  const contextPairCounts = countBy(
    packets.flatMap((packet) =>
      packet.topRepeatedUnresolvedCitationPatterns.map((pattern) => `${pattern.rawCitation}|${packet.issueAppearanceLikely}`)
    )
  ).map((row) => {
    const [citation, context] = String(row.key).split("|");
    return { citation, context, count: row.count };
  });
  const byOrdinanceEvidence = [...packets].sort(
    (a, b) => b.contextSummary.ordinanceLike - a.contextSummary.ordinanceLike || b.docCount - a.docCount
  );
  const byRulesEvidence = [...packets].sort((a, b) => b.contextSummary.rulesLike - a.contextSummary.rulesLike || b.docCount - a.docCount);
  const ambiguous = packets.filter((packet) => packet.contextSummary.ambiguous + packet.contextSummary.noUsefulContext >= packet.contextSummary.ordinanceLike + packet.contextSummary.rulesLike);
  const suggestedReviewOrder = [...packets].sort((a, b) => {
    const aScore = a.contextSummary.ordinanceLike + a.contextSummary.rulesLike - a.contextSummary.ambiguous - a.contextSummary.noUsefulContext;
    const bScore = b.contextSummary.ordinanceLike + b.contextSummary.rulesLike - b.contextSummary.ambiguous - b.contextSummary.noUsefulContext;
    return bScore - aScore || b.docCount - a.docCount || a.batchKey.localeCompare(b.batchKey);
  });

  return {
    packets,
    summary: {
      blockedBatchCount: packets.length,
      blockedDocCount: packets.reduce((sum, packet) => sum + packet.docCount, 0),
      mostRepeatedBlockedCitationContextPairs: contextPairCounts.slice(0, 20),
      batchesWithStrongestOrdinanceLikeEvidence: byOrdinanceEvidence.slice(0, 10).map((packet) => ({
        batchKey: packet.batchKey,
        docCount: packet.docCount,
        ordinanceLike: packet.contextSummary.ordinanceLike
      })),
      batchesWithStrongestRulesLikeEvidence: byRulesEvidence.slice(0, 10).map((packet) => ({
        batchKey: packet.batchKey,
        docCount: packet.docCount,
        rulesLike: packet.contextSummary.rulesLike
      })),
      batchesTooAmbiguousForManualContextCorrection: ambiguous.map((packet) => ({
        batchKey: packet.batchKey,
        docCount: packet.docCount,
        ambiguous: packet.contextSummary.ambiguous,
        noUsefulContext: packet.contextSummary.noUsefulContext
      })),
      suggestedHumanReviewOrderUsingContextEvidence: suggestedReviewOrder.map((packet) => ({
        batchKey: packet.batchKey,
        docCount: packet.docCount,
        issueAppearanceLikely: packet.issueAppearanceLikely,
        recommendedReviewerPosture: packet.recommendedReviewerPosture
      }))
    }
  };
}
