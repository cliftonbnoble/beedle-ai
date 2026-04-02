export const CASE_ASSISTANT_GUARDRAILS = [
  "Guidance is advisory and not a binding legal determination.",
  "Statements marked as inference indicate probabilistic reasoning from retrieved authorities.",
  "If retrieval support is weak or mixed, confidence must be downgraded and uncertainty stated explicitly."
] as const;

export const CASE_SECTION_PRIORITY = [
  "index codes",
  "rules",
  "ordinance",
  "conclusions of law",
  "reasoning",
  "findings"
] as const;

export const OUTCOME_KEYWORDS = {
  grant: ["grant", "approved", "approve", "allowed", "sustain"],
  deny: ["deny", "denied", "rejected", "dismissed"],
  partial: ["partial", "partially", "limited grant", "modified"]
} as const;

export const DRAFT_CONCLUSIONS_GUARDRAILS = [
  "Drafting output is advisory and must be reviewed by a judge/ALJ.",
  "Conclusions of Law text must remain grounded in retrieved authorities and cited anchors.",
  "When support is sparse or conflicting, the draft must explicitly state limitations and uncertainty."
] as const;

export const TEMPLATE_GUARDRAILS = [
  "Templates are scaffolds and must be reviewed/edited by a judge or ALJ.",
  "Do not insert case-specific facts unless they are explicitly provided by the user.",
  "Do not invent authorities or citations."
] as const;

export const CANONICAL_DECISION_SECTIONS = [
  "Introduction",
  "Findings of Fact",
  "Related Case / Procedural History",
  "Conclusions of Law",
  "Order"
] as const;
