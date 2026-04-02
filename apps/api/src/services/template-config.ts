import {
  taxonomyConfigInspectResponseSchema,
  taxonomyConfigSchema,
  type TaxonomyConfig,
  type TaxonomyResolveResponse
} from "@beedle/shared";

type CaseTypeEntry = TaxonomyConfig["case_types"][number];

const RAW_TAXONOMY_CONFIG: TaxonomyConfig = {
  version: "1.0.0",
  default_case_type_id: "general",
  canonical_sections: [
    {
      name: "Introduction",
      purpose: "Frame the dispute, requested relief, and legal context.",
      placeholder: "[Briefly identify the parties, requested relief, and governing forum authority.]",
      base_prompts: ["What relief is being requested and under what authority?", "What is the core dispute the tribunal must resolve?"],
      example_structure: ["[Short structured paragraph 1]", "[Short structured paragraph 2]"]
    },
    {
      name: "Findings of Fact",
      purpose: "Record the material facts that were established.",
      placeholder: "[Insert numbered factual findings supported by the record. Avoid legal conclusions in this section.]",
      base_prompts: [
        "What facts were proven by testimony/documents?",
        "Which facts are disputed and how were conflicts resolved?",
        "Which findings are essential to the legal test?"
      ],
      example_structure: ["1. [Fact finding statement].", "2. [Fact finding statement].", "3. [Fact finding statement]."]
    },
    {
      name: "Related Case / Procedural History",
      purpose: "Document procedural timeline and prior related determinations.",
      placeholder: "[Summarize procedural posture, prior related rulings, and relevant filing/hearing chronology.]",
      base_prompts: [
        "What prior actions, notices, or related cases are material?",
        "What hearings, filings, or procedural milestones matter here?"
      ],
      example_structure: ["[Short structured paragraph 1]", "[Short structured paragraph 2]"]
    },
    {
      name: "Conclusions of Law",
      purpose: "Apply governing legal standards to established findings.",
      placeholder: "[Apply each controlling legal standard to the findings. State criterion-by-criterion determinations.]",
      base_prompts: ["What rules/ordinances control this determination?", "How does each legal element map to specific findings?"],
      example_structure: ["A. [Identify controlling legal standard].", "B. [Apply standard to finding].", "C. [State legal conclusion]."]
    },
    {
      name: "Order",
      purpose: "State operative ruling language and enforceable directives.",
      placeholder: "[State final disposition, scope of relief, conditions, and effective date.]",
      base_prompts: ["What is the final disposition (grant/deny/partial)?", "What conditions, deadlines, or directives should be included?"],
      example_structure: ["IT IS ORDERED that [grant/deny/partial].", "Any conditions: [condition text].", "Effective date: [date]."]
    }
  ],
  template_defaults: {
    default_template_mode: "guided_scaffold",
    guardrails: [
      "Templates are scaffolds and must be reviewed/edited by a judge or ALJ.",
      "Do not insert case-specific facts unless they are explicitly provided by the user.",
      "Do not invent authorities or citations."
    ]
  },
  issue_tag_catalog: [
    "variance",
    "lot coverage",
    "notice",
    "compliance",
    "mitigation",
    "penalty",
    "eligibility",
    "procedural history"
  ],
  case_types: [
    {
      id: "zoning_variance",
      label: "Zoning Variance",
      description: "Scaffold for variance relief requests involving ordinance constraints, notice, and neighborhood impact findings.",
      aliases: ["zoning variance", "variance", "land_use_variance"],
      focus_prompts: [
        "Identify the requested variance and statutory authority.",
        "Tie each required variance factor to explicit findings.",
        "Address mitigation conditions and enforceability."
      ],
      default_issue_tags: ["variance", "lot coverage", "mitigation"],
      template_section_hints: {
        "Findings of Fact": [
          "Property characteristics and current zoning constraints",
          "Evidence regarding hardship/practical difficulty",
          "Neighborhood impact evidence and mitigation"
        ],
        "Conclusions of Law": [
          "Apply ordinance variance criteria in sequence",
          "State whether each criterion is satisfied and why"
        ],
        Order: ["Specify grant/deny/partial relief and any conditions of approval"]
      },
      index_code_mappings: ["IC-104"],
      rules_hints: ["Rule 3.1"],
      ordinance_hints: ["Ordinance 77-19"]
    },
    {
      id: "licensing_enforcement",
      label: "Licensing Enforcement",
      description: "Scaffold for license enforcement actions, compliance findings, and penalty determinations.",
      aliases: ["licensing", "enforcement", "license_enforcement"],
      focus_prompts: [
        "Identify alleged violations and governing rules.",
        "Separate proven vs unproven allegations.",
        "Explain penalty proportionality and statutory basis."
      ],
      default_issue_tags: ["compliance", "penalty"],
      template_section_hints: {
        "Findings of Fact": ["Inspection/investigation timeline", "Observed conduct and documentary evidence", "Respondent compliance history"],
        "Conclusions of Law": ["Map findings to each charged provision", "State basis for any sustained or dismissed count"],
        Order: ["Set sanctions, deadlines, and corrective obligations"]
      },
      index_code_mappings: [],
      rules_hints: [],
      ordinance_hints: []
    },
    {
      id: "benefits_eligibility",
      label: "Benefits Eligibility",
      description: "Scaffold for eligibility determinations based on factual criteria and governing benefit rules.",
      aliases: ["benefits", "eligibility", "benefit_eligibility"],
      focus_prompts: [
        "List each eligibility criterion and relevant evidence.",
        "Resolve credibility/document conflicts directly.",
        "State effective date and scope of any award/denial."
      ],
      default_issue_tags: ["eligibility"],
      template_section_hints: {
        "Findings of Fact": ["Claimant background and timeline", "Material evidence and credibility findings", "Eligibility-factor specific facts"],
        "Conclusions of Law": ["Apply each legal criterion to findings", "Identify any disqualifying or partially satisfying factors"],
        Order: ["Grant, deny, or modify benefits with dates and rationale"]
      },
      index_code_mappings: [],
      rules_hints: [],
      ordinance_hints: []
    },
    {
      id: "general",
      label: "General",
      description: "Generic structured scaffold for decisions following canonical section requirements.",
      aliases: ["other", "default", "general_case"],
      focus_prompts: [
        "Define core issue and requested relief clearly.",
        "Separate factual findings from legal conclusions.",
        "Ensure order language is precise and enforceable."
      ],
      default_issue_tags: [],
      template_section_hints: {},
      index_code_mappings: [],
      rules_hints: [],
      ordinance_hints: []
    }
  ]
};

function normalizeCaseType(input: string): string {
  return input.toLowerCase().trim().replace(/\s+/g, "_");
}

// TODO(taxonomy-config): replace static config with D1-backed admin-managed configuration + change audit trail.
// TODO(taxonomy-config): add adjudicator-reviewed calibration metadata per case type for confidence tuning.
const ACTIVE_CONFIG = taxonomyConfigSchema.parse(RAW_TAXONOMY_CONFIG);

function findCaseTypeByIdOrAlias(caseType: string) {
  const normalized = normalizeCaseType(caseType);
  for (const entry of ACTIVE_CONFIG.case_types) {
    if (normalizeCaseType(entry.id) === normalized) {
      return { entry, matchType: "id" as const };
    }
    if (entry.aliases.some((alias) => normalizeCaseType(alias) === normalized)) {
      return { entry, matchType: "alias" as const };
    }
  }
  return null;
}

export function getActiveTaxonomyConfig(): TaxonomyConfig {
  return ACTIVE_CONFIG;
}

export function inspectActiveTaxonomyConfig() {
  return taxonomyConfigInspectResponseSchema.parse({
    config: ACTIVE_CONFIG,
    stats: {
      case_type_count: ACTIVE_CONFIG.case_types.length,
      canonical_section_count: ACTIVE_CONFIG.canonical_sections.length,
      issue_tag_count: ACTIVE_CONFIG.issue_tag_catalog.length
    }
  });
}

export function resolveCaseTypeTemplate(caseType: string): {
  caseType: CaseTypeEntry;
  resolution: TaxonomyResolveResponse;
} {
  const found = findCaseTypeByIdOrAlias(caseType);
  if (found) {
    return {
      caseType: found.entry,
      resolution: {
        requested_case_type: caseType,
        resolved_case_type_id: found.entry.id,
        resolved_case_type_label: found.entry.label,
        match_type: found.matchType,
        warnings: []
      }
    };
  }

  const fallback =
    ACTIVE_CONFIG.case_types.find((item) => item.id === ACTIVE_CONFIG.default_case_type_id) ?? ACTIVE_CONFIG.case_types[0];
  if (!fallback) {
    throw new Error("taxonomy config has no fallback case type");
  }

  return {
    caseType: fallback,
    resolution: {
      requested_case_type: caseType,
      resolved_case_type_id: fallback.id,
      resolved_case_type_label: fallback.label,
      match_type: "fallback",
      warnings: [
        `Unknown case_type "${caseType}". Falling back to "${fallback.id}".`,
        "Provide a configured case_type id or alias for more specific template guidance."
      ]
    }
  };
}

export function validateTaxonomyConfig(input: unknown) {
  return taxonomyConfigSchema.parse(input);
}
