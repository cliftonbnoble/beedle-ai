import type { AdminIngestionMetadataUpdate } from "@beedle/shared";

type ReferenceInputs = { indexCodes: string; rulesSections: string; ordinanceSections: string };

const parseList = (value: string) => value.split(",").map(item => item.trim()).filter(Boolean);

/** An unchanged legacy array must not become a replacement mutation. */
export function reviewerReferencePatch(current: ReferenceInputs, baseline: ReferenceInputs) {
  const patch: Pick<AdminIngestionMetadataUpdate, "index_codes" | "rules_sections" | "ordinance_sections"> = {};
  for (const [input, field] of [
    ["indexCodes", "index_codes"], ["rulesSections", "rules_sections"], ["ordinanceSections", "ordinance_sections"]
  ] as const) {
    const next = parseList(current[input]);
    if (JSON.stringify(next) !== JSON.stringify(parseList(baseline[input]))) patch[field] = next;
  }
  return patch;
}
