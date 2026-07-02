import type { SearchResponse } from "@beedle/shared";

export type SearchResultRow = SearchResponse["results"][number];

function normalize(value: string) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

export function friendlySectionLabel(value: string) {
  const normalized = normalize(value);
  if (!normalized) return "Matched passage";
  if (/conclusions?_of_law|authority_discussion|analysis_reasoning/.test(normalized)) return "Conclusions of Law";
  if (/findings?_of_fact|fact_findings/.test(normalized)) return "Findings of Fact";
  if (/procedural|history|background/.test(normalized)) return "Procedural History";
  if (/order|disposition|holding/.test(normalized)) return "Order";
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}
