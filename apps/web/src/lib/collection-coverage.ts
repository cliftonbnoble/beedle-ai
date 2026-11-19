import type { CollectionCoverage } from "@beedle/shared";

const number = new Intl.NumberFormat("en-US");

export function collectionCoverageText({ environment, searchableDocuments, inventoryDocuments }: CollectionCoverage) {
  const searchable = number.format(searchableDocuments);
  const documents = searchableDocuments === 1 ? "document" : "documents";
  if (environment === "preview") return `This preview contains ${searchable} searchable ${documents}.`;
  const excluded = inventoryDocuments - searchableDocuments;
  if (excluded === 0) return `Search covers the full collection of ${searchable} ${documents}.`;
  return `Search covers ${searchable} ${documents}. ${number.format(excluded)} ${excluded === 1 ? "is" : "are"} outside this release.`;
}
