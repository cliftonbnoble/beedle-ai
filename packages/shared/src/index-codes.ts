import indexCodeOptions from "./index-codes.json";

export type CanonicalIndexCodeOption = {
  code: string;
  description: string;
  ordinance: string;
  rules: string;
};

export const canonicalIndexCodeOptions: readonly CanonicalIndexCodeOption[] = indexCodeOptions;
