import { randomUUID } from "node:crypto";

// Never fetch a newer revision to authorize an older computed correction.
export function reviewPatch(payload, expectedRevision, mutationId = randomUUID()) {
  if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 1) {
    throw new Error("Candidate has no captured document revision. Regenerate and review the candidate snapshot before writing.");
  }
  return {...payload,expectedRevision,mutationId};
}
