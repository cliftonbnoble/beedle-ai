import type { Env } from "../lib/types";
import { ProviderOperationError, withinProviderDeadline, type OperationContext } from "../lib/provider-operation";
import { reserveComputeBudget } from "./compute-budget";

const EMBEDDING_TIMEOUT_MS = 15000;
function isAiBindingUnavailableError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return /needs to be run remotely/i.test(message);
}
export const BGE_QUERY_INSTRUCTION = "Represent this sentence for searching relevant passages: ";
type EmbeddingOptions = OperationContext & { isQuery?: boolean; timeoutMs?: number };
export type EmbeddingOutcome = { vector: number[]; reason: null } | { vector: null; reason: "unavailable" | "timeout" | "cancelled" | "invalid_response" };

export async function embedWithOutcome(env: Env, input: string, options: EmbeddingOptions = {}): Promise<EmbeddingOutcome> {
  if (!env.AI) return { vector: null, reason: "unavailable" };
  if (options.signal?.aborted) return { vector: null, reason: "cancelled" };
  if ((options.deadlineAt ?? Infinity) <= Date.now()) return { vector: null, reason: "timeout" };
  await reserveComputeBudget(env, "embedding");
  const text = options.isQuery ? `${BGE_QUERY_INSTRUCTION}${input}` : input;
  try {
    // This binding has no AbortSignal. Stop waiting at the deadline, retain the
    // reservation, and attach rejection handlers via the race for late failures.
    const response = await withinProviderDeadline(options, Math.min(options.timeoutMs ?? EMBEDDING_TIMEOUT_MS, EMBEDDING_TIMEOUT_MS), async () =>
      env.AI!.run(env.AI_EMBEDDING_MODEL as keyof AiModels, { text: [text] })
    ) as { data?: unknown };
    const vector = Array.isArray(response?.data) ? response.data[0] : null;
    if (!Array.isArray(vector) || !vector.length || vector.length > 4096 || !vector.every(value => typeof value === "number" && Number.isFinite(value))) {
      return { vector: null, reason: "invalid_response" };
    }
    return { vector, reason: null };
  } catch (error) {
    if (isAiBindingUnavailableError(error)) return { vector: null, reason: "unavailable" };
    if (error instanceof ProviderOperationError && (error.reason === "timeout" || error.reason === "cancelled")) return { vector: null, reason: error.reason };
    throw error;
  }
}

// Compatibility for passage/job callers; search consumes the explicit outcome.
export async function embed(env: Env, input: string, options?: EmbeddingOptions): Promise<number[] | null> {
  const outcome = await embedWithOutcome(env, input, options);
  if (outcome.reason && outcome.reason !== "unavailable") console.warn(JSON.stringify({event:"embedding_degraded",reason:outcome.reason}));
  return outcome.vector;
}
