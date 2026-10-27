import { embeddingDimensions } from "./vector-profile";
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

export async function embedBatchWithOutcome(env: Env, inputs: string[], options: EmbeddingOptions = {}): Promise<EmbeddingOutcome[]> {
  if(!inputs.length||inputs.length>16) throw new Error("Embedding batch must contain 1–16 texts");
  env.TELEMETRY?.count("embedding_texts_requested",inputs.length);
  const failed=(reason:"unavailable"|"timeout"|"cancelled"|"invalid_response"):EmbeddingOutcome[]=>{env.TELEMETRY?.tag("embedding_result",reason);return inputs.map(()=>({vector:null,reason}));};
  if (!env.AI) return failed("unavailable");
  if (options.signal?.aborted) return failed("cancelled");
  if ((options.deadlineAt ?? Infinity) <= Date.now()) return failed("timeout");
  const dimensions=embeddingDimensions(env);
  await reserveComputeBudget(env, "embedding",inputs.length);
  const text = inputs.map(input=>options.isQuery ? `${BGE_QUERY_INSTRUCTION}${input}` : input);
  try {
    // This binding has no AbortSignal. Late settlement is observed by the race;
    // its reserved texts remain charged even after this caller stops waiting.
    const response = await withinProviderDeadline(options, Math.min(options.timeoutMs ?? EMBEDDING_TIMEOUT_MS, EMBEDDING_TIMEOUT_MS), async () => {env.TELEMETRY?.count("embedding_texts_submitted",inputs.length);return env.AI!.run(env.AI_EMBEDDING_MODEL as keyof AiModels, { text });}
    ) as { data?: unknown };
    if(!Array.isArray(response?.data)||response.data.length!==inputs.length) return failed("invalid_response");
    const outcomes=response.data.map((vector):EmbeddingOutcome=>{
      if(!Array.isArray(vector)||vector.length!==dimensions||!vector.every(value=>typeof value==='number'&&Number.isFinite(value))) return {vector:null,reason:"invalid_response"};
      return {vector,reason:null};
    });
    env.TELEMETRY?.tag("embedding_result",outcomes.some(outcome=>outcome.reason)?"invalid_response":"ok");return outcomes;
  } catch (error) {
    if (isAiBindingUnavailableError(error)) return failed("unavailable");
    if (error instanceof ProviderOperationError && (error.reason === "timeout" || error.reason === "cancelled")) return failed(error.reason);
    throw error;
  }
}
export async function embedWithOutcome(env:Env,input:string,options:EmbeddingOptions={}) {
  return (await embedBatchWithOutcome(env,[input],options))[0]!;
}

// Compatibility for passage/job callers; search consumes the explicit outcome.
export async function embed(env: Env, input: string, options?: EmbeddingOptions): Promise<number[] | null> {
  const outcome = await embedWithOutcome(env, input, options);
  if(outcome.reason)env.TELEMETRY?.tag("embedding_result",outcome.reason);
  return outcome.vector;
}
