import { z } from "zod";
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
    const response: unknown = await withinProviderDeadline(options, Math.min(options.timeoutMs ?? EMBEDDING_TIMEOUT_MS, EMBEDDING_TIMEOUT_MS), async () => {
      env.TELEMETRY?.count("embedding_texts_submitted",inputs.length);
      // Trusted deployment model name bridges the SDK's static model registry.
      // Provider results remain unknown and are validated below.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      return env.AI!.run(env.AI_EMBEDDING_MODEL as keyof AiModels, { text });
    }
    );
    // Capacity checks precede Zod traversal: malformed arrays must not turn a
    // bounded binding operation into an unbounded synchronous validation pass.
    if(!response || typeof response!=='object' || Array.isArray(response))return failed("invalid_response");
    const data:unknown=Reflect.get(response,'data'),shape:unknown=Reflect.get(response,'shape');
    if(!Array.isArray(data)||data.length!==inputs.length || (shape!==undefined&&(!Array.isArray(shape)||shape.length!==2)))return failed("invalid_response");
    const envelope=z.object({data:z.array(z.unknown()).length(inputs.length),shape:z.tuple([z.number().int().positive(),z.number().int().positive()]).optional()}).safeParse({data,shape});
    if(!envelope.success || (envelope.data.shape && (envelope.data.shape[0]!==inputs.length || envelope.data.shape[1]!==dimensions))) return failed("invalid_response");
    const vectorSchema=z.array(z.number().finite()).length(dimensions);
    const outcomes=envelope.data.data.map((value):EmbeddingOutcome=>{
      if(!Array.isArray(value)||value.length!==dimensions)return {vector:null,reason:"invalid_response"};
      const vector=vectorSchema.safeParse(value);
      return vector.success ? {vector:vector.data,reason:null} : {vector:null,reason:"invalid_response"};
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
