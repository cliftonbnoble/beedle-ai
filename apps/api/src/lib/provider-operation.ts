import type {RequestTelemetry} from "./telemetry";
import { z } from "zod";

export type OperationContext = { signal?: AbortSignal; deadlineAt?: number; telemetry?: RequestTelemetry };
export type ProviderFailure = "timeout" | "cancelled" | "response_too_large" | "invalid_response" | "truncated" | "provider_rejected" | "provider_rate_limited" | "unconfigured" | "network_failure";
export class ProviderOperationError extends Error {
  constructor(public readonly reason: ProviderFailure, public readonly status?: number) {
    super(`Provider operation failed: ${reason}${status ? ` (${status})` : ""}`);
    this.name = "ProviderOperationError";
  }
}

// Covers connection, headers, body consumption and validation. The explicit race
// also bounds bindings/mocks that cannot actually interrupt their underlying I/O.
export async function withinProviderDeadline<T>(context: OperationContext, timeoutMs: number, operation: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const remaining = Math.min(timeoutMs, (context.deadlineAt ?? Infinity) - Date.now());
  if (context.signal?.aborted) throw new ProviderOperationError("cancelled");
  if (remaining <= 0) throw new ProviderOperationError("timeout");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onCancel: (() => void) | undefined;
  const interrupted = new Promise<never>((_, reject) => {
    const stop = (reason: "timeout" | "cancelled") => {
      controller.abort(reason);
      reject(new ProviderOperationError(reason));
    };
    timer = setTimeout(() => stop("timeout"), remaining);
    onCancel = () => stop("cancelled");
    context.signal?.addEventListener("abort", onCancel, { once: true });
  });
  try { return await Promise.race([operation(controller.signal), interrupted]); }
  finally {
    if (timer) clearTimeout(timer);
    if (onCancel) context.signal?.removeEventListener("abort", onCancel);
  }
}

async function boundedBody(response: Response, limit: number, signal: AbortSignal): Promise<string> {
  const advertised = Number(response.headers.get("content-length"));
  if (advertised > limit) {
    void response.body?.cancel().catch(() => {});
    throw new ProviderOperationError("response_too_large");
  }
  if (!response.body) throw new ProviderOperationError("invalid_response");
  const reader = response.body.getReader();
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", cancel, { once: true });
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let bytes = 0;
  let text = "";
  try {
    while (true) {
      if (signal.aborted) throw new ProviderOperationError(signal.reason === "cancelled" ? "cancelled" : "timeout");
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > limit) { cancel(); throw new ProviderOperationError("response_too_large"); }
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();
    return text;
  } finally { signal.removeEventListener("abort", cancel); reader.releaseLock(); }
}

const textSchema = z.string().max(40_000);
const completionSchema = z.object({
  choices: z.array(z.object({
    finish_reason: z.string().nullable().optional(),
    message: z.object({ content: z.union([textSchema, z.array(z.union([textSchema, z.object({ type: z.literal("text").optional(), text: textSchema })])).max(64)]) })
  })).min(1).max(16)
});

async function completeWithinBounds(url: string, init: RequestInit, context: OperationContext = {}, timeoutMs = 18_000): Promise<string> {
  try {
    return await withinProviderDeadline(context, timeoutMs, async signal => {
      const response = await fetch(url, { ...init, signal });
      context.telemetry?.providerStatus(response.status);
      // Error text is never logged or returned; bound even that discard path.
      const body = await boundedBody(response, response.ok ? 128 * 1024 : 8 * 1024, signal);
      if (!response.ok) throw new ProviderOperationError(response.status===429?"provider_rate_limited":"provider_rejected", response.status);
      let value: unknown;
      try { value = JSON.parse(body); } catch { throw new ProviderOperationError("invalid_response"); }
      const parsed = completionSchema.safeParse(value);
      if (!parsed.success) throw new ProviderOperationError("invalid_response");
      const choice = parsed.data.choices[0]!;
      if (choice.finish_reason === "length") throw new ProviderOperationError("truncated");
      if (choice.finish_reason && choice.finish_reason !== "stop") throw new ProviderOperationError("invalid_response");
      const content = choice.message.content;
      const answer = (typeof content === "string" ? content : content.map(part => typeof part === "string" ? part : part.text).join("\n")).trim();
      if (!answer || answer.length > 40_000) throw new ProviderOperationError("invalid_response");
      return answer;
    });
  } catch (error) {
    if (error instanceof ProviderOperationError) throw error;
    throw new ProviderOperationError("network_failure");
  }
}

export async function boundedCompletion(url:string,init:RequestInit,context:OperationContext={},timeoutMs=18_000):Promise<string>{
 const run=()=>completeWithinBounds(url,init,context,timeoutMs);
 try{const result=context.telemetry?await context.telemetry.measure('llm',run):await run();context.telemetry?.tag('llm_result','ok');return result;}
 catch(error){context.telemetry?.tag('llm_result',error instanceof ProviderOperationError?error.reason:'network_failure');throw error;}
}
