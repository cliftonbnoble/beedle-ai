import type { Env } from "../lib/types";

// Outbound Workers AI calls must be bounded so a stalled embedding cannot hang a search,
// ingest, or backfill request (LLM-02). The AI binding has no AbortSignal, so race the call
// against a timeout and degrade to null on timeout. Every caller already handles a null
// embedding (vector search is skipped; the chunk is simply not marked vector-active), so a
// timeout degrades gracefully instead of stalling the request.
const EMBEDDING_TIMEOUT_MS = 15000;

// In local `wrangler dev` the AI binding object exists (so `env.AI` is truthy) but invoking it throws
// "Binding AI needs to be run remotely". There is nothing to retry or fix — the embedding simply cannot
// be produced in that environment — so we treat it like a missing binding and degrade to null. In
// production `env.AI.run` succeeds, so this never matches; genuine AI errors still surface.
function isAiBindingUnavailableError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return /needs to be run remotely/i.test(message);
}

export async function embed(env: Env, input: string): Promise<number[] | null> {
  if (!env.AI) {
    return null;
  }

  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<null>((resolve) => {
    timeout = setTimeout(() => resolve(null), EMBEDDING_TIMEOUT_MS);
  });

  try {
    const response = (await Promise.race([
      env.AI.run(env.AI_EMBEDDING_MODEL as keyof AiModels, { text: [input] }),
      timeoutPromise
    ])) as { data?: number[][]; shape?: number[] } | null;

    if (!response?.data?.[0]) {
      return null;
    }

    return response.data[0];
  } catch (error) {
    if (isAiBindingUnavailableError(error)) {
      return null;
    }
    throw error;
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}
