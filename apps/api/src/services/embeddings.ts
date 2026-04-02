import type { Env } from "../lib/types";

export async function embed(env: Env, input: string): Promise<number[] | null> {
  if (!env.AI) {
    return null;
  }

  const response = await env.AI.run(env.AI_EMBEDDING_MODEL as keyof AiModels, { text: [input] }) as {
    data?: number[][];
    shape?: number[];
  };

  if (!response?.data?.[0]) {
    return null;
  }

  return response.data[0];
}
