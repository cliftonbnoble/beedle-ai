import type { Env } from "../lib/types";
import { createBackfillJob } from "./maintenance-jobs";
// Compatibility entry point: callers receive a durable plan, never run a corpus
// inference loop in the HTTP request. Use the job client to drive/poll progress.
export async function backfillRetrievalVectors(env:Env,input:unknown) {
  return createBackfillJob(env,"vector_backfill",input);
}
