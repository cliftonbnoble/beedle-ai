import { search } from "../services/search";
import { readJson, json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleSearch(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await search(env, payload, {session:true});
    return json(result,{headers:{"cache-control":"private, no-store"}});
  } catch (error) {
    return toErrorResponse(error);
  }
}
