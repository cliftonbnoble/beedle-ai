export function json(data: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json");
  return new Response(JSON.stringify(data), { ...init, headers });
}

export async function readJson(request: Request): Promise<unknown> {
  const contentType = request.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    throw new Error("content-type must be application/json");
  }
  return request.json();
}

export function toErrorResponse(error: unknown): Response {
  const message = error instanceof Error ? error.message : "Unexpected error";
  return json({ error: message }, { status: 400 });
}
