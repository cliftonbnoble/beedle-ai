export function json(data: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json");
  return new Response(JSON.stringify(data), { ...init, headers });
}

export const DEFAULT_JSON_BODY_MAX_BYTES = 1 * 1024 * 1024;

export class RequestBodyTooLargeError extends Error {
  constructor(readonly maxBytes: number) {
    super(`Request body exceeds the ${maxBytes}-byte limit.`);
    this.name = "RequestBodyTooLargeError";
  }
}

// Only errors constructed in this module may expose their message to an API client. Service errors
// often contain provider, SQL, or storage details even when they look like ordinary Error objects.
export class RequestValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RequestValidationError";
  }
}

export async function readJson(request: Request, options: { maxBytes?: number } = {}): Promise<unknown> {
  const contentType = request.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    throw new RequestValidationError("content-type must be application/json");
  }

  const maxBytes = options.maxBytes ?? DEFAULT_JSON_BODY_MAX_BYTES;
  const contentLength = Number(request.headers.get("content-length") || "0");
  if (Number.isFinite(contentLength) && contentLength > maxBytes) {
    throw new RequestBodyTooLargeError(maxBytes);
  }

  // Do not use request.json(): a chunked request can omit Content-Length, in which case it would
  // buffer an unbounded body before schema validation gets a chance to reject it.
  const reader = request.body?.getReader();
  if (!reader) return JSON.parse("");
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        await reader.cancel("JSON body exceeds configured limit");
        throw new RequestBodyTooLargeError(maxBytes);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new RequestValidationError("Request body must contain valid JSON");
  }
}

function zodIssueSummary(error: Error): string {
  const issues = (error as { issues?: Array<{ path?: Array<string | number>; message?: string }> }).issues;
  if (!Array.isArray(issues) || issues.length === 0) return "Invalid request";
  return issues
    .slice(0, 5)
    .map((issue) => `${(issue.path || []).join(".") || "request"}: ${issue.message || "invalid"}`)
    .join("; ")
    .slice(0, 300);
}

export function toErrorResponse(error: unknown): Response {
  if (error instanceof RequestBodyTooLargeError) {
    return json({ error: "Request body is too large" }, { status: 413 });
  }
  if (error instanceof RequestValidationError) {
    return json({ error: error.message }, { status: 400 });
  }
  if (error instanceof Error && error.name === "ZodError") {
    return json({ error: zodIssueSummary(error) }, { status: 400 });
  }
  console.error("Unhandled service error:", error);
  return json({ error: "Internal error" }, { status: 500 });
}
