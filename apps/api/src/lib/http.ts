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

export async function readJson(request: Request, options: { maxBytes?: number } = {}): Promise<unknown> {
  const contentType = request.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    throw new Error("content-type must be application/json");
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
  return JSON.parse(new TextDecoder().decode(bytes));
}

// Failures from the platform itself (D1/AI/Vectorize/R2/network) are server faults, not client faults.
// They must surface as 500 with a generic body: a raw D1/binding message is internal detail the client
// can't act on, and reporting an outage as 400 breaks monitoring, retries, and the web error UX.
const INFRASTRUCTURE_ERROR_PATTERN =
  /D1_|too many SQL variables|no such table|no such column|needs to be run remotely|fetch failed|error code: 10\d\d|Network connection|timed out|timeout|Vectorize|VECTOR_|R2_/i;

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
  if (error instanceof Error && error.name === "ZodError") {
    return json({ error: zodIssueSummary(error) }, { status: 400 });
  }
  const message = error instanceof Error ? error.message : "Unexpected error";
  if (!(error instanceof Error) || INFRASTRUCTURE_ERROR_PATTERN.test(message)) {
    console.error("Unhandled service error:", error);
    return json({ error: "Internal error" }, { status: 500 });
  }
  // Remaining Error messages are crafted domain/validation messages (size guards, unsupported types,
  // content-type checks, missing ids) — genuinely client-actionable, so 400 with the message stands.
  return json({ error: message }, { status: 400 });
}
