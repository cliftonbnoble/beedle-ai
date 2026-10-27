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

export class RequestConflictError extends Error {}
export class RequestNotFoundError extends Error {}
export class UnsupportedMediaTypeError extends Error {}

export class RequestQuotaError extends Error {
  constructor(message: string, readonly retryAfterSeconds: number) { super(message); }
}

export class ReviewerQueueUnavailableError extends Error {}
export class ServiceUnavailableError extends Error {}
export class SearchIndexUnavailableError extends Error {}
export class SearchWorkBudgetError extends Error {
  constructor(readonly reason:string,readonly usage:Record<string,number>) {super("Search exceeded its work budget. Narrow the query or filters and try again.");}
}

export async function readJson(request: Request, options: { maxBytes?: number } = {}): Promise<unknown> {
  const contentType = request.headers.get("content-type") || "";
  const charset=/charset\s*=\s*["']?([^;"'\s]+)/i.exec(contentType)?.[1];
  const encoding=request.headers.get("content-encoding");
  if (!/^application\/json(?:\s*;|$)/i.test(contentType) || (charset&&!/^utf-?8$/i.test(charset)) || (encoding&&encoding.toLowerCase()!=="identity")) {
    throw new UnsupportedMediaTypeError("content-type must be application/json with an unencoded UTF-8 body");
  }

  const maxBytes = options.maxBytes ?? DEFAULT_JSON_BODY_MAX_BYTES;
  const contentLength = Number(request.headers.get("content-length") || "0");
  if (Number.isFinite(contentLength) && contentLength > maxBytes) {
    throw new RequestBodyTooLargeError(maxBytes);
  }

  // Do not use request.json(): a chunked request can omit Content-Length, in which case it would
  // buffer an unbounded body before schema validation gets a chance to reject it.
  const reader = request.body?.getReader();
  if (!reader) throw new RequestValidationError("Request body must contain valid JSON");
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        await reader.cancel("JSON body exceeds configured limit").catch(()=>{});
        throw new RequestBodyTooLargeError(maxBytes);
      }
      chunks.push(value);
    }
  } catch(error) {
    if(error instanceof RequestBodyTooLargeError)throw error;
    throw new RequestValidationError("Unable to read the JSON request body");
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
    return JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(bytes));
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
  if(error instanceof RequestNotFoundError)return json({error:error.message},{status:404});
  if(error instanceof UnsupportedMediaTypeError)return json({error:error.message},{status:415});
  if(error instanceof ReviewerQueueUnavailableError)return json({error:error.message},{status:503,headers:{"retry-after":"5"}});
  if(error instanceof SearchIndexUnavailableError)return json({error:error.message},{status:503,headers:{"retry-after":"60"}});
  if(error instanceof SearchWorkBudgetError)return json({error:error.message,reason:error.reason},{status:503});
  if (error instanceof RequestQuotaError) return json({ error: error.message }, { status:429, headers:{"retry-after":String(error.retryAfterSeconds)} });
  if (error instanceof ServiceUnavailableError) return json({ error:"Service is temporarily unavailable. Please retry later." }, {status:503,headers:{"retry-after":"60"}});
  if (error instanceof RequestConflictError) return json({ error: error.message }, { status: 409 });
  if (error instanceof RequestBodyTooLargeError) {
    return json({ error: "Request body is too large" }, { status: 413 });
  }
  if (error instanceof RequestValidationError) {
    return json({ error: error.message }, { status: 400 });
  }
  if (error instanceof Error && error.name === "ZodError") {
    return json({ error: zodIssueSummary(error) }, { status: 400 });
  }
  // The ingress summary records status and safe operation categories. Never log raw errors here.
  return json({ error: "Internal error" }, { status: 500 });
}
