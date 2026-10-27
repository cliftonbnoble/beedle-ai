import { RequestBodyTooLargeError, RequestValidationError, UnsupportedMediaTypeError } from "./http";

// Stream the envelope through a byte/part-count limiter before the native parser
// can allocate File objects or field strings. Limits do not rely on Content-Length.
export async function readLimitedFormData(request: Request, maxBytes: number, maxParts = 16): Promise<FormData> {
  const contentType = request.headers.get("content-type") || "";
  if(!/^multipart\/form-data(?:\s*;|$)/i.test(contentType))throw new UnsupportedMediaTypeError("content-type must be multipart/form-data");
  const match = /^multipart\/form-data\s*;.*\bboundary=(?:"([^"\r\n]+)"|([^;\s]+))/i.exec(contentType);
  const boundary = match?.[1] || match?.[2];
  if (!boundary || boundary.length > 70 || !/^[\x20-\x7e]+$/.test(boundary)) throw new RequestValidationError("A valid multipart boundary is required.");
  const reader = request.body?.getReader();
  if (!reader) throw new RequestValidationError("Multipart body is required.");
  const marker = new TextEncoder().encode(`\r\n--${boundary}`);
  const prefix = new Uint8Array(marker.length);
  for (let i = 1, j = 0; i < marker.length; i++) {
    while (j && marker[i] !== marker[j]) j = prefix[j - 1]!;
    if (marker[i] === marker[j]) j++;
    prefix[i] = j;
  }
  let matched = 2; // account for the initial boundary's omitted CRLF
  let boundarySuffix: number[] | undefined;
  let total = 0;
  let parts = 0;
  let limitError: RequestBodyTooLargeError | undefined;
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) { controller.close(); return; }
        total += value.byteLength;
        if (total > maxBytes) throw new RequestBodyTooLargeError(maxBytes);
        for (const byte of value) {
          if (boundarySuffix) {
            boundarySuffix.push(byte);
            if (boundarySuffix.length === 2) {
              if ((boundarySuffix[0] === 13 && byte === 10) || (boundarySuffix[0] === 45 && byte === 45)) {
                if (++parts > maxParts + 1) throw new RequestBodyTooLargeError(maxBytes);
              }
              boundarySuffix = undefined;
            }
          }
          while (matched && byte !== marker[matched]) matched = prefix[matched - 1]!;
          if (byte === marker[matched]) matched++;
          if (matched === marker.length) {
            boundarySuffix = [];
            matched = prefix[matched - 1]!;
          }
        }
        controller.enqueue(value);
      } catch (error) {
        if (error instanceof RequestBodyTooLargeError) limitError = error;
        await reader.cancel().catch(() => {});
        controller.error(error);
      }
    },
    cancel(reason) { return reader.cancel(reason); }
  }, { highWaterMark: 0 });
  try {
    const form = await new Response(stream, { headers: { "content-type": contentType } }).formData();
    let fields = 0;
    form.forEach((value) => {
      if (++fields > maxParts) throw new RequestBodyTooLargeError(maxBytes);
      if (typeof value === "string" && new TextEncoder().encode(value).length > 8192) throw new RequestValidationError("Multipart metadata fields may not exceed 8192 bytes.");
    });
    return form;
  } catch (error) {
    if (limitError) throw limitError;
    if (error instanceof RequestBodyTooLargeError || error instanceof RequestValidationError) throw error;
    throw new RequestValidationError("Invalid multipart body.");
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
