import { unzipSync } from "fflate";
import { RequestValidationError } from "../lib/http";

export const sourceContentTypes = {
  pdf: "application/pdf",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  markdown: "text/markdown; charset=utf-8"
} as const;
export type SourceFormat = keyof typeof sourceContentTypes;

// MIME supplied by a client is only a hint, never a parser or response-header
// selector. Text intake must have an explicit Markdown extension.
export function detectSourceFormat(bytes: Uint8Array, filename: string, fileType: "decision_docx" | "law_pdf"): SourceFormat {
  const extension = filename.toLowerCase().split(".").pop();
  if (extension === "md" || extension === "markdown") {
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch {
      throw new RequestValidationError("Markdown source must be valid UTF-8 text.");
    }
    if (!text.trim() || /[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(text) || /^\s*(?:<!doctype\s+html|<html\b|<svg\b|<\?xml\b)/i.test(text)) {
      throw new RequestValidationError("Expected a Markdown document, not binary or an HTML/XML document.");
    }
    return "markdown";
  }
  if (fileType === "law_pdf" && extension === "pdf" && new TextDecoder().decode(bytes.subarray(0, 5)) === "%PDF-") return "pdf";
  if (fileType === "decision_docx" && extension === "docx" && bytes[0] === 0x50 && bytes[1] === 0x4b && bytes[2] === 3 && bytes[3] === 4) {
    const entries = new Set<string>();
    try {
      // Inspect the directory only; never inflate archive contents to sniff MIME.
      unzipSync(bytes, { filter: (entry) => { entries.add(entry.name); return false; } });
    } catch {
      throw new RequestValidationError("Invalid DOCX archive.");
    }
    if (entries.has("[Content_Types].xml") && entries.has("word/document.xml") && ![...entries].some((name) => /vbaProject\.bin$/i.test(name))) return "docx";
  }
  throw new RequestValidationError("Source content does not match a supported PDF, DOCX, or Markdown format.");
}

export function safeSourceFilename(value: string): string {
  return String(value || "source").replace(/[^a-zA-Z0-9._-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 120) || "source";
}

export function sourceDownloadHeaders(filename: string, contentType = "application/octet-stream"): Headers {
  return new Headers({
    "content-type": contentType,
    "content-disposition": `attachment; filename="${safeSourceFilename(filename)}"`,
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "content-security-policy": "default-src 'none'; sandbox",
    "referrer-policy": "no-referrer",
    "x-frame-options": "DENY"
  });
}
