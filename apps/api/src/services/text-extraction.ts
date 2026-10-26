import { parser as xmlParser, type QualifiedTag } from "sax";
import { getResolvedPDFJS } from "unpdf";
import { RequestValidationError } from "../lib/http";
import { readDocxDocumentXml } from "./docx-archive";

export interface ExtractionPage {
  page: number;
  width: number;
  height: number;
  items: Array<{ text: string; x: number; y: number; width: number; height: number }>;
}
export interface ExtractionDiagnostics {
  status: "extracted";
  parser: string;
  format: "pdf" | "docx" | "markdown";
  characterCount: number;
  pages: ExtractionPage[];
}
export class ExtractionReviewRequired extends RequestValidationError {
  constructor(readonly reason: string) {
    super(`Source requires OCR or manual extraction review: ${reason}`);
    this.name = "ExtractionReviewRequired";
  }
}
export const extractionLimits = { pdfBytes: 3 * 1024 * 1024, pages: 50, characters: 250_000, items: 5000, milliseconds: 10_000 };
const wordNamespaces = new Set([
  "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
  "http://purl.oclc.org/ooxml/wordprocessingml/main"
]);

export function requireReadableText(paragraphs: string[]): number {
  const text = paragraphs.join("\n");
  const visible = text.replace(/\s/g, "");
  const letters = (text.match(/[\p{L}\p{N}]/gu) || []).length;
  if (text.length > extractionLimits.characters) throw new ExtractionReviewRequired("extracted text exceeds the bounded intake limit");
  if (letters < 40 || letters / Math.max(1, visible.length) < 0.45 || /\u0000|\ufffd/.test(text)) {
    throw new ExtractionReviewRequired("insufficient meaningful text; verify the original pages");
  }
  return text.length;
}

export function extractDocxText(bytes: Uint8Array): string[] {
  let xml: string;
  try { xml = readDocxDocumentXml(bytes); }
  catch { throw new ExtractionReviewRequired("unreadable DOCX document XML"); }
  // sax supports strictEntities; @types/sax does not yet declare that option.
  const options = { xmlns: true, strictEntities: true };
  const parser = xmlParser(true, options);
  const paragraphs: string[] = [];
  const stack: QualifiedTag[] = [];
  let text = "";
  let characters = 0;
  let elements = 0;
  let documentRoot = false;
  const wordTag = (tag: QualifiedTag | undefined, local: string) => Boolean(tag && wordNamespaces.has(tag.uri) && tag.local === local);
  const append = (value: string) => {
    characters += value.length;
    if (characters > extractionLimits.characters) throw new ExtractionReviewRequired("DOCX text exceeds the bounded intake limit");
    text += value;
  };
  parser.ondoctype = () => { throw new ExtractionReviewRequired("DOCTYPE declarations are unsupported"); };
  parser.onerror = () => { throw new ExtractionReviewRequired("malformed DOCX XML"); };
  parser.onopentag = (node) => {
    const tag = node as QualifiedTag;
    if (!stack.length) documentRoot = wordTag(tag, "document");
    if (stack.length >= 64 || ++elements > 50_000) throw new ExtractionReviewRequired("DOCX structure exceeds the bounded intake limit");
    if (wordTag(tag, "p")) {
      if (stack.some((parent) => wordTag(parent, "p"))) throw new ExtractionReviewRequired("nested DOCX paragraphs require review");
      text = "";
    }
    if (wordTag(tag, "tab") || wordTag(tag, "br") || wordTag(tag, "cr")) append(" ");
    stack.push(tag);
  };
  parser.ontext = (value) => { if (wordTag(stack[stack.length - 1], "t") && stack.some((tag) => wordTag(tag, "p"))) append(value); };
  parser.oncdata = parser.ontext;
  parser.onclosetag = () => {
    if (wordTag(stack.pop(), "p")) {
      if (text.trim()) paragraphs.push(text.trim());
      text = "";
    }
  };
  try {
    for (let at = 0; at < xml.length; at += 4096) parser.write(xml.slice(at, at + 4096));
    parser.close();
  } catch (error) {
    if (error instanceof ExtractionReviewRequired) throw error;
    throw new ExtractionReviewRequired("malformed DOCX XML");
  }
  if (!documentRoot) throw new ExtractionReviewRequired("missing Word document root/namespace");
  requireReadableText(paragraphs);
  return paragraphs;
}

export async function extractPdfText(bytes: Uint8Array): Promise<{ paragraphs: string[]; pages: ExtractionPage[]; parser: string }> {
  if (bytes.length > extractionLimits.pdfBytes) throw new ExtractionReviewRequired("PDF exceeds 3 MiB; use the reviewed offline extraction workflow");
  const pdfjs = await getResolvedPDFJS();
  const task = pdfjs.getDocument({
    data: bytes.slice(), // PDF.js may transfer its input; retain the original bytes.
    useSystemFonts: false,
    disableFontFace: true,
    useWorkerFetch: false,
    useWasm: false,
    stopAtErrors: true,
    verbosity: 0
  });
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; void task.destroy().catch(() => {}); }, extractionLimits.milliseconds);
  const paragraphs: string[] = [];
  const pages: ExtractionPage[] = [];
  let characters = 0;
  let items = 0;
  try {
    const pdf = await task.promise;
    if (pdf.numPages < 1 || pdf.numPages > extractionLimits.pages) throw new ExtractionReviewRequired("PDF page count exceeds the bounded intake limit");
    for (let number = 1; number <= pdf.numPages; number++) {
      const page = await pdf.getPage(number);
      const viewport = page.getViewport({ scale: 1 });
      const extracted: ExtractionPage = { page: number, width: viewport.width, height: viewport.height, items: [] };
      const reader = page.streamTextContent().getReader();
      let line = "";
      const lines: string[] = [];
      let bodyCharacters = 0;
      const margin = Math.min(72, viewport.height * 0.12);
      try {
        while (true) {
          if (timedOut) throw new ExtractionReviewRequired("PDF extraction deadline exceeded");
          const { done, value } = await reader.read();
          if (done) break;
          for (const item of value.items) {
            if (!("str" in item)) continue;
            characters += item.str.length;
            if (++items > extractionLimits.items || characters > extractionLimits.characters) throw new ExtractionReviewRequired("PDF text exceeds the bounded intake limit");
            extracted.items.push({ text: item.str, x: item.transform[4], y: item.transform[5], width: item.width, height: item.height });
            const [, top] = viewport.convertToViewportPoint(item.transform[4], item.transform[5]);
            if (top >= margin && top <= viewport.height - margin) bodyCharacters += (item.str.match(/[\p{L}\p{N}]/gu) || []).length;
            line += item.str + " ";
            if (item.hasEOL) { if (line.trim()) lines.push(line.trim()); line = ""; }
          }
        }
      } finally {
        await reader.cancel().catch(() => {});
        reader.releaseLock();
        page.cleanup();
      }
      if (line.trim()) lines.push(line.trim());
      if (lines.join("").replace(/\s/g, "").length < 20) throw new ExtractionReviewRequired(`page ${number} has little or no extractable text; OCR/manual review is required`);
      if (bodyCharacters < 20) throw new ExtractionReviewRequired(`page ${number} has insufficient body text outside its margins; readable headers/footers do not establish complete extraction`);
      paragraphs.push(...lines);
      pages.push(extracted);
    }
    requireReadableText(paragraphs);
    return { paragraphs, pages, parser: `unpdf/1.9.1 pdfjs/${pdfjs.version}` };
  } catch (error) {
    if (error instanceof ExtractionReviewRequired) throw error;
    throw new ExtractionReviewRequired(timedOut ? "PDF extraction deadline exceeded" : "unreadable, encrypted, or unsupported PDF");
  } finally {
    clearTimeout(timer);
    await task.destroy().catch(() => {});
  }
}
