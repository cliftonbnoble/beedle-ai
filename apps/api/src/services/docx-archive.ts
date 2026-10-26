import { Inflate } from "fflate";
import { RequestBodyTooLargeError, RequestValidationError } from "../lib/http";

export const docxLimits = {
  compressedBytes: 15 * 1024 * 1024,
  totalBytes: 40 * 1024 * 1024,
  entryBytes: 10 * 1024 * 1024,
  entries: 512,
  ratio: 200
};
type Limits = typeof docxLimits;
interface Entry { name: string; size: number; originalSize: number; compression: number; crc: number; offset: number }

function invalid(message: string): never {
  throw new RequestValidationError(`Invalid DOCX archive: ${message}`);
}

// Read the bounded central directory without decompressing or allocating from
// untrusted sizes. ZIP64/encrypted/multi-disk archives are deliberately unsupported.
export function inspectDocxArchive(bytes: Uint8Array, limits: Limits = docxLimits): Map<string, Entry> {
  if (bytes.length > limits.compressedBytes) throw new RequestBodyTooLargeError(limits.compressedBytes);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const u16 = (offset: number) => view.getUint16(offset, true);
  const u32 = (offset: number) => view.getUint32(offset, true);
  let end = -1;
  for (let at = bytes.length - 22; at >= Math.max(0, bytes.length - 65557); at--) {
    if (u32(at) === 0x06054b50 && at + 22 + u16(at + 20) === bytes.length) { end = at; break; }
  }
  if (end < 0) invalid("missing end record");
  const count = u16(end + 10);
  const directoryStart = u32(end + 16);
  if (u16(end + 4) || u16(end + 6) || u16(end + 8) !== count || count === 0xffff) invalid("unsupported disk/ZIP64 layout");
  if (count > limits.entries) throw new RequestBodyTooLargeError(limits.entries);
  if (directoryStart + u32(end + 12) !== end) invalid("directory bounds");
  const entries = new Map<string, Entry>();
  const spans: Array<[number, number]> = [];
  let cursor = directoryStart;
  let total = 0;
  for (let index = 0; index < count; index++) {
    if (cursor + 46 > end || u32(cursor) !== 0x02014b50) invalid("directory entry");
    const flags = u16(cursor + 8);
    const compression = u16(cursor + 10);
    const size = u32(cursor + 20);
    const originalSize = u32(cursor + 24);
    const nameLength = u16(cursor + 28);
    const next = cursor + 46 + nameLength + u16(cursor + 30) + u16(cursor + 32);
    const local = u32(cursor + 42);
    if (next > end || nameLength > 1024 || u16(cursor + 34) || (flags & 0x41) || ![0, 8].includes(compression)) invalid("unsupported entry");
    let name = "";
    try { name = new TextDecoder(flags & 0x800 ? "utf-8" : "latin1", { fatal: true }).decode(bytes.subarray(cursor + 46, cursor + 46 + nameLength)); }
    catch { invalid("filename encoding"); }
    if (!name || name.startsWith("/") || /[\\\u0000]/.test(name) || name.split("/").includes("..") || entries.has(name)) invalid("duplicate or unsafe path");
    total += originalSize;
    if (originalSize > limits.entryBytes || total > limits.totalBytes || originalSize > Math.max(1, size) * limits.ratio) {
      throw new RequestBodyTooLargeError(limits.entryBytes);
    }
    if (local + 30 > directoryStart || u32(local) !== 0x04034b50 || u16(local + 6) !== flags || u16(local + 8) !== compression) invalid("local header");
    const localNameLength = u16(local + 26);
    const offset = local + 30 + localNameLength + u16(local + 28);
    if (offset + size > directoryStart || localNameLength !== nameLength) invalid("entry bounds");
    for (let i = 0; i < nameLength; i++) if (bytes[local + 30 + i] !== bytes[cursor + 46 + i]) invalid("inconsistent filename");
    if (!(flags & 8) && (u32(local + 18) !== size || u32(local + 22) !== originalSize || u32(local + 14) !== u32(cursor + 16))) invalid("inconsistent entry sizes/checksum");
    if (compression === 0 && size !== originalSize) invalid("stored size mismatch");
    entries.set(name, { name, size, originalSize, compression, crc: u32(cursor + 16), offset });
    spans.push([local, offset + size]);
    cursor = next;
  }
  if (cursor !== end) invalid("directory size mismatch");
  spans.sort((a, b) => a[0] - b[0]);
  for (let i = 1; i < spans.length; i++) if (spans[i]![0] < spans[i - 1]![1]) invalid("overlapping entries");
  if (!entries.has("word/document.xml") || !entries.has("[Content_Types].xml")) invalid("missing document XML");
  if ([...entries.keys()].some((name) => /vbaProject\.bin$/i.test(name))) invalid("macro content");
  return entries;
}

const crcTable = Uint32Array.from({ length: 256 }, (_, value) => {
  for (let bit = 0; bit < 8; bit++) value = (value >>> 1) ^ ((value & 1) ? 0xedb88320 : 0);
  return value >>> 0;
});

export function readDocxDocumentXml(bytes: Uint8Array, limits: Limits = docxLimits): string {
  const entries = inspectDocxArchive(bytes, limits);
  const entry = entries.get("word/document.xml")!;
  const output = new Uint8Array(entry.originalSize); // directory size is bounded above
  let written = 0;
  let checksum = 0xffffffff;
  let finished = false;
  const consume = (chunk: Uint8Array, final: boolean) => {
    if (written + chunk.length > entry.originalSize || written + chunk.length > limits.entryBytes) throw new RequestBodyTooLargeError(limits.entryBytes);
    output.set(chunk, written);
    written += chunk.length;
    for (const byte of chunk) checksum = crcTable[(checksum ^ byte) & 255]! ^ (checksum >>> 8);
    finished = final;
  };
  try {
    if (entry.compression === 0) consume(bytes.subarray(entry.offset, entry.offset + entry.size), true);
    else {
      const inflater = new Inflate(consume);
      // Small input pieces bound each transient DEFLATE output allocation, even
      // when forged metadata understates the actual decompressed size.
      for (let offset = 0; offset < entry.size; offset += 256) {
        const end = Math.min(entry.size, offset + 256);
        inflater.push(bytes.subarray(entry.offset + offset, entry.offset + end), end === entry.size);
      }
    }
  } catch (error) {
    if (error instanceof RequestBodyTooLargeError) throw error;
    invalid("corrupt compressed XML");
  }
  if (!finished || written !== entry.originalSize || ((checksum ^ 0xffffffff) >>> 0) !== entry.crc) invalid("XML size/checksum mismatch");
  try { return new TextDecoder("utf-8", { fatal: true }).decode(output); }
  catch { invalid("XML must be UTF-8"); }
}
