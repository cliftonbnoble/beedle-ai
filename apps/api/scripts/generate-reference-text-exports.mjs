import fs from "node:fs/promises";
import path from "node:path";
import zlib from "node:zlib";

const ordinancePdfPath = process.env.ORDINANCE_PDF;
const rulesPdfPath = process.env.RULES_PDF;
const outDir = process.env.TEXT_EXPORT_OUT_DIR || path.resolve(process.cwd(), "reports", "reference-text-exports");

if (!ordinancePdfPath || !rulesPdfPath) {
  console.error("ORDINANCE_PDF and RULES_PDF env vars are required.");
  process.exit(1);
}

function normalizeWhitespace(input) {
  return input.replace(/\r/g, "").replace(/\u00A0/g, " ").replace(/[ \t]+/g, " ").trim();
}

function decodePdfEscapesToBytes(text) {
  const bytes = [];
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (ch !== "\\") {
      bytes.push(ch.charCodeAt(0) & 0xff);
      continue;
    }
    const next = text[i + 1];
    if (!next) continue;
    if (next === "n") {
      bytes.push(10);
      i += 1;
      continue;
    }
    if (next === "r") {
      bytes.push(13);
      i += 1;
      continue;
    }
    if (next === "t") {
      bytes.push(9);
      i += 1;
      continue;
    }
    if (next === "(" || next === ")" || next === "\\") {
      bytes.push(next.charCodeAt(0) & 0xff);
      i += 1;
      continue;
    }
    bytes.push(next.charCodeAt(0) & 0xff);
    i += 1;
  }
  return bytes;
}

function parsePdfObjects(buffer) {
  const source = buffer.toString("latin1");
  const objects = new Map();
  const re = /(\d+)\s+(\d+)\s+obj([\s\S]*?)endobj/g;
  let match;
  while ((match = re.exec(source))) {
    const id = Number(match[1]);
    const body = match[3] || "";
    const streamStart = body.indexOf("stream\r\n");
    const streamStartN = body.indexOf("stream\n");
    const start = streamStart >= 0 ? streamStart + 8 : streamStartN >= 0 ? streamStartN + 7 : -1;
    let dict = body;
    let stream = null;
    if (start >= 0) {
      const endMarker = body.indexOf("\nendstream", start);
      const endMarkerR = body.indexOf("\rendstream", start);
      const end = endMarker >= 0 ? endMarker : endMarkerR;
      if (end > start) {
        dict = body.slice(0, start - (streamStart >= 0 ? 8 : 7));
        stream = Buffer.from(body.slice(start, end), "latin1");
      }
    }
    objects.set(id, { id, dict, stream });
  }
  return objects;
}

function inflateMaybe(stream) {
  if (!stream) return null;
  try {
    return zlib.inflateSync(stream).toString("latin1");
  } catch {
    return null;
  }
}

function parseCMap(cmapText) {
  const mapping = new Map();
  const lines = cmapText.split(/\r?\n/);
  for (const lineRaw of lines) {
    const line = lineRaw.trim();
    const bfchar = line.match(/^<([0-9A-Fa-f]+)><([0-9A-Fa-f]+)>$/);
    if (bfchar) {
      mapping.set(parseInt(bfchar[1], 16), String.fromCodePoint(parseInt(bfchar[2], 16)));
      continue;
    }
    const bfrange = line.match(/^<([0-9A-Fa-f]+)><([0-9A-Fa-f]+)><([0-9A-Fa-f]+)>$/);
    if (bfrange) {
      const start = parseInt(bfrange[1], 16);
      const end = parseInt(bfrange[2], 16);
      const destStart = parseInt(bfrange[3], 16);
      for (let i = start; i <= end; i += 1) mapping.set(i, String.fromCodePoint(destStart + (i - start)));
    }
  }
  return mapping;
}

function extractFontMaps(objects) {
  const cMapsByObject = new Map();
  for (const [id, obj] of objects) {
    const text = inflateMaybe(obj.stream);
    if (text && text.includes("begincmap")) cMapsByObject.set(id, parseCMap(text));
  }

  const fontObjToCMap = new Map();
  for (const [id, obj] of objects) {
    const ref = obj.dict.match(/\/ToUnicode\s+(\d+)\s+0\s+R/);
    if (ref && cMapsByObject.has(Number(ref[1]))) fontObjToCMap.set(id, cMapsByObject.get(Number(ref[1])));
  }

  const aliasToMap = new Map();
  for (const obj of objects.values()) {
    for (const match of obj.dict.matchAll(/\/([A-Za-z0-9]+)\s+(\d+)\s+0\s+R/g)) {
      const alias = match[1];
      const fontObj = Number(match[2]);
      if (fontObjToCMap.has(fontObj) && !aliasToMap.has(alias)) aliasToMap.set(alias, fontObjToCMap.get(fontObj));
    }
  }
  return aliasToMap;
}

function contentObjectIdsByPage(objects) {
  const pages = [];
  for (const [id, obj] of objects) {
    if (!/\/Type\s*\/Page\b/.test(obj.dict) || /\/Type\s*\/Pages\b/.test(obj.dict)) continue;
    const refs = [];
    const arr = obj.dict.match(/\/Contents\s*\[([^\]]+)\]/);
    if (arr?.[1]) for (const m of arr[1].matchAll(/(\d+)\s+0\s+R/g)) refs.push(Number(m[1]));
    const single = obj.dict.match(/\/Contents\s+(\d+)\s+0\s+R/);
    if (single?.[1] && refs.length === 0) refs.push(Number(single[1]));
    pages.push({ id, refs });
  }
  pages.sort((a, b) => a.id - b.id);
  return pages;
}

function stringLiteralsFromArray(arrayBody) {
  const out = [];
  const re = /\((?:\\.|[^\\)])*\)/g;
  let match;
  while ((match = re.exec(arrayBody))) out.push(match[0].slice(1, -1));
  return out;
}

function decodeWithMap(bytes, cmap) {
  let out = "";
  for (const code of bytes) out += cmap?.get(code) ?? String.fromCharCode(code);
  return out;
}

function extractTextFromContentStream(streamText, aliasToMap) {
  const lines = [];
  let currentFont = null;
  const re = /\/([A-Za-z0-9]+)\s+[-\d.]+\s+Tf|\[(.*?)\]\s*TJ|\((?:\\.|[^\\)])*\)\s*Tj/gs;
  let match;
  while ((match = re.exec(streamText))) {
    if (match[1]) {
      currentFont = match[1];
      continue;
    }
    if (typeof match[2] === "string") {
      const fragments = stringLiteralsFromArray(match[2]);
      const text = fragments.map((frag) => decodeWithMap(decodePdfEscapesToBytes(frag), aliasToMap.get(currentFont))).join("");
      const clean = normalizeWhitespace(text);
      if (clean) lines.push(clean);
      continue;
    }
    if (match[0].endsWith("Tj")) {
      const literal = match[0].replace(/\)\s*Tj$/, "").replace(/^\(/, "");
      const text = decodeWithMap(decodePdfEscapesToBytes(literal), aliasToMap.get(currentFont));
      const clean = normalizeWhitespace(text);
      if (clean) lines.push(clean);
    }
  }
  return lines;
}

function extractPdfPagesText(bytes) {
  const objects = parsePdfObjects(bytes);
  const aliasToMap = extractFontMaps(objects);
  const pages = contentObjectIdsByPage(objects);
  return pages.map((page, idx) => {
    const lines = [];
    for (const ref of page.refs) {
      const streamText = inflateMaybe(objects.get(ref)?.stream);
      if (streamText) lines.push(...extractTextFromContentStream(streamText, aliasToMap));
    }
    return `--- PAGE ${idx + 1} ---\n${lines.join("\n")}`;
  });
}

async function main() {
  await fs.mkdir(outDir, { recursive: true });
  const [ordBytes, ruleBytes] = await Promise.all([fs.readFile(ordinancePdfPath), fs.readFile(rulesPdfPath)]);

  const ordText = extractPdfPagesText(ordBytes).join("\n\n");
  const ruleText = extractPdfPagesText(ruleBytes).join("\n\n");

  const ordOut = path.join(outDir, "rent-ordinance.export.txt");
  const ruleOut = path.join(outDir, "rules-and-regs.export.txt");
  await Promise.all([fs.writeFile(ordOut, ordText), fs.writeFile(ruleOut, ruleText)]);

  console.log("Generated text exports:");
  console.log(`ORDINANCE_TEXT_EXPORT=${ordOut}`);
  console.log(`RULES_TEXT_EXPORT=${ruleOut}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
