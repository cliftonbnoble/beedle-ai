import fs from "node:fs/promises";
import zlib from "node:zlib";
import { choosePreferredSource, normalizeSectionRef as normalizeSectionRefUtil } from "./reference-normalization-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const indexPdfPath = process.env.INDEX_CODES_PDF;
const ordinancePdfPath = process.env.ORDINANCE_PDF;
const rulesPdfPath = process.env.RULES_PDF;
const ordinanceTrueTextPath = process.env.ORDINANCE_TRUE_TEXT;
const rulesTrueTextPath = process.env.RULES_TRUE_TEXT;
const ordinanceLayoutTextPath = process.env.ORDINANCE_LAYOUT_TEXT || process.env.ORDINANCE_TEXT_EXPORT;
const rulesLayoutTextPath = process.env.RULES_LAYOUT_TEXT || process.env.RULES_TEXT_EXPORT;
const dryRun = process.env.REFERENCES_DRY_RUN === "1";
const minOrdinanceSections = Number(process.env.MIN_ORDINANCE_SECTIONS || "15");
const minRulesSections = Number(process.env.MIN_RULES_SECTIONS || "10");

if (!indexPdfPath) {
  console.error("INDEX_CODES_PDF env var is required.");
  process.exit(1);
}
if (!ordinanceTrueTextPath && !ordinanceLayoutTextPath && !ordinancePdfPath) {
  console.error("Provide at least one ordinance source: ORDINANCE_TRUE_TEXT or ORDINANCE_LAYOUT_TEXT (or ORDINANCE_TEXT_EXPORT) or ORDINANCE_PDF.");
  process.exit(1);
}
if (!rulesTrueTextPath && !rulesLayoutTextPath && !rulesPdfPath) {
  console.error("Provide at least one rules source: RULES_TRUE_TEXT or RULES_LAYOUT_TEXT (or RULES_TEXT_EXPORT) or RULES_PDF.");
  process.exit(1);
}

async function readTextExportIfProvided(filePath, label) {
  if (!filePath) return null;
  try {
    return await fs.readFile(filePath, "utf8");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${label} text export path is invalid or unreadable: ${filePath} (${message})`);
  }
}

function normalizeWhitespace(input) {
  return input.replace(/\r/g, "").replace(/\u00A0/g, " ").replace(/[ \t]+/g, " ").trim();
}

function normalizeSectionRef(input) {
  return normalizeSectionRefUtil(input);
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
    if (next === "b") {
      bytes.push(8);
      i += 1;
      continue;
    }
    if (next === "f") {
      bytes.push(12);
      i += 1;
      continue;
    }
    if (next === "(" || next === ")" || next === "\\") {
      bytes.push(next.charCodeAt(0) & 0xff);
      i += 1;
      continue;
    }

    if (/[0-7]/.test(next)) {
      const oct = (text.slice(i + 1, i + 4).match(/^[0-7]{1,3}/) || [""])[0];
      if (oct) {
        bytes.push(parseInt(oct, 8) & 0xff);
        i += oct.length;
        continue;
      }
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
      const src = parseInt(bfchar[1], 16);
      const dst = parseInt(bfchar[2], 16);
      mapping.set(src, String.fromCodePoint(dst));
      continue;
    }
    const bfrange = line.match(/^<([0-9A-Fa-f]+)><([0-9A-Fa-f]+)><([0-9A-Fa-f]+)>$/);
    if (bfrange) {
      const start = parseInt(bfrange[1], 16);
      const end = parseInt(bfrange[2], 16);
      const destStart = parseInt(bfrange[3], 16);
      for (let i = start; i <= end; i += 1) {
        mapping.set(i, String.fromCodePoint(destStart + (i - start)));
      }
    }
  }
  return mapping;
}

function extractFontMaps(objects) {
  const cMapsByObject = new Map();
  for (const [id, obj] of objects) {
    const streamText = inflateMaybe(obj.stream);
    if (streamText && streamText.includes("begincmap")) {
      cMapsByObject.set(id, parseCMap(streamText));
    }
  }

  const fontObjToCMap = new Map();
  for (const [id, obj] of objects) {
    const ref = obj.dict.match(/\/ToUnicode\s+(\d+)\s+0\s+R/);
    if (!ref) continue;
    const cmapObj = Number(ref[1]);
    if (cMapsByObject.has(cmapObj)) {
      fontObjToCMap.set(id, cMapsByObject.get(cmapObj));
    }
  }

  const aliasToMap = new Map();
  for (const [id, obj] of objects) {
    for (const match of obj.dict.matchAll(/\/([A-Za-z0-9]+)\s+(\d+)\s+0\s+R/g)) {
      const alias = match[1];
      const fontObj = Number(match[2]);
      const cmap = fontObjToCMap.get(fontObj);
      if (cmap && !aliasToMap.has(alias)) {
        aliasToMap.set(alias, cmap);
      }
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
    if (arr?.[1]) {
      for (const m of arr[1].matchAll(/(\d+)\s+0\s+R/g)) refs.push(Number(m[1]));
    } else {
      const single = obj.dict.match(/\/Contents\s+(\d+)\s+0\s+R/);
      if (single?.[1]) refs.push(Number(single[1]));
    }
    pages.push({ id, refs });
  }
  pages.sort((a, b) => a.id - b.id);
  return pages;
}

function decodeWithMap(bytes, cmap) {
  let out = "";
  for (const code of bytes) {
    out += cmap?.get(code) ?? String.fromCharCode(code);
  }
  return out;
}

function stringLiteralsFromArray(arrayBody) {
  const out = [];
  const re = /\((?:\\.|[^\\)])*\)/g;
  let match;
  while ((match = re.exec(arrayBody))) {
    out.push(match[0].slice(1, -1));
  }
  return out;
}

function extractTextFromContentStream(streamText, aliasToMap) {
  const lines = [];
  let currentFont = null;
  const re = /\/([A-Za-z0-9]+)\s+[-\d.]+\s+Tf|\[(.*?)\]\s*TJ|\((?:\\.|[^\\)])*\)\s*Tj|T\*|Td/gs;
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
  const out = [];

  for (let i = 0; i < pages.length; i += 1) {
    const page = pages[i];
    const lines = [];
    for (const ref of page.refs) {
      const obj = objects.get(ref);
      if (!obj?.stream) continue;
      const streamText = inflateMaybe(obj.stream);
      if (!streamText) continue;
      lines.push(...extractTextFromContentStream(streamText, aliasToMap));
    }
    out.push({
      page: i + 1,
      text: lines.join("\n")
    });
  }

  return out;
}

function parseIndexCodes(pages) {
  const records = [];
  let currentFamily = null;
  const normalizeRefToken = (value) =>
    String(value || "")
      .trim()
      .replace(/[;,.:]+$/g, "")
      .replace(/^§+/, "")
      .replace(/^rule\s+/i, "")
      .replace(/^ordinance\s+/i, "");

  const extractSectionRefs = (text) =>
    Array.from(text.matchAll(/\b([0-9]{1,2}\.[0-9]{1,2}(?:\([a-z0-9]+\))*)\b/gi)).map((match) => normalizeRefToken(match[1]));

  for (const page of pages) {
    const lines = page.text.split(/\n+/).map((line) => normalizeWhitespace(line)).filter(Boolean);
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      if (/^index\s+codes?/i.test(line)) continue;
      if (/^[A-Z][A-Za-z/&\-\s]{3,80}$/.test(line) && !/\d/.test(line)) {
        currentFamily = line;
        continue;
      }
      const codeMatch = line.match(/\b(?:IC[-\s]?)?([0-9]{2,5}[A-Z]?)\b/);
      if (!codeMatch) continue;

      const codeIdentifier = codeMatch[0].replace(/\s+/g, "").toUpperCase().replace(/^IC/, "IC-");
      const tail = normalizeWhitespace(line.slice(line.indexOf(codeMatch[0]) + codeMatch[0].length));
      const continuation = [lines[i + 1], lines[i + 2]]
        .filter((value) => value && !/\b(?:IC[-\s]?)?[0-9]{2,5}[A-Z]?\b/.test(value))
        .join(" ");
      const after = normalizeWhitespace(`${tail} ${continuation}`);
      if (!after || after.length < 3) continue;

      const linkedOrdinance = Array.from(after.matchAll(/(?:ordinance|ord\.?|ro)\s*([0-9]+(?:\.[0-9]+)*(?:\([a-z0-9]+\))*)/gi)).map((m) =>
        normalizeRefToken(m[1])
      );
      const linkedRules = Array.from(after.matchAll(/(?:rule|rules|r\s*&\s*r|rr)\s*([0-9]+(?:\.[0-9]+)*(?:\([a-z0-9]+\))*)/gi)).map((m) =>
        normalizeRefToken(m[1])
      );
      // Fallback extraction from free text when explicit labels are missing.
      const genericRefs = extractSectionRefs(after);
      for (const ref of genericRefs) {
        if (ref.startsWith("37.")) {
          linkedOrdinance.push(ref);
        } else {
          linkedRules.push(ref);
        }
      }

      records.push({
        code_identifier: codeIdentifier,
        family: currentFamily,
        label: after.split(/[.;]/)[0]?.slice(0, 120) || after.slice(0, 120),
        description: after.slice(0, 400),
        reserved: /\breserved\b/i.test(after),
        legacy_pre_1002: /pre[-\s]?10\/?02|legacy/i.test(after),
        linked_ordinance_sections: Array.from(new Set(linkedOrdinance.filter(Boolean))),
        linked_rules_sections: Array.from(new Set(linkedRules.filter(Boolean))).map((r) => `Rule ${r}`),
        source_page_anchor: `index-codes#p${page.page}`
      });
    }
  }

  const deduped = new Map();
  for (const row of records) {
    const existing = deduped.get(row.code_identifier);
    if (!existing) {
      deduped.set(row.code_identifier, row);
      continue;
    }
    deduped.set(row.code_identifier, {
      ...existing,
      label: existing.label && existing.label.length >= row.label.length ? existing.label : row.label,
      description: existing.description && existing.description.length >= row.description.length ? existing.description : row.description,
      reserved: existing.reserved || row.reserved,
      legacy_pre_1002: existing.legacy_pre_1002 || row.legacy_pre_1002,
      linked_ordinance_sections: Array.from(new Set([...(existing.linked_ordinance_sections || []), ...(row.linked_ordinance_sections || [])])),
      linked_rules_sections: Array.from(new Set([...(existing.linked_rules_sections || []), ...(row.linked_rules_sections || [])]))
    });
  }
  return Array.from(deduped.values());
}

function parseOrdinanceSections(pages) {
  const sections = [];
  let current = null;
  let collisionsAvoided = 0;

  function flush() {
    if (!current) return;
    current.body_text = normalizeWhitespace(current.body_text || "");
    if (current.section_number && current.body_text.length > 20) sections.push(current);
    current = null;
  }

  for (const page of pages) {
    const lines = page.text.split(/\n+/).map((line) => normalizeWhitespace(line)).filter(Boolean);
    for (const line of lines) {
      const match = line.match(/^(?:section|sec\.?|§)\s*([0-9]+(?:\.[0-9]+)*)\s*([A-Za-z0-9()\-]*)\s*[:\-]?\s*(.*)$/i) || line.match(/^([0-9]+(?:\.[0-9]+)+)\s*([A-Za-z0-9()\-]*)\s*[:\-]?\s*(.*)$/);
      if (match) {
        flush();
        current = {
          section_number: match[1],
          subsection_path: match[2] || null,
          heading: match[3] ? match[3].slice(0, 180) : null,
          body_text: "",
          page_anchor: `rent-ordinance#p${page.page}`
        };
        continue;
      }
      if (!current) continue;
      current.body_text += `${line}\n`;
    }
  }
  flush();
  const deduped = new Map();
  for (const row of sections) {
    const key = normalizeSectionRef(`${row.section_number}${row.subsection_path || ""}`);
    if (!key) continue;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, row);
      continue;
    }
    collisionsAvoided += 1;
    deduped.set(key, {
      ...existing,
      body_text: normalizeWhitespace(`${existing.body_text}\n${row.body_text}`),
      heading: existing.heading || row.heading
    });
  }
  return { sections: Array.from(deduped.values()), collisionsAvoided };
}

function parseRulesSections(pages) {
  const sections = [];
  let currentPart = null;
  let current = null;
  let collisionsAvoided = 0;

  function flush() {
    if (!current) return;
    current.body_text = normalizeWhitespace(current.body_text || "");
    if (current.section_number && current.body_text.length > 20) sections.push(current);
    current = null;
  }

  for (const page of pages) {
    const lines = page.text.split(/\n+/).map((line) => normalizeWhitespace(line)).filter(Boolean);
    for (const line of lines) {
      const partMatch = line.match(/^part\s+([A-Za-z0-9.\-]+)/i);
      if (partMatch) {
        currentPart = partMatch[1];
      }

      const sectionMatch = line.match(/^(?:section|sec\.?|§)\s*([0-9]+(?:\.[0-9]+)*)\s*[:\-]?\s*(.*)$/i) || line.match(/^([0-9]+(?:\.[0-9]+)+)\s*[:\-]?\s*(.*)$/);
      if (sectionMatch) {
        flush();
        current = {
          part: currentPart,
          section_number: sectionMatch[1],
          heading: sectionMatch[2] ? sectionMatch[2].slice(0, 180) : null,
          body_text: "",
          page_anchor: `rules-regulations#p${page.page}`
        };
        continue;
      }

      if (!current) continue;
      current.body_text += `${line}\n`;
    }
  }
  flush();
  const deduped = new Map();
  for (const row of sections) {
    const key = normalizeSectionRef(`${row.part || ""}-${row.section_number}`);
    if (!key) continue;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, row);
      continue;
    }
    collisionsAvoided += 1;
    deduped.set(key, {
      ...existing,
      body_text: normalizeWhitespace(`${existing.body_text}\n${row.body_text}`),
      heading: existing.heading || row.heading
    });
  }
  return { sections: Array.from(deduped.values()), collisionsAvoided };
}

function parseOrdinanceSectionsFromText(text, sourceLabel) {
  const lines = text.split(/\r?\n/);
  const sections = [];
  let current = null;
  let collisionsAvoided = 0;
  let expectedMarkers = 0;

  function flush() {
    if (!current) return;
    current.body_text = normalizeWhitespace(current.body_text || "");
    if (current.section_number && current.body_text.length > 20) sections.push(current);
    current = null;
  }

  for (let i = 0; i < lines.length; i += 1) {
    const line = normalizeWhitespace(lines[i] || "");
    if (!line) continue;
    const match =
      line.match(/^(?:section|sec\.?|§)\s*([0-9]+(?:\.[0-9]+)*(?:\([a-z0-9]+\))*)\s*[-–—:]?\s*(.*)$/i) ||
      line.match(/^([0-9]+(?:\.[0-9]+)+(?:\([a-z0-9]+\))*)\s+(.+)$/i);
    if (match) {
      expectedMarkers += 1;
      flush();
      const sectionToken = match[1] || "";
      const sectionNumber = (sectionToken.match(/^[0-9]+(?:\.[0-9]+)*/) || [""])[0];
      const subpath = sectionToken.slice(sectionNumber.length) || null;
      current = {
        section_number: sectionNumber,
        subsection_path: subpath,
        heading: match[2] ? match[2].slice(0, 180) : null,
        body_text: "",
        page_anchor: `${sourceLabel}#line${i + 1}`
      };
      continue;
    }
    const subsectionMatch = line.match(/^(\([a-z0-9]+\)(?:\([a-z0-9]+\))*)\s*[-–—:]?\s*(.*)$/i);
    if (subsectionMatch && current?.section_number) {
      const baseSection = current.section_number;
      const parentSubpath = current.subsection_path || "";
      expectedMarkers += 1;
      flush();
      current = {
        section_number: baseSection,
        subsection_path: `${parentSubpath}${subsectionMatch[1]}`,
        heading: subsectionMatch[2] ? subsectionMatch[2].slice(0, 180) : null,
        body_text: "",
        page_anchor: `${sourceLabel}#line${i + 1}`
      };
      continue;
    }
    if (!current) continue;
    current.body_text += `${line}\n`;
  }
  flush();

  const deduped = new Map();
  for (const row of sections) {
    const key = normalizeSectionRef(`${row.section_number}${row.subsection_path || ""}`);
    if (!key) continue;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, row);
      continue;
    }
    collisionsAvoided += 1;
    deduped.set(key, {
      ...existing,
      body_text: normalizeWhitespace(`${existing.body_text}\n${row.body_text}`),
      heading: existing.heading || row.heading
    });
  }

  return { sections: Array.from(deduped.values()), collisionsAvoided, expectedMarkers };
}

function parseRulesSectionsFromText(text, sourceLabel) {
  const lines = text.split(/\r?\n/);
  const sections = [];
  let current = null;
  let currentPart = null;
  let collisionsAvoided = 0;
  let expectedMarkers = 0;

  function flush() {
    if (!current) return;
    current.body_text = normalizeWhitespace(current.body_text || "");
    if (current.section_number && current.body_text.length > 20) sections.push(current);
    current = null;
  }

  for (let i = 0; i < lines.length; i += 1) {
    const line = normalizeWhitespace(lines[i] || "");
    if (!line) continue;
    const partMatch = line.match(/^part\s+([A-Za-z0-9.\-]+)/i);
    if (partMatch) currentPart = partMatch[1];

    const match =
      line.match(/^(?:rule|section|sec\.?|§)\s*([0-9]+(?:\.[0-9]+)*(?:\([a-z0-9]+\))*)\s*[-–—:]?\s*(.*)$/i) ||
      line.match(/^([0-9]+(?:\.[0-9]+)+(?:\([a-z0-9]+\))*)\s+(.+)$/i);
    if (match) {
      expectedMarkers += 1;
      flush();
      current = {
        part: currentPart,
        section_number: match[1],
        heading: match[2] ? match[2].slice(0, 180) : null,
        body_text: "",
        page_anchor: `${sourceLabel}#line${i + 1}`
      };
      continue;
    }
    const subsectionMatch = line.match(/^(\([a-z0-9]+\)(?:\([a-z0-9]+\))*)\s*[-–—:]?\s*(.*)$/i);
    if (subsectionMatch && current?.section_number) {
      const baseSection = current.section_number;
      expectedMarkers += 1;
      flush();
      current = {
        part: currentPart,
        section_number: `${baseSection}${subsectionMatch[1]}`,
        heading: subsectionMatch[2] ? subsectionMatch[2].slice(0, 180) : null,
        body_text: "",
        page_anchor: `${sourceLabel}#line${i + 1}`
      };
      continue;
    }
    if (!current) continue;
    current.body_text += `${line}\n`;
  }
  flush();

  const deduped = new Map();
  for (const row of sections) {
    const key = normalizeSectionRef(`${row.part || ""}-${row.section_number}`);
    if (!key) continue;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, row);
      continue;
    }
    collisionsAvoided += 1;
    deduped.set(key, {
      ...existing,
      body_text: normalizeWhitespace(`${existing.body_text}\n${row.body_text}`),
      heading: existing.heading || row.heading
    });
  }

  return { sections: Array.from(deduped.values()), collisionsAvoided, expectedMarkers };
}

function buildCrosswalk(indexCodes, ordinanceSections, rulesSections) {
  const ordinanceSet = new Set(
    ordinanceSections.map((row) => normalizeSectionRef(`${row.section_number}${row.subsection_path || ""}`)).filter(Boolean)
  );
  const rulesSet = new Set(
    rulesSections.map((row) => normalizeSectionRef(`${row.part ? `${row.part}-` : ""}${row.section_number}`)).filter(Boolean)
  );
  const out = [];

  for (const code of indexCodes) {
    for (const ord of code.linked_ordinance_sections || []) {
      const normalized = normalizeSectionRef(ord);
      const resolved = ordinanceSet.has(normalized) || Array.from(ordinanceSet).some((item) => item.startsWith(normalized));
      out.push({
        index_code: code.code_identifier,
        ordinance_section: ord,
        source: resolved ? "index_code_linked_ordinance" : "index_code_linked_ordinance_unresolved"
      });
    }
    for (const rule of code.linked_rules_sections || []) {
      const normalized = normalizeSectionRef(rule);
      const resolved = rulesSet.has(normalized) || Array.from(rulesSet).some((item) => item.startsWith(normalized));
      out.push({
        index_code: code.code_identifier,
        rules_section: rule,
        source: resolved ? "index_code_linked_rule" : "index_code_linked_rule_unresolved"
      });
    }
  }

  return out;
}

function collisionSummary(rows, citationBuilder) {
  const counts = new Map();
  for (const row of rows) {
    const citation = citationBuilder(row);
    const normalized = normalizeSectionRef(citation);
    if (!normalized) continue;
    counts.set(normalized, (counts.get(normalized) || 0) + 1);
  }
  let collisions = 0;
  const samples = [];
  for (const [normalized, count] of counts.entries()) {
    if (count > 1) {
      collisions += count - 1;
      if (samples.length < 12) samples.push({ normalized_citation: normalized, duplicate_count: count });
    }
  }
  return { duplicate_normalized_citations_encountered: collisions, sample_collisions: samples };
}

async function postJson(endpoint, payload) {
  const response = await fetch(`${apiBase}${endpoint}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`Failed ${endpoint}: ${response.status} ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  const [indexBytes, ordBytes, ruleBytes, ordinanceTrueText, ordinanceLayoutText, rulesTrueText, rulesLayoutText] = await Promise.all([
    fs.readFile(indexPdfPath),
    ordinancePdfPath ? fs.readFile(ordinancePdfPath).catch(() => null) : Promise.resolve(null),
    rulesPdfPath ? fs.readFile(rulesPdfPath).catch(() => null) : Promise.resolve(null),
    readTextExportIfProvided(ordinanceTrueTextPath, "ORDINANCE_TRUE_TEXT"),
    readTextExportIfProvided(ordinanceLayoutTextPath, "ORDINANCE_LAYOUT_TEXT"),
    readTextExportIfProvided(rulesTrueTextPath, "RULES_TRUE_TEXT"),
    readTextExportIfProvided(rulesLayoutTextPath, "RULES_LAYOUT_TEXT")
  ]);

  const indexPages = extractPdfPagesText(indexBytes);
  const ordinancePages = ordBytes ? extractPdfPagesText(ordBytes) : [];
  const rulesPages = ruleBytes ? extractPdfPagesText(ruleBytes) : [];

  const indexCodes = parseIndexCodes(indexPages);
  const ordinancePdfParse = parseOrdinanceSections(ordinancePages);
  const rulesPdfParse = parseRulesSections(rulesPages);
  const ordinanceTrueParse = ordinanceTrueText ? parseOrdinanceSectionsFromText(ordinanceTrueText, "rent-ordinance-true-text") : null;
  const ordinanceLayoutParse = ordinanceLayoutText ? parseOrdinanceSectionsFromText(ordinanceLayoutText, "rent-ordinance-layout-text") : null;
  const rulesTrueParse = rulesTrueText ? parseRulesSectionsFromText(rulesTrueText, "rules-regulations-true-text") : null;
  const rulesLayoutParse = rulesLayoutText ? parseRulesSectionsFromText(rulesLayoutText, "rules-regulations-layout-text") : null;

  const ordinanceSelected = choosePreferredSource({
    trueText: ordinanceTrueParse
      ? { source_path: ordinanceTrueTextPath, section_count: ordinanceTrueParse.sections.length, data: ordinanceTrueParse }
      : undefined,
    layoutText: ordinanceLayoutParse
      ? { source_path: ordinanceLayoutTextPath, section_count: ordinanceLayoutParse.sections.length, data: ordinanceLayoutParse }
      : undefined,
    pdf: ordinancePdfPath ? { source_path: ordinancePdfPath, section_count: ordinancePdfParse.sections.length, data: ordinancePdfParse } : undefined,
    minThreshold: minOrdinanceSections
  });
  const rulesSelected = choosePreferredSource({
    trueText: rulesTrueParse ? { source_path: rulesTrueTextPath, section_count: rulesTrueParse.sections.length, data: rulesTrueParse } : undefined,
    layoutText: rulesLayoutParse
      ? { source_path: rulesLayoutTextPath, section_count: rulesLayoutParse.sections.length, data: rulesLayoutParse }
      : undefined,
    pdf: rulesPdfPath ? { source_path: rulesPdfPath, section_count: rulesPdfParse.sections.length, data: rulesPdfParse } : undefined,
    minThreshold: minRulesSections
  });

  const ordinanceChosen = {
    parser: ordinanceSelected.source_type,
    source_path: ordinanceSelected.source_path || null,
    fallback_reason: ordinanceSelected.fallback_reason || null,
    data: ordinanceSelected.data
  };
  const rulesChosen = {
    parser: rulesSelected.source_type,
    source_path: rulesSelected.source_path || null,
    fallback_reason: rulesSelected.fallback_reason || null,
    data: rulesSelected.data
  };

  const ordinanceSections = ordinanceChosen.data.sections;
  const rulesSections = rulesChosen.data.sections;
  const crosswalk = buildCrosswalk(indexCodes, ordinanceSections, rulesSections);
  const crosswalkCandidates = crosswalk.length;
  const resolvedCrosswalkCount = crosswalk.filter((row) => !String(row.source).includes("_unresolved")).length;

  const coverageReport = {
    ordinance: {
      parser_used: ordinanceChosen.parser,
      source_path: ordinanceChosen.source_path,
      fallback_reason: ordinanceChosen.fallback_reason,
      expected_section_count:
        ordinanceChosen.parser === "pdf" ? ordinancePages.length : ordinanceChosen.data.expectedMarkers || ordinanceChosen.data.sections.length,
      parsed_section_count: ordinanceSections.length,
      duplicate_collisions_avoided: ordinanceChosen.data.collisionsAvoided || 0,
      low_confidence_sections: ordinanceSections.filter((row) => normalizeWhitespace(row.body_text || "").length < 140).length,
      ...collisionSummary(ordinanceSections, (row) => `${row.section_number}${row.subsection_path || ""}`)
    },
    rules: {
      parser_used: rulesChosen.parser,
      source_path: rulesChosen.source_path,
      fallback_reason: rulesChosen.fallback_reason,
      expected_section_count: rulesChosen.parser === "pdf" ? rulesPages.length : rulesChosen.data.expectedMarkers || rulesChosen.data.sections.length,
      parsed_section_count: rulesSections.length,
      duplicate_collisions_avoided: rulesChosen.data.collisionsAvoided || 0,
      low_confidence_sections: rulesSections.filter((row) => normalizeWhitespace(row.body_text || "").length < 140).length,
      ...collisionSummary(rulesSections, (row) => `${row.part ? `${row.part}-` : ""}${row.section_number}`)
    },
    crosswalk: {
      total_candidates: crosswalkCandidates,
      resolved_links: resolvedCrosswalkCount,
      unresolved_links: Math.max(0, crosswalkCandidates - resolvedCrosswalkCount)
    }
  };

  const payload = {
    source_trace: {
      index_codes: indexPdfPath,
      ordinance: ordinanceChosen.source_path || ordinancePdfPath || "",
      rules: rulesChosen.source_path || rulesPdfPath || ""
    },
    index_codes: indexCodes,
    ordinance_sections: ordinanceSections,
    rules_sections: rulesSections,
    crosswalk,
    coverage_report: coverageReport
  };

  console.log("Normalization counts:");
  const counts = {
    index_codes: indexCodes.length,
    ordinance_sections: ordinanceSections.length,
    rules_sections: rulesSections.length,
    crosswalk: crosswalk.length,
    unresolved_crosswalk: coverageReport.crosswalk.unresolved_links
  };
  console.log(
    JSON.stringify(counts, null, 2)
  );
  console.log("Collision diagnostics:");
  console.log(
    JSON.stringify(
      {
        ordinance: {
          duplicate_normalized_citations_encountered: coverageReport.ordinance.duplicate_normalized_citations_encountered,
          sample_collisions: coverageReport.ordinance.sample_collisions
        },
        rules: {
          duplicate_normalized_citations_encountered: coverageReport.rules.duplicate_normalized_citations_encountered,
          sample_collisions: coverageReport.rules.sample_collisions
        }
      },
      null,
      2
    )
  );
  if (counts.index_codes < 20 || counts.ordinance_sections < 15 || counts.rules_sections < 10) {
    console.warn("WARNING: low normalization coverage detected. Review parser output and consider text-export-assisted normalization.");
  }
  console.log(
    `Source choices: ordinance=${coverageReport.ordinance.parser_used} (${coverageReport.ordinance.source_path || "n/a"}), rules=${coverageReport.rules.parser_used} (${coverageReport.rules.source_path || "n/a"})`
  );

  if (dryRun) {
    console.log("Dry run only; skipping POST /admin/references/rebuild");
    return;
  }

  const result = await postJson("/admin/references/rebuild", payload);
  console.log("Rebuild result:");
  console.log(JSON.stringify(result, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
