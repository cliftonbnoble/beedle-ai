import { deflateSync } from "node:zlib";

// A real xref/FlateDecode PDF with an embedded ToUnicode mapping, constructed
// locally so extraction tests do not depend on legal documents or network access.
export function pdfFixture({ blank = false, pages = 1, marginOnly = false } = {}) {
  const text = "The tenant retained possession. The landlord did not prove the alleged breach.";
  const cmap = "/CIDInit /ProcSet findresource begin 12 dict begin begincmap /CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def /CMapName /Test def /CMapType 2 def 1 begincodespacerange <00> <FF> endcodespacerange 1 beginbfchar <54> <0054> endbfchar endcmap CMapName currentdict /CMap defineresource pop end end";
  const objects = ["<< /Type /Catalog /Pages 2 0 R >>", "", "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode 4 0 R >>", `<< /Length ${cmap.length} >>\nstream\n${cmap}\nendstream`];
  const kids = [];
  for (let page = 0; page < pages; page++) {
    const pageId = objects.length + 1;
    kids.push(`${pageId} 0 R`);
    objects.push(`<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents ${pageId + 1} 0 R >>`);
    const content = deflateSync(Buffer.from(blank ? "" : marginOnly
      ? `BT /F1 12 Tf 40 760 Td (Court of Record - Appeal and Hearing - Selectable Header) Tj ET BT /F1 12 Tf 40 30 Td (Certified Copy - Page ${page + 1} - Selectable Footer) Tj ET`
      : `BT /F1 12 Tf 40 720 Td (${text}) Tj ET`));
    objects.push(Buffer.concat([Buffer.from(`<< /Length ${content.length} /Filter /FlateDecode >>\nstream\n`), content, Buffer.from("\nendstream")]));
  }
  objects[1] = `<< /Type /Pages /Kids [${kids.join(" ")}] /Count ${pages} >>`;
  const parts = [Buffer.from("%PDF-1.7\n")];
  const offsets = [0];
  for (let i = 0; i < objects.length; i++) {
    offsets.push(parts.reduce((sum, part) => sum + part.length, 0));
    parts.push(Buffer.from(`${i + 1} 0 obj\n`), Buffer.from(objects[i]), Buffer.from("\nendobj\n"));
  }
  const xref = parts.reduce((sum, part) => sum + part.length, 0);
  parts.push(Buffer.from(`xref\n0 ${objects.length + 1}\n0000000000 65535 f \n${offsets.slice(1).map(offset => `${String(offset).padStart(10, "0")} 00000 n \n`).join("")}trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF`));
  return new Uint8Array(Buffer.concat(parts));
}
