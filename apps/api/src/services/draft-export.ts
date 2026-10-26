import { renderReviewPackage } from "./draft-review-package";
import {
  draftExportRequestSchema,
  draftExportResponseSchema,
  type DraftConclusionsResponse,
  type DraftExportResponse,
  type DraftTemplateResponse
} from "@beedle/shared";

function slugify(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, 64);
}

function escapeHtml(input: string): string {
  return input
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function markdownFromTemplate(input: DraftTemplateResponse, title: string): string {
  const lines: string[] = [`# ${title}`, "", `Case Type: ${input.case_type}`, `Mode: ${input.template_mode}`, "", "## Sections"];
  for (const section of input.template_sections) {
    lines.push(`### ${section.section_name}`);
    lines.push(section.section_purpose);
    lines.push("", section.placeholder_text, "");
    if (section.drafting_prompts.length) {
      lines.push("Prompts:");
      for (const prompt of section.drafting_prompts) lines.push(`- ${prompt}`);
    }
    lines.push("");
  }
  lines.push("## Guidance Notes");
  for (const note of input.guidance_notes) lines.push(`- ${note}`);
  return lines.join("\n");
}

function textFromTemplate(input: DraftTemplateResponse, title: string): string {
  const lines: string[] = [title, "=".repeat(title.length), "", `Case Type: ${input.case_type}`, `Mode: ${input.template_mode}`, ""];
  for (const section of input.template_sections) {
    lines.push(`[${section.section_name}]`, section.section_purpose, section.placeholder_text, "");
    if (section.drafting_prompts.length) lines.push(`Prompts: ${section.drafting_prompts.join(" | ")}`, "");
  }
  return lines.join("\n");
}

function htmlFromTemplate(input: DraftTemplateResponse, title: string): string {
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>${escapeHtml(title)}</title>
  <style>
    body { font-family: Georgia, serif; margin: 2rem; line-height: 1.5; color: #111; }
    h1,h2,h3 { margin-bottom: 0.4rem; }
    .section { border-top: 1px solid #ccc; padding-top: 0.5rem; margin-top: 0.5rem; }
    .placeholder { white-space: pre-wrap; background: #faf8f2; padding: 0.75rem; border: 1px solid #ddd; }
    .muted { color: #666; }
  </style>
</head>
<body>
  <h1>${escapeHtml(title)}</h1>
  <p class="muted">Case Type: ${escapeHtml(input.case_type)} | Mode: ${escapeHtml(input.template_mode)}</p>
  ${input.template_sections
    .map(
      (section) =>
        `<div class="section"><h3>${escapeHtml(section.section_name)}</h3><p>${escapeHtml(section.section_purpose)}</p><div class="placeholder">${escapeHtml(
          section.placeholder_text
        )}</div></div>`
    )
    .join("")}
</body>
</html>`;
}

function mimeForFormat(format: "markdown" | "text" | "html"): string {
  if (format === "markdown") return "text/markdown; charset=utf-8";
  if (format === "html") return "text/html; charset=utf-8";
  return "text/plain; charset=utf-8";
}

export async function exportDraft(input: unknown): Promise<DraftExportResponse> {
  // TODO(export): add minimal DOCX writer only after formatting parity tests cover markdown/text/html.
  const parsed = draftExportRequestSchema.parse(input);
  const format = parsed.format;
  const ext = format === "markdown" ? "md" : format === "html" ? "html" : "txt";
  const generatedAt = new Date().toISOString();

  if (parsed.kind === "conclusions") {
    const payload = parsed.conclusions as DraftConclusionsResponse;
    const title = parsed.document_title || "Conclusions of Law Draft";
    const content =
      renderReviewPackage(payload, title, format);

    return draftExportResponseSchema.parse({
      kind: parsed.kind,
      format,
      filename: `${slugify(title || "conclusions-draft") || "conclusions-draft"}.${ext}`,
      mime_type: mimeForFormat(format),
      content,
      metadata: {
        generated_at: generatedAt,
        citation_count: payload.citations.length,
        support_item_count: payload.paragraph_support.length
      }
    });
  }

  const payload = parsed.template as DraftTemplateResponse;
  const title = parsed.document_title || payload.template_title || "Decision Template";
  const content =
    format === "markdown" ? markdownFromTemplate(payload, title) : format === "html" ? htmlFromTemplate(payload, title) : textFromTemplate(payload, title);

  return draftExportResponseSchema.parse({
    kind: parsed.kind,
    format,
    filename: `${slugify(title || "decision-template") || "decision-template"}.${ext}`,
    mime_type: mimeForFormat(format),
    content,
    metadata: {
      generated_at: generatedAt,
      citation_count: payload.citations.length,
      support_item_count: payload.template_sections.length
    }
  });
}
