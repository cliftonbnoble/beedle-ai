import type { canonicalIndexCodeOptions } from "@beedle/shared";

type IndexCodeOption = (typeof canonicalIndexCodeOptions)[number];

// Single canonical copy of the previously-triplicated helper. Keeps the strongest variant's behavior:
// when the same code appears twice, prefer the entry with a real description over a "[reserved]" stub
// (the catalog is deduped at source since REPO-02, so this is a safety net, not a hot path).
export function dedupeIndexCodeOptions(options: readonly IndexCodeOption[]): IndexCodeOption[] {
  const byCode = new Map<string, IndexCodeOption>();
  for (const option of options) {
    const existing = byCode.get(option.code);
    if (!existing) {
      byCode.set(option.code, option);
      continue;
    }
    const existingReserved = existing.description.toLowerCase().includes("[reserved]");
    const nextReserved = option.description.toLowerCase().includes("[reserved]");
    if (existingReserved && !nextReserved) {
      byCode.set(option.code, option);
    }
  }
  return Array.from(byCode.values());
}

// Clipboard writes reject when permission is denied (or in insecure contexts); callers show feedback
// only on success instead of surfacing an unhandled rejection.
export async function copyTextToClipboard(value: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(value);
    return true;
  } catch {
    return false;
  }
}

// Blob download that matches fetchJson's transport semantics (credentials included) — the previous
// page-local copy silently dropped credentials and used a different error format.
export async function downloadFromUrl(url: string, fallbackFilename: string): Promise<void> {
  const response = await fetch(url, { credentials: "include" });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Export failed (${response.status}): ${text}`);
  }
  const blob = await response.blob();
  const href = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  const disposition = response.headers.get("content-disposition") || "";
  const matched = disposition.match(/filename="?([^";]+)"?/i);
  anchor.href = href;
  anchor.download = matched?.[1] || fallbackFilename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(href);
}
