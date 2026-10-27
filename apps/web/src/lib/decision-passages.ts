import type { RetrievalPreviewChunk, SearchResponse } from "@beedle/shared";

export function parseParagraphAnchor(anchor: string) {
  const match = anchor.match(/^(.+)-p(\d+)$/);
  if (!match?.[1] || !match[2]) return null;
  const index = Number(match[2]);
  return Number.isSafeInteger(index) ? { prefix: match[1], index } : null;
}

// Provisional source chunks and the reconstructed reader can have different IDs.
// Resolve through their source paragraph anchors, never citation/title similarity.
export function matchedPreviewChunks(chunks: RetrievalPreviewChunk[], rows: SearchResponse["results"], documentId: string) {
  const currentChunks = chunks.filter(chunk => chunk.documentId === documentId);
  const visible = new Set(currentChunks.map(chunk => chunk.chunkId));
  const ranges = currentChunks.map(chunk => ({ chunk, start: parseParagraphAnchor(chunk.paragraphAnchorStart), end: parseParagraphAnchor(chunk.paragraphAnchorEnd) }));
  const matched = new Set<string>();
  for (const row of rows) {
    if (row.documentId !== documentId) continue;
    for (const passage of [row, row.matchedPassage, row.primaryAuthorityPassage, row.supportingFactPassage]) {
      if (!passage) continue;
      if (visible.has(passage.chunkId)) { matched.add(passage.chunkId); continue; }
      if (!passage.paragraphAnchor) continue;
      const anchor = parseParagraphAnchor(passage.paragraphAnchor);
      for (const { chunk, start, end } of ranges) {
        if (chunk.documentId !== documentId) continue;
        if (passage.paragraphAnchor === chunk.paragraphAnchorStart || passage.paragraphAnchor === chunk.paragraphAnchorEnd ||
          (anchor && start && end && anchor.prefix === start.prefix && start.prefix === end.prefix && anchor.index >= start.index && anchor.index <= end.index))
          matched.add(chunk.chunkId);
      }
    }
  }
  return matched;
}
