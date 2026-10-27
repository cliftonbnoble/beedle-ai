import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from '../../api/tests/helpers/load-ts.mjs';
const { matchedPreviewChunks, parseParagraphAnchor } = loadTs('../web/src/lib/decision-passages.ts');
const chunks = [
 { documentId: 'A', chunkId: 'reader-one', paragraphAnchorStart: 'facts-p0', paragraphAnchorEnd: 'facts-p2' },
 { documentId: 'A', chunkId: 'reader-two', paragraphAnchorStart: 'law-p0', paragraphAnchorEnd: 'law-p1' },
 { documentId: 'B', chunkId: 'other-document', paragraphAnchorStart: 'facts-p0', paragraphAnchorEnd: 'facts-p2' }
];
test('reader matches source anchors across chunk profiles and includes authority and supporting passages', () => {
 const rows = [{ documentId: 'A', chunkId: 'source-chunk', paragraphAnchor: 'facts-p1', primaryAuthorityPassage: { chunkId: 'authority', paragraphAnchor: 'law-p0' } }];
 assert.deepEqual([...matchedPreviewChunks(chunks, rows, 'A')].sort(), ['reader-one', 'reader-two']);
 assert.deepEqual([...matchedPreviewChunks(chunks, [{ documentId: 'A', chunkId: 'reader-two', paragraphAnchor: 'old-anchor', supportingFactPassage: { chunkId: 'fact', paragraphAnchor: 'facts-p2' } }], 'A')].sort(), ['reader-one', 'reader-two']);
});
test('reader does not match other documents, unrelated prefixes, outside or unsafe anchor indices', () => {
 for (const row of [ {documentId:'B', chunkId:'source', paragraphAnchor:'facts-p1'}, {documentId:'A',chunkId:'source',paragraphAnchor:'facts-other-p1'}, {documentId:'A',chunkId:'source',paragraphAnchor:'facts-p9'}, {documentId:'A',chunkId:'source',paragraphAnchor:'facts-p9999999999999999999999'} ]) assert.equal(matchedPreviewChunks(chunks, [row], 'A').size, 0);
 assert.equal(parseParagraphAnchor('p1'), null); assert.equal(parseParagraphAnchor('section-p9007199254740992'), null);
 assert.equal(matchedPreviewChunks(chunks, [{documentId:'A',chunkId:'other-document',paragraphAnchor:'unrelated-p0'}], 'A').size, 0);
});
