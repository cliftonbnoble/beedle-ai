# Markdown Corpus Conversion

This project now has a markdown-first corpus rebuild path in the clean repo.

## Source of truth

Source corpus:

```bash
/Users/cliftonnoble/Downloads/Bee’s Files 2
```

Clean repo:

```bash
/Users/cliftonnoble/Code/beedle-ai
```

## Why this path

We use Microsoft MarkItDown to turn the Word-heavy corpus into markdown because markdown is easier to inspect, easier to chunk deterministically, and generally a better source format for re-embedding and retrieval tuning.

The corpus contains many legacy `.doc` files, so the practical pipeline is:

1. Convert legacy `.doc` to staged `.docx` with macOS `textutil`
2. Convert `.docx`, `.dotx`, and `.txt` into markdown with MarkItDown
3. Preserve the original folder structure under `import-batches/markdown-corpus`
4. Import the markdown corpus into the clean dataset
5. Let the existing ingestion pipeline rebuild sections, chunks, and vectors from the markdown source

## One-time setup

```bash
cd "/Users/cliftonnoble/Code/beedle-ai"
bash scripts/setup-markitdown.sh
```

## Spot check commands

Representative small checks across older and newer folders:

```bash
cd "/Users/cliftonnoble/Code/beedle-ai"
source .venv-markitdown/bin/activate
python3 scripts/convert_corpus_to_markdown.py --source "/Users/cliftonnoble/Downloads/Bee’s Files 2/Andy_s Decisions/1998" --output-root "/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-samples/andy-1998" --stage-root "/Users/cliftonnoble/Code/beedle-ai/import-batches/legacy-docx-stage-samples/andy-1998" --report-path "/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-samples/andy-1998-report.json" --limit 10 --force
python3 scripts/convert_corpus_to_markdown.py --source "/Users/cliftonnoble/Downloads/Bee’s Files 2/Connie_s Decisions/2019 Decisions" --output-root "/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-samples/connie-2019" --stage-root "/Users/cliftonnoble/Code/beedle-ai/import-batches/legacy-docx-stage-samples/connie-2019" --report-path "/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-samples/connie-2019-report.json" --limit 10 --force
python3 scripts/convert_corpus_to_markdown.py --source "/Users/cliftonnoble/Downloads/Bee’s Files 2/Erin_s Decisions" --output-root "/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-samples/erin" --stage-root "/Users/cliftonnoble/Code/beedle-ai/import-batches/legacy-docx-stage-samples/erin" --report-path "/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-samples/erin-report.json" --limit 10 --force
```

## Full conversion

```bash
cd "/Users/cliftonnoble/Code/beedle-ai"
source .venv-markitdown/bin/activate
python3 scripts/convert_corpus_to_markdown.py --source "/Users/cliftonnoble/Downloads/Bee’s Files 2" --force
```

Outputs:

- markdown corpus:
```bash
/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus
```
- staged legacy `.docx` files:
```bash
/Users/cliftonnoble/Code/beedle-ai/import-batches/legacy-docx-stage
```
- conversion report:
```bash
/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus-report.json
```

## Markdown-first ingest path

The clean ingest service now accepts markdown source files directly. The semantic document type stays `decision_docx`, but the original stored source can now be a `.md` file with `text/markdown` content type.

That means we can keep the existing ingestion tables and chunk builder while switching the source format from Word to markdown.

## Local rebuild workflow

### 1. Apply local D1 migrations

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
pnpm wrangler d1 migrations apply beedle --local
```

### 2. Start the local API

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
pnpm wrangler dev --port 8797
```

### 3. Dry-run markdown import

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
MARKDOWN_DIR="/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus" \
API_BASE_URL="http://127.0.0.1:8797" \
MARKDOWN_DRY_RUN=1 \
MARKDOWN_LIMIT=20 \
pnpm import:markdown
```

### 4. Import markdown without vector writes during ingest

This is the recommended bulk path.

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
MARKDOWN_DIR="/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus" \
MARKDOWN_JURISDICTION="San Francisco Rent Board" \
API_BASE_URL="http://127.0.0.1:8797" \
SKIP_VECTOR_ON_INGEST=1 \
MARKDOWN_LIMIT=150 \
MARKDOWN_OFFSET=0 \
pnpm import:markdown
```

Then continue in batches:

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
MARKDOWN_DIR="/Users/cliftonnoble/Code/beedle-ai/import-batches/markdown-corpus" \
MARKDOWN_JURISDICTION="San Francisco Rent Board" \
API_BASE_URL="http://127.0.0.1:8797" \
SKIP_VECTOR_ON_INGEST=1 \
MARKDOWN_LIMIT=150 \
MARKDOWN_OFFSET=150 \
pnpm import:markdown
```

### 5. Enable searchability and activate retrieval

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
API_BASE_URL="http://127.0.0.1:8797" pnpm loop:searchability-enable
API_BASE_URL="http://127.0.0.1:8797" pnpm run:retrieval-catch-up-loop
```

### 6. Backfill vectors after the bulk import

```bash
cd "/Users/cliftonnoble/Code/beedle-ai/apps/api"
API_BASE_URL="http://127.0.0.1:8797" pnpm backfill:retrieval-vectors
```

## Recommended order of operations

1. Finish the full markdown conversion
2. Run a local markdown import for a small batch
3. Inspect QC and search results
4. Bulk import the markdown corpus with `SKIP_VECTOR_ON_INGEST=1`
5. Activate searchability and retrieval
6. Run vector backfill after the corpus is loaded
7. Run retrieval and search QA reports against the rebuilt dataset

## Notes

- The conversion script preserves folder structure.
- The markdown import script extracts the case number from markdown when available and uses it as the citation.
- The markdown parser already works better on modern `.docx` output than on the original Word files, but some metadata extraction gaps still need tuning and should be validated with QA after import.
- Generated corpus files stay under `import-batches/`, which is already gitignored.
