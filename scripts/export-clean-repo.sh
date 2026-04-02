#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-$SRC_DIR/../beedle-ai-clean}"

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync is required but not installed." >&2
  exit 1
fi

if [ -e "$DEST_DIR" ]; then
  echo "Destination already exists: $DEST_DIR" >&2
  echo "Remove it or pass a different destination path." >&2
  exit 1
fi

mkdir -p "$DEST_DIR"

rsync -a \
  --exclude ".git" \
  --exclude "node_modules" \
  --exclude ".env" \
  --exclude ".env.*" \
  --exclude ".DS_Store" \
  --exclude ".vscode" \
  --exclude ".idea" \
  --exclude "dist" \
  --exclude "build" \
  --exclude ".turbo" \
  --exclude ".vercel" \
  --exclude ".wrangler" \
  --exclude "apps/web/.next" \
  --exclude "apps/web/.vercel" \
  --exclude "apps/api/.wrangler" \
  --exclude "apps/api/backups" \
  --exclude "apps/api/reports" \
  --exclude "coverage" \
  --exclude "import-batches" \
  --exclude "*.sqlite" \
  --exclude "*.sqlite-*" \
  --exclude "*.db" \
  --exclude "*.db-*" \
  --exclude "*.bak" \
  --exclude "*.log" \
  --exclude "pnpm-debug.log*" \
  --exclude "npm-debug.log*" \
  --exclude "yarn-debug.log*" \
  --exclude "yarn-error.log*" \
  "$SRC_DIR/" "$DEST_DIR/"

echo "Clean repo export created at:"
echo "  $DEST_DIR"
echo
echo "Next steps:"
echo "  cd \"$DEST_DIR\""
echo "  git init"
echo "  git add ."
echo "  git status --short"
