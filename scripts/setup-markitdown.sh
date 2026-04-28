#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$ROOT/.venv-markitdown"

python3 -m venv "$VENV"
source "$VENV/bin/activate"
python -m pip install --upgrade pip
python -m pip install 'markitdown[all]'

echo
echo "MarkItDown environment ready: $VENV"
echo "Activate with: source '$VENV/bin/activate'"
