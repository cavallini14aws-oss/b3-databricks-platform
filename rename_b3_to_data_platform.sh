#!/usr/bin/env bash
set -euo pipefail

if [ ! -d "data_platform" ]; then
  echo "ERRO: pasta data_platform nao encontrada"
  exit 1
fi

mv data_platform data_platform

python3 <<'PY'
from pathlib import Path

TEXT_EXTENSIONS = {
    ".py", ".yml", ".yaml", ".md", ".toml", ".txt", ".json", ".cfg", ".ini", ".sh"
}

skip_dirs = {
    ".git", ".venv", ".venv-mlops-test", "__pycache__", "dist", "build", ".pytest_cache"
}

for path in Path(".").rglob("*"):
    if not path.is_file():
        continue

    if any(part in skip_dirs for part in path.parts):
        continue

    if path.suffix.lower() not in TEXT_EXTENSIONS:
        continue

    try:
        text = path.read_text()
    except Exception:
        continue

    new_text = text.replace("data_platform", "data_platform")

    if new_text != text:
        path.write_text(new_text)
        print(f"OK: atualizado {path}")
PY

echo
echo "==> buscas residuais"
rg -n "data_platform" . || true

echo
echo "==> compile check"
python3 -m compileall data_platform pipelines config services .github || true

echo
echo "==> diff resumido"
git diff -- . ':!*.pyc'

echo
echo "==> proximos comandos"
echo 'git add .'
echo 'git commit -m "refactor: rename data_platform to data_platform"'
echo 'git push origin main'
