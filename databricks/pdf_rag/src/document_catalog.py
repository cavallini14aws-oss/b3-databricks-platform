from __future__ import annotations

import yaml
from pathlib import Path


CATALOG_FILE = Path(__file__).resolve().parents[1] / "config" / "pdf_rag_document_catalog.yml"


def load_document_catalog() -> list[dict]:
    if not CATALOG_FILE.exists():
        raise FileNotFoundError(f"Catalog file not found: {CATALOG_FILE}")

    data = yaml.safe_load(CATALOG_FILE.read_text(encoding="utf-8")) or {}
    documents = data.get("documents", [])

    if not isinstance(documents, list):
        raise ValueError("Invalid catalog format: 'documents' must be a list")

    return documents
