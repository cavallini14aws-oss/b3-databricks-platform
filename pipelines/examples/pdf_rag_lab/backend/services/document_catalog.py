from __future__ import annotations

from pathlib import Path

import yaml

from pipelines.examples.pdf_rag_lab.backend.services.text_normalization import normalize_text


CATALOG_PATH = Path("config/pdf_rag_document_catalog.yml")


def load_document_catalog() -> list[dict]:
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(f"Catálogo não encontrado: {CATALOG_PATH}")

    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    documents = data.get("documents", [])

    normalized_docs = []
    for item in documents:
        aliases = item.get("aliases", [])
        normalized_docs.append(
            {
                "document_id": item["document_id"],
                "display_name": item["display_name"],
                "source_file_name": item["source_file_name"],
                "aliases": [normalize_text(alias) for alias in aliases],
                "display_name_normalized": normalize_text(item["display_name"]),
            }
        )

    return normalized_docs


def build_catalog_alias_map() -> dict[str, list[str]]:
    alias_map = {}
    for item in load_document_catalog():
        alias_map[item["display_name_normalized"]] = list(
            {
                item["display_name_normalized"],
                *item["aliases"],
            }
        )
    return alias_map
