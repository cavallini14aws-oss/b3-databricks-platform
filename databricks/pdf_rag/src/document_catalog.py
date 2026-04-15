from __future__ import annotations

import re
from pathlib import Path

import yaml


CATALOG_FILE = Path(__file__).resolve().parents[1] / "config" / "pdf_rag_document_catalog.yml"
DEFAULT_RAW_DOCS_PATH = Path("/Volumes/workspace/pdf_rag/raw_docs")


def _slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "document"


def _display_name_from_file(file_name: str) -> str:
    stem = Path(file_name).stem
    stem = stem.replace("_", " ").replace("-", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem


def _aliases_from_file(file_name: str) -> list[str]:
    display_name = _display_name_from_file(file_name)
    normalized = display_name.lower()
    aliases = {
        normalized,
        normalized.replace("c g", "cg"),
        normalized.replace("jung", "c g jung"),
    }

    if "redbook" in normalized.replace(" ", "") or "red book" in normalized:
        aliases.update(
            {
                "red book",
                "the red book",
                "liber novus",
                "livro vermelho",
                "livro vermelho em ingles",
                "livro vermelho versao ingles",
            }
        )

    return sorted(alias for alias in aliases if alias)


def _load_curated_documents() -> list[dict]:
    if not CATALOG_FILE.exists():
        return []

    data = yaml.safe_load(CATALOG_FILE.read_text(encoding="utf-8")) or {}
    documents = data.get("documents", [])

    if not isinstance(documents, list):
        raise ValueError("Invalid catalog format: 'documents' must be a list")

    return documents


def _discover_pdf_documents(raw_docs_path: str | Path) -> list[dict]:
    base_path = Path(raw_docs_path)

    if not base_path.exists():
        return []

    discovered = []
    for pdf_path in sorted(base_path.glob("*.pdf")):
        file_name = pdf_path.name
        display_name = _display_name_from_file(file_name)
        discovered.append(
            {
                "document_id": _slugify(display_name),
                "display_name": display_name,
                "source_file_name": file_name,
                "aliases": _aliases_from_file(file_name),
                "catalog_source": "auto_discovered",
            }
        )

    return discovered


def load_document_catalog(raw_docs_path: str | Path = DEFAULT_RAW_DOCS_PATH) -> list[dict]:
    curated_documents = _load_curated_documents()
    discovered_documents = _discover_pdf_documents(raw_docs_path)

    by_file_name: dict[str, dict] = {}

    for document in discovered_documents:
        by_file_name[document["source_file_name"]] = document

    for document in curated_documents:
        normalized = dict(document)
        normalized.setdefault("catalog_source", "curated")
        by_file_name[normalized["source_file_name"]] = normalized

    documents = list(by_file_name.values())

    seen_ids = set()
    for document in documents:
        original_id = str(document["document_id"])
        document_id = original_id
        suffix = 2

        while document_id in seen_ids:
            document_id = f"{original_id}_{suffix}"
            suffix += 1

        document["document_id"] = document_id
        seen_ids.add(document_id)

    return sorted(documents, key=lambda item: item["source_file_name"])
