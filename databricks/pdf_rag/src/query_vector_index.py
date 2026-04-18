from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import yaml
from databricks.vector_search.client import VectorSearchClient

from databricks.pdf_rag.src.foundation_model_client import (
    resolve_databricks_host,
    resolve_databricks_token,
)


def _config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "pdf_rag_vector_search.yml"


def load_config() -> dict[str, Any]:
    path = _config_path()
    return yaml.safe_load(path.read_text(encoding="utf-8"))["vector_search"]


def create_vector_search_client() -> VectorSearchClient:
    host = resolve_databricks_host()
    token = resolve_databricks_token()

    return VectorSearchClient(
        workspace_url=host,
        personal_access_token=token,
    )


def query_vector_index(
    query_text: str,
    num_results: int = 5,
    query_type: str = "hybrid",
) -> dict[str, Any]:
    if not query_text or not query_text.strip():
        raise ValueError("query_text não pode ser vazio")

    cfg = load_config()
    client = create_vector_search_client()

    index = client.get_index(
        endpoint_name=cfg["endpoint_name"],
        index_name=cfg["index_name"],
    )

    return index.similarity_search(
        query_text=query_text,
        columns=cfg["query_columns"],
        num_results=num_results,
        query_type=query_type,
    )


def _extract_columns(results: dict[str, Any], fallback_columns: list[str]) -> list[str]:
    manifest = results.get("manifest", {})
    manifest_columns = manifest.get("columns", [])

    if manifest_columns:
        extracted = []
        for item in manifest_columns:
            if isinstance(item, dict) and item.get("name"):
                extracted.append(item["name"])
        if extracted:
            return extracted

    return fallback_columns


def _extract_data_array(results: dict[str, Any]) -> list[list[Any]]:
    result = results.get("result", {})
    data_array = result.get("data_array", [])

    if not isinstance(data_array, list):
        return []

    return data_array


def normalize_vector_results(results: dict[str, Any]) -> list[dict[str, Any]]:
    cfg = load_config()
    columns = _extract_columns(results, cfg["query_columns"])
    data_array = _extract_data_array(results)

    normalized = []
    for row in data_array:
        if not isinstance(row, list):
            continue

        item = {}
        for index, column in enumerate(columns):
            item[column] = row[index] if index < len(row) else None

        normalized.append(item)

    return normalized


def retrieve_context(
    query_text: str,
    num_results: int = 5,
    query_type: str = "hybrid",
) -> list[dict[str, Any]]:
    results = query_vector_index(
        query_text=query_text,
        num_results=num_results,
        query_type=query_type,
    )
    return normalize_vector_results(results)


def format_context(
    chunks: list[dict[str, Any]],
    max_chars: int = 6000,
) -> str:
    parts = []

    for idx, chunk in enumerate(chunks, start=1):
        file_name = chunk.get("file_name") or "documento_desconhecido"
        display_name = chunk.get("display_name") or file_name
        page_number = chunk.get("page_number")
        chunk_text = chunk.get("chunk_text") or ""

        source = f"[Fonte {idx}] {display_name}"
        if page_number is not None:
            source = f"{source}, página {page_number}"

        parts.append(f"{source}\n{chunk_text}".strip())

    context = "\n\n---\n\n".join(parts).strip()

    if len(context) > max_chars:
        return context[:max_chars].rstrip() + "\n\n[contexto truncado]"

    return context


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Uso: python databricks/pdf_rag/src/query_vector_index.py \"sua pergunta\"")

    query_text = sys.argv[1]
    results = query_vector_index(query_text=query_text)

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
