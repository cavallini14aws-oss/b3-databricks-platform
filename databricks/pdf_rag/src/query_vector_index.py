from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml
from databricks.vector_search.client import VectorSearchClient


def load_config() -> dict:
    path = Path(__file__).resolve().parents[1] / "config" / "pdf_rag_vector_search.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["vector_search"]


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Uso: python databricks/pdf_rag/src/query_vector_index.py \"sua pergunta\"")

    query_text = sys.argv[1]
    cfg = load_config()
    client = VectorSearchClient()

    index = client.get_index(
        endpoint_name=cfg["endpoint_name"],
        index_name=cfg["index_name"],
    )

    results = index.similarity_search(
        query_text=query_text,
        columns=cfg["query_columns"],
        num_results=5,
        query_type="hybrid",
    )

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
