from __future__ import annotations

import time
from pathlib import Path

import yaml
from databricks.vector_search.client import VectorSearchClient


def load_config() -> dict:
    path = Path("databricks/pdf_rag/config/pdf_rag_vector_search.yml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))["vector_search"]


def endpoint_exists(client: VectorSearchClient, endpoint_name: str) -> bool:
    try:
        client.get_endpoint(endpoint_name)
        return True
    except Exception:
        return False


def index_exists(client: VectorSearchClient, endpoint_name: str, index_name: str) -> bool:
    try:
        client.get_index(endpoint_name=endpoint_name, index_name=index_name)
        return True
    except Exception:
        return False


def ensure_endpoint(client: VectorSearchClient, cfg: dict) -> None:
    endpoint_name = cfg["endpoint_name"]
    endpoint_type = cfg["endpoint_type"]

    if endpoint_exists(client, endpoint_name):
        print(f"[OK] endpoint already exists: {endpoint_name}")
        return

    print(f"[INFO] creating endpoint: {endpoint_name}")
    client.create_endpoint(
        name=endpoint_name,
        endpoint_type=endpoint_type,
    )
    print(f"[OK] endpoint creation requested: {endpoint_name}")


def ensure_index(client: VectorSearchClient, cfg: dict) -> None:
    endpoint_name = cfg["endpoint_name"]
    index_name = cfg["index_name"]

    if index_exists(client, endpoint_name, index_name):
        print(f"[OK] index already exists: {index_name}")
        return

    print(f"[INFO] creating delta sync index: {index_name}")
    client.create_delta_sync_index(
        endpoint_name=endpoint_name,
        source_table_name=cfg["source_table_name"],
        index_name=index_name,
        pipeline_type=cfg["pipeline_type"],
        primary_key=cfg["primary_key"],
        embedding_source_column=cfg["embedding_source_column"],
        embedding_model_endpoint_name=cfg["embedding_model_endpoint_name"],
        model_endpoint_name_for_query=cfg["model_endpoint_name_for_query"],
    )
    print(f"[OK] index creation requested: {index_name}")


def trigger_sync(client: VectorSearchClient, cfg: dict) -> None:
    endpoint_name = cfg["endpoint_name"]
    index_name = cfg["index_name"]

    print(f"[INFO] triggering sync for index: {index_name}")
    index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)
    index.sync()
    print(f"[OK] sync requested: {index_name}")


def main() -> None:
    cfg = load_config()
    client = VectorSearchClient()

    print("[INFO] Vector Search bootstrap starting")
    print(f"[INFO] endpoint={cfg['endpoint_name']}")
    print(f"[INFO] index={cfg['index_name']}")
    print(f"[INFO] source_table={cfg['source_table_name']}")

    ensure_endpoint(client, cfg)
    time.sleep(5)
    ensure_index(client, cfg)
    time.sleep(5)
    trigger_sync(client, cfg)

    print("[OK] Vector Search bootstrap completed")


if __name__ == "__main__":
    main()
