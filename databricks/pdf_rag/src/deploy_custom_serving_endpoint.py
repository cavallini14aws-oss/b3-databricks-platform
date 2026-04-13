from __future__ import annotations

import os
from pathlib import Path

import requests
import yaml


def load_config() -> dict:
    path = Path("databricks/pdf_rag/config/pdf_rag_custom_serving.yml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))["custom_serving"]


def main() -> None:
    cfg = load_config()

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    model_version = os.getenv("PDF_RAG_CUSTOM_MODEL_VERSION", "1")

    if not host:
        raise ValueError("DATABRICKS_HOST não configurado")
    if not token:
        raise ValueError("DATABRICKS_TOKEN não configurado")

    url = f"{host}/api/2.0/serving-endpoints"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "name": cfg["endpoint_name"],
        "config": {
            "served_entities": [
                {
                    "name": cfg["served_model_name"],
                    "entity_name": cfg["uc_model_name"],
                    "entity_version": model_version,
                    "workload_size": cfg["workload_size"],
                    "scale_to_zero_enabled": cfg["scale_to_zero_enabled"],
                }
            ],
            "traffic_config": {
                "routes": [
                    {
                        "served_model_name": cfg["served_model_name"],
                        "traffic_percentage": cfg["traffic_percentage"],
                    }
                ]
            },
        },
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    print(response.text)


if __name__ == "__main__":
    main()
