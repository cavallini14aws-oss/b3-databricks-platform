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

    if not host:
        raise ValueError("DATABRICKS_HOST não configurado")
    if not token:
        raise ValueError("DATABRICKS_TOKEN não configurado")

    url = f"{host}/serving-endpoints/{cfg['endpoint_name']}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "dataframe_records": [
            {"prompt": "Responda apenas OK."}
        ]
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    print(response.text)


if __name__ == "__main__":
    main()
