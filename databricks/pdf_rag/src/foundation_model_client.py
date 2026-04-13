from __future__ import annotations

import os
import requests


def invoke_foundation_model(prompt: str) -> str:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    endpoint = os.getenv("DATABRICKS_FOUNDATION_ENDPOINT")

    if not host:
        raise ValueError("DATABRICKS_HOST não configurado")
    if not token:
        raise ValueError("DATABRICKS_TOKEN não configurado")
    if not endpoint:
        raise ValueError("DATABRICKS_FOUNDATION_ENDPOINT não configurado")

    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()

    choices = data.get("choices", [])
    if not choices:
        raise ValueError("Resposta vazia do endpoint")
    return choices[0].get("message", {}).get("content", "").strip()
