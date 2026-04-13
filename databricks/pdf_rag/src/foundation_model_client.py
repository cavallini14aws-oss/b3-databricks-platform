from __future__ import annotations

import os
from pathlib import Path

import requests
import yaml


def load_foundation_model_config() -> dict:
    path = Path("databricks/pdf_rag/config/pdf_rag_foundation_models.yml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))["foundation_models"]


def invoke_foundation_model(
    prompt: str,
    system_prompt: str | None = None,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> str:
    cfg = load_foundation_model_config()

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    endpoint = os.getenv(cfg["endpoint_name_env"])

    if not host:
        raise ValueError("DATABRICKS_HOST não configurado")
    if not token:
        raise ValueError("DATABRICKS_TOKEN não configurado")
    if not endpoint:
        raise ValueError(f"{cfg['endpoint_name_env']} não configurado")

    timeout_seconds = cfg["request_timeout_seconds"]
    max_tokens = max_tokens or cfg["default_max_tokens"]
    temperature = temperature if temperature is not None else cfg["default_temperature"]

    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
    response.raise_for_status()
    data = response.json()

    choices = data.get("choices", [])
    if not choices:
        raise ValueError("Resposta vazia do endpoint Foundation Model")

    content = choices[0].get("message", {}).get("content", "").strip()
    if not content:
        raise ValueError("Conteúdo vazio retornado pelo endpoint Foundation Model")

    return content
