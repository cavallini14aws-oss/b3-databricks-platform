from __future__ import annotations

import os
from pathlib import Path

import requests
import yaml


def load_foundation_model_config() -> dict:
    path = Path(__file__).resolve().parents[1] / "config" / "pdf_rag_foundation_models.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["foundation_models"]


def _get_databricks_context_value(name: str) -> str | None:
    try:
        import IPython

        user_ns = IPython.get_ipython().user_ns
        dbutils = user_ns.get("dbutils")
        if dbutils is None:
            return None

        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

        if name == "host":
            return ctx.apiUrl().get()

        if name == "token":
            return ctx.apiToken().get()

    except Exception:
        return None

    return None


def resolve_databricks_host() -> str:
    host = os.getenv("DATABRICKS_HOST") or _get_databricks_context_value("host")

    if not host:
        raise ValueError("DATABRICKS_HOST não configurado e não resolvido via notebook context")

    if not host.startswith("http"):
        host = f"https://{host}"

    return host.rstrip("/")


def resolve_databricks_token() -> str:
    token = os.getenv("DATABRICKS_TOKEN") or _get_databricks_context_value("token")

    if not token:
        raise ValueError("DATABRICKS_TOKEN não configurado e não resolvido via notebook context")

    return token


def invoke_foundation_model(
    prompt: str,
    system_prompt: str | None = None,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> str:
    cfg = load_foundation_model_config()

    host = resolve_databricks_host()
    token = resolve_databricks_token()
    endpoint = os.getenv(cfg["endpoint_name_env"])

    if not endpoint:
        endpoint = cfg.get("default_endpoint_name")

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

    if response.status_code == 403:
        raise PermissionError(
            "Sem permissão para invocar o Foundation Model endpoint "
            f"{endpoint}. Verifique CAN QUERY/CAN USE no serving endpoint "
            "ou use uma identidade de execução autorizada."
        )

    response.raise_for_status()
    data = response.json()

    choices = data.get("choices", [])
    if not choices:
        raise ValueError("Resposta vazia do endpoint Foundation Model")

    content = choices[0].get("message", {}).get("content", "").strip()
    if not content:
        raise ValueError("Conteúdo vazio retornado pelo endpoint Foundation Model")

    return content
