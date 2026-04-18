from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
import yaml


def _config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "pdf_rag_foundation_models.yml"


def load_foundation_model_config() -> dict[str, Any]:
    path = _config_path()
    return yaml.safe_load(path.read_text(encoding="utf-8"))["foundation_models"]


def _get_dbutils_context() -> Any | None:
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return None


def _optional_context_value(value: Any) -> str:
    try:
        if value is None:
            return ""
        if hasattr(value, "get"):
            return str(value.get())
        return str(value)
    except Exception:
        return ""


def resolve_databricks_host() -> str:
    host = os.getenv("DATABRICKS_HOST", "").strip()
    if host:
        return host.rstrip("/")

    ctx = _get_dbutils_context()
    if ctx is not None:
        api_url = _optional_context_value(getattr(ctx, "apiUrl", lambda: None)())
        if api_url:
            return api_url.rstrip("/")

        browser_host = _optional_context_value(getattr(ctx, "browserHostName", lambda: None)())
        if browser_host:
            if browser_host.startswith("http"):
                return browser_host.rstrip("/")
            return f"https://{browser_host}".rstrip("/")

    raise ValueError("DATABRICKS_HOST não configurado e não resolvido via contexto Databricks")


def resolve_databricks_token() -> str:
    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    if token:
        return token

    ctx = _get_dbutils_context()
    if ctx is not None:
        api_token = _optional_context_value(getattr(ctx, "apiToken", lambda: None)())
        if api_token:
            return api_token

    raise ValueError("DATABRICKS_TOKEN não configurado e não resolvido via contexto Databricks")


def resolve_runtime_env(cfg: dict[str, Any]) -> str:
    env_name_env = cfg.get("env_name_env", "LOCAL_ENV")
    default_env = cfg.get("default_env", "dev")
    return os.getenv(env_name_env, os.getenv("DATABRICKS_ENV", default_env)).strip() or default_env


def resolve_endpoint_candidates(cfg: dict[str, Any]) -> tuple[str, list[str], bool]:
    endpoint_env_name = cfg.get("endpoint_name_env", "DATABRICKS_FOUNDATION_ENDPOINT")
    explicit_endpoint = os.getenv(endpoint_env_name, "").strip()

    if explicit_endpoint:
        return "strict", [explicit_endpoint], True

    runtime_env = resolve_runtime_env(cfg)
    env_cfg = cfg.get("environments", {}).get(runtime_env, {})

    strategy = env_cfg.get("endpoint_strategy", "strict")
    default_endpoint = env_cfg.get("default_endpoint_name", "")
    endpoint_priority = env_cfg.get("endpoint_priority", [])

    candidates = [item for item in endpoint_priority if item and item != "TO_BE_DEFINED"]

    if default_endpoint and default_endpoint != "TO_BE_DEFINED" and default_endpoint not in candidates:
        candidates.insert(0, default_endpoint)

    if not candidates:
        raise ValueError(
            f"Nenhum endpoint Foundation Model definido para env={runtime_env}. "
            f"Configure {endpoint_env_name} ou ajuste pdf_rag_foundation_models.yml."
        )

    return strategy, candidates, False


def _extract_chat_content(data: dict[str, Any]) -> str:
    choices = data.get("choices", [])
    if not choices:
        raise ValueError("Resposta vazia do endpoint Foundation Model")

    content = choices[0].get("message", {}).get("content", "").strip()
    if not content:
        raise ValueError("Conteúdo vazio retornado pelo endpoint Foundation Model")

    return content


def _invoke_endpoint(
    endpoint: str,
    host: str,
    token: str,
    prompt: str,
    system_prompt: str | None,
    max_tokens: int,
    temperature: float,
    timeout_seconds: int,
) -> str:
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

    return _extract_chat_content(response.json())


def invoke_foundation_model(
    prompt: str,
    system_prompt: str | None = None,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> str:
    cfg = load_foundation_model_config()

    host = resolve_databricks_host()
    token = resolve_databricks_token()

    timeout_seconds = int(cfg["request_timeout_seconds"])
    resolved_max_tokens = max_tokens or int(cfg["default_max_tokens"])
    resolved_temperature = (
        float(temperature)
        if temperature is not None
        else float(cfg["default_temperature"])
    )

    strategy, candidates, explicit = resolve_endpoint_candidates(cfg)

    errors = []

    for endpoint in candidates:
        try:
            return _invoke_endpoint(
                endpoint=endpoint,
                host=host,
                token=token,
                prompt=prompt,
                system_prompt=system_prompt,
                max_tokens=resolved_max_tokens,
                temperature=resolved_temperature,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            errors.append(f"{endpoint}: {exc}")

            if explicit or strategy == "strict":
                raise

            continue

    joined_errors = "\n".join(errors)
    raise RuntimeError(
        "Nenhum endpoint Foundation Model respondeu com sucesso. "
        f"strategy={strategy}. Erros:\n{joined_errors}"
    )
