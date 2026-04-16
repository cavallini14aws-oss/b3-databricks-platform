from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
import yaml


RETRYABLE_STATUS_CODES = {403, 408, 429, 500, 502, 503, 504}


def load_foundation_model_config() -> dict[str, Any]:
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


def resolve_environment(cfg: dict[str, Any]) -> str:
    env_name = cfg.get("environment_env", "LOCAL_ENV")
    env = os.getenv(env_name) or cfg.get("default_environment", "dev")
    return env.lower().strip()


def resolve_environment_config(cfg: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    env = resolve_environment(cfg)
    environments = cfg.get("environments", {})

    if env not in environments:
        raise ValueError(f"Ambiente sem configuração Foundation Model: {env}")

    return env, environments[env]


def resolve_endpoint_candidates(cfg: dict[str, Any]) -> tuple[str, str, list[str]]:
    env, env_cfg = resolve_environment_config(cfg)

    strategy = env_cfg.get("endpoint_strategy", "strict")
    explicit_endpoint = os.getenv(cfg["endpoint_name_env"])

    if explicit_endpoint:
        return env, strategy, [explicit_endpoint]

    endpoint_priority = env_cfg.get("endpoint_priority") or []
    default_endpoint = env_cfg.get("default_endpoint_name")

    candidates = []

    if default_endpoint:
        candidates.append(default_endpoint)

    for endpoint in endpoint_priority:
        if endpoint not in candidates:
            candidates.append(endpoint)

    candidates = [
        endpoint
        for endpoint in candidates
        if endpoint and endpoint != "TO_BE_DEFINED"
    ]

    if not candidates:
        raise ValueError(
            f"Nenhum endpoint Foundation Model válido configurado para env={env}"
        )

    if strategy == "strict":
        return env, strategy, [candidates[0]]

    if strategy == "first_available":
        return env, strategy, candidates

    raise ValueError(f"endpoint_strategy inválida para env={env}: {strategy}")


def parse_response_content(data: dict[str, Any]) -> str:
    choices = data.get("choices", [])
    if not choices:
        raise ValueError("Resposta vazia do endpoint Foundation Model")

    message = choices[0].get("message", {})
    content = message.get("content", "")

    if isinstance(content, str):
        parsed = content.strip()
        if parsed:
            return parsed

    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict):
                item_text = item.get("text")
                if item_text:
                    text_parts.append(str(item_text))

        parsed = "\n".join(text_parts).strip()
        if parsed:
            return parsed

    raise ValueError("Conteúdo vazio retornado pelo endpoint Foundation Model")


def should_try_next_endpoint(response: requests.Response) -> bool:
    if response.status_code in RETRYABLE_STATUS_CODES:
        return True

    return False


def invoke_endpoint(
    host: str,
    token: str,
    endpoint: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    temperature: float,
    timeout_seconds: int,
) -> str:
    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)

    if response.status_code >= 400:
        error_text = response.text.strip()
        raise RuntimeError(
            f"Foundation endpoint failed endpoint={endpoint} "
            f"status_code={response.status_code} response={error_text}"
        )

    return parse_response_content(response.json())


def invoke_foundation_model(
    prompt: str,
    system_prompt: str | None = None,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> str:
    cfg = load_foundation_model_config()

    env, strategy, endpoints = resolve_endpoint_candidates(cfg)
    host = resolve_databricks_host()
    token = resolve_databricks_token()

    timeout_seconds = int(cfg["request_timeout_seconds"])
    max_tokens = max_tokens or int(cfg["default_max_tokens"])
    temperature = temperature if temperature is not None else float(cfg["default_temperature"])

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    failures = []

    for endpoint in endpoints:
        try:
            print(
                f"[INFO] invoking foundation endpoint env={env} "
                f"strategy={strategy} endpoint={endpoint}"
            )
            result = invoke_endpoint(
                host=host,
                token=token,
                endpoint=endpoint,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                timeout_seconds=timeout_seconds,
            )
            print(f"[OK] foundation endpoint used: {endpoint}")
            return result
        except Exception as exc:
            failure = f"{endpoint}: {exc}"
            failures.append(failure)

            if strategy == "strict":
                raise RuntimeError(
                    "Foundation Model invocation failed in strict mode. "
                    f"env={env} endpoint={endpoint} error={exc}"
                ) from exc

            print(f"[WARN] foundation endpoint failed, trying next. {failure}")

    raise RuntimeError(
        "Nenhum endpoint Foundation Model funcionou. "
        f"env={env} strategy={strategy} failures={failures}"
    )
