from __future__ import annotations

import requests

from pipelines.examples.pdf_rag_lab.backend.services.config import (
    DATABRICKS_HOST,
    DATABRICKS_SERVING_ENDPOINT,
    DATABRICKS_TOKEN,
    DEFAULT_LLM_MODEL,
    LLM_PROVIDER,
    OLLAMA_URL,
)


def build_prompt(question: str, chunks: list[dict]) -> str:
    context_blocks = []

    for idx, chunk in enumerate(chunks, start=1):
        page = chunk.get("page_number")
        text = chunk.get("chunk_text", "")
        context_blocks.append(f"[trecho={idx} | pagina={page}]\n{text}")

    context = "\n\n".join(context_blocks)

    return f"""
Você é um assistente de RAG.
Use somente o contexto recuperado.
Se a base for insuficiente, diga isso claramente.
Se a pergunta pedir resumo, responda com:
1. tema principal
2. principais tópicos
3. forma como a introdução apresenta o assunto
4. uma resposta curta e objetiva

Contexto recuperado:
{context}

Pergunta:
{question}

Responda em português do Brasil.
""".strip()


def _generate_with_ollama(prompt: str, model_name: str) -> str:
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": model_name,
            "prompt": prompt,
            "stream": False,
        },
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    return data.get("response", "").strip()


def _extract_databricks_text(data: dict) -> str:
    if isinstance(data, dict):
        if "choices" in data and data["choices"]:
            choice = data["choices"][0]
            message = choice.get("message", {})
            if isinstance(message, dict):
                content = message.get("content")
                if content:
                    return str(content).strip()

        if "predictions" in data and data["predictions"]:
            first = data["predictions"][0]
            if isinstance(first, dict):
                for key in ["content", "text", "generated_text", "response"]:
                    value = first.get(key)
                    if value:
                        return str(value).strip()
            if isinstance(first, str):
                return first.strip()

        for key in ["content", "text", "generated_text", "response"]:
            value = data.get(key)
            if value:
                return str(value).strip()

    raise ValueError(f"Resposta Databricks sem conteúdo reconhecível: {data}")


def _generate_with_databricks(prompt: str, model_name: str) -> str:
    if not DATABRICKS_HOST:
        raise ValueError("DATABRICKS_HOST não configurado.")
    if not DATABRICKS_TOKEN:
        raise ValueError("DATABRICKS_TOKEN não configurado.")
    if not DATABRICKS_SERVING_ENDPOINT:
        raise ValueError("DATABRICKS_SERVING_ENDPOINT não configurado.")

    url = f"{DATABRICKS_HOST}/serving-endpoints/{DATABRICKS_SERVING_ENDPOINT}/invocations"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messages": [
            {
                "role": "system",
                "content": "Você é um assistente de RAG que responde apenas com base no contexto recuperado.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": 0.2,
        "max_tokens": 800,
        "model": model_name,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=180)
    response.raise_for_status()
    data = response.json()
    return _extract_databricks_text(data)


def generate_answer(question: str, chunks: list[dict], model_name: str = DEFAULT_LLM_MODEL) -> str:
    prompt = build_prompt(question, chunks)

    if LLM_PROVIDER == "ollama":
        return _generate_with_ollama(prompt, model_name)

    if LLM_PROVIDER == "databricks":
        return _generate_with_databricks(prompt, model_name)

    raise ValueError(f"LLM_PROVIDER inválido: {LLM_PROVIDER}")
