from __future__ import annotations

import os

import requests

from pipelines.examples.pdf_rag_lab.backend.services.config import DEFAULT_LLM_MODEL


def _build_context(chunks: list[dict]) -> str:
    lines = []
    for chunk in chunks:
        document_name = chunk.get("catalog_display_name") or chunk.get("document_name") or "Documento"
        page_number = chunk.get("page_number")
        chunk_text = chunk.get("chunk_text", "").strip()

        if not chunk_text:
            continue

        lines.append(
            f"[DOCUMENTO: {document_name} | PÁGINA: {page_number}]\n{chunk_text}"
        )

    return "\n\n".join(lines)


def _build_prompt(question: str, chunks: list[dict]) -> str:
    context = _build_context(chunks)

    return f"""
Você é um assistente de leitura documental corporativa.

Sua tarefa é responder SOMENTE com base no contexto recuperado.
Não invente fatos.
Não extrapole além do texto.
Se o contexto for parcial ou insuficiente, diga isso explicitamente.

Regras:
1. Use apenas o contexto fornecido.
2. Se a pergunta pede introdução, tema principal, primeiras páginas ou resumo inicial, descreva apenas o que o trecho inicial recuperado sugere.
3. Se apenas um trecho curto estiver disponível, não afirme com certeza o "tema principal do livro inteiro".
4. Se houver OCR ruim ou texto truncado, reconheça a limitação.
5. Diferencie claramente:
   - o que está explícito no contexto
   - o que é apenas sugestão limitada do trecho
6. Responda em português do Brasil.
7. Seja objetivo, claro e honesto.

Pergunta:
{question}

Contexto recuperado:
{context}

Formato obrigatório da resposta:
1. O que o contexto permite afirmar com segurança
2. Limitações do contexto, se houver
3. Resposta objetiva final
""".strip()


def _generate_with_ollama(prompt: str) -> str:
    model_name = os.getenv("DEFAULT_LLM_MODEL", DEFAULT_LLM_MODEL)

    response = requests.post(
        "http://127.0.0.1:11434/api/generate",
        json={
            "model": model_name,
            "prompt": prompt,
            "stream": False,
        },
        timeout=180,
    )
    response.raise_for_status()
    return response.json()["response"].strip()


def _generate_with_databricks(prompt: str) -> str:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    endpoint = os.getenv("DATABRICKS_SERVING_ENDPOINT")

    if not host:
        raise ValueError("DATABRICKS_HOST não configurado.")
    if not token:
        raise ValueError("DATABRICKS_TOKEN não configurado.")
    if not endpoint:
        raise ValueError("DATABRICKS_SERVING_ENDPOINT não configurado.")

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

    response = requests.post(url, headers=headers, json=payload, timeout=180)
    response.raise_for_status()
    data = response.json()

    choices = data.get("choices", [])
    if not choices:
        raise ValueError("Resposta vazia do endpoint Databricks.")

    message = choices[0].get("message", {})
    content = message.get("content", "").strip()
    if not content:
        raise ValueError("Conteúdo vazio retornado pelo endpoint Databricks.")

    return content


def generate_answer(question: str, chunks: list[dict]) -> str:
    if not chunks:
        return "Não encontrei contexto suficiente para responder com segurança."

    prompt = _build_prompt(question, chunks)
    provider = os.getenv("LLM_PROVIDER", "ollama").strip().lower()

    if provider == "databricks":
        return _generate_with_databricks(prompt)

    return _generate_with_ollama(prompt)
