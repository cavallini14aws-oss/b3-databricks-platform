from __future__ import annotations

import requests

from pipelines.examples.pdf_rag_lab.backend.services.config import DEFAULT_LLM_MODEL


OLLAMA_URL = "http://localhost:11434/api/generate"


def build_prompt(question: str, chunks: list[dict]) -> str:
    context_blocks = []

    for idx, chunk in enumerate(chunks, start=1):
        page = chunk.get("page_number")
        text = chunk.get("chunk_text", "")
        context_blocks.append(f"[trecho={idx} | page={page}]\n{text}")

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


def generate_answer(question: str, chunks: list[dict], model_name: str = DEFAULT_LLM_MODEL) -> str:
    prompt = build_prompt(question, chunks)

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
