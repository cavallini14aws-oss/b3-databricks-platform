from __future__ import annotations

import requests

from pipelines.examples.pdf_rag_lab.backend.services.config import DEFAULT_LLM_MODEL


OLLAMA_URL = "http://localhost:11434/api/generate"


def build_prompt(question: str, chunks: list[dict]) -> str:
    context_blocks = []

    for chunk in chunks:
        page = chunk.get("page_number")
        text = chunk.get("chunk_text", "")
        context_blocks.append(f"[page={page}] {text}")

    context = "\n\n".join(context_blocks)

    return f"""
Você é um assistente de RAG.
Responda usando apenas o contexto abaixo.
Se a resposta não estiver no contexto, diga que não encontrou base suficiente.

Contexto:
{context}

Pergunta:
{question}

Resposta:
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
        timeout=120,
    )
    response.raise_for_status()

    data = response.json()
    return data.get("response", "").strip()
