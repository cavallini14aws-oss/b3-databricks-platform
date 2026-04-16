from __future__ import annotations

from pathlib import Path

import yaml
from databricks.vector_search.client import VectorSearchClient

from src.foundation_model_client import invoke_foundation_model


def load_vector_config() -> dict:
    path = Path(__file__).resolve().parents[1] / "config" / "pdf_rag_vector_search.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["vector_search"]


def retrieve_chunks(question: str, num_results: int = 5) -> list[dict]:
    cfg = load_vector_config()
    client = VectorSearchClient()

    index = client.get_index(
        endpoint_name=cfg["endpoint_name"],
        index_name=cfg["index_name"],
    )

    results = index.similarity_search(
        query_text=question,
        columns=cfg["query_columns"],
        num_results=num_results,
        query_type="hybrid",
    )

    rows = results.get("result", {}).get("data_array", [])
    columns = results.get("manifest", {}).get("columns", [])
    column_names = [col["name"] for col in columns]

    return [dict(zip(column_names, row)) for row in rows]


def build_context(chunks: list[dict]) -> str:
    parts = []

    for idx, chunk in enumerate(chunks, start=1):
        file_name = chunk.get("file_name", "")
        page_number = chunk.get("page_number", "")
        chunk_text = chunk.get("chunk_text", "")

        parts.append(
            f"[Trecho {idx} | arquivo={file_name} | página={page_number}]\n{chunk_text}"
        )

    return "\n\n".join(parts)


def answer_question_ptbr(question: str, num_results: int = 5) -> dict:
    chunks = retrieve_chunks(question=question, num_results=num_results)
    context = build_context(chunks)

    system_prompt = """
Você é um assistente RAG especializado em C. G. Jung.
Responda sempre em português brasileiro.
Use apenas os trechos recuperados como base principal.
Se os trechos estiverem em inglês, traduza e sintetize em português.
Não invente fonte, página ou citação.
Se a evidência for insuficiente, diga isso com clareza.
""".strip()

    prompt = f"""
Pergunta:
{question}

Trechos recuperados:
{context}

Responda em português brasileiro, com síntese clara e referências aos trechos/páginas quando possível.
""".strip()

    answer = invoke_foundation_model(
        prompt=prompt,
        system_prompt=system_prompt,
        max_tokens=900,
        temperature=0.1,
    )

    return {
        "question": question,
        "answer": answer,
        "retrieved_chunks": chunks,
    }


def main(question: str) -> None:
    result = answer_question_ptbr(question)
    print("[OK] RAG PT-BR answer generated")
    print(result["answer"])


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        raise SystemExit("Uso: python rag_answer_ptbr.py \"sua pergunta\"")

    main(sys.argv[1])
