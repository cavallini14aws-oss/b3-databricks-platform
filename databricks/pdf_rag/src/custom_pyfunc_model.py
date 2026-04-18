from __future__ import annotations

from typing import Any

import mlflow.pyfunc
import pandas as pd

from databricks.pdf_rag.src.foundation_model_client import invoke_foundation_model
from databricks.pdf_rag.src.query_vector_index import format_context, retrieve_context


SYSTEM_PROMPT = """
Você é um assistente RAG técnico em português brasileiro.

Regras:
- Responda somente com base no contexto recuperado.
- Se o contexto não for suficiente, diga que não encontrou evidência suficiente.
- Não invente páginas, autores ou informações.
- Seja direto, claro e útil.
""".strip()


def _get_question(row: pd.Series) -> str:
    question = row.get("question")

    if question is None or not str(question).strip():
        question = row.get("prompt")

    if question is None or not str(question).strip():
        raise ValueError("Entrada inválida: informe a coluna question ou prompt")

    return str(question).strip()


def _build_rag_prompt(question: str, context_text: str) -> str:
    return f"""
Pergunta:
{question}

Contexto recuperado:
{context_text}

Responda em português brasileiro com base apenas no contexto acima.
""".strip()


class PdfRagCustomPyFunc(mlflow.pyfunc.PythonModel):
    def predict(self, context: Any, model_input: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(model_input, pd.DataFrame):
            raise TypeError("model_input deve ser um pandas DataFrame")

        outputs = []

        for _, row in model_input.iterrows():
            question = _get_question(row)

            num_results = int(row.get("num_results", 5) or 5)
            max_context_chars = int(row.get("max_context_chars", 6000) or 6000)
            max_tokens = int(row.get("max_tokens", 800) or 800)
            temperature = float(row.get("temperature", 0.1) or 0.1)

            chunks = retrieve_context(
                query_text=question,
                num_results=num_results,
            )

            context_text = format_context(
                chunks=chunks,
                max_chars=max_context_chars,
            )

            if not context_text:
                answer = "Não encontrei evidência suficiente no contexto recuperado para responder."
            else:
                rag_prompt = _build_rag_prompt(
                    question=question,
                    context_text=context_text,
                )

                answer = invoke_foundation_model(
                    prompt=rag_prompt,
                    system_prompt=SYSTEM_PROMPT,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )

            outputs.append(
                {
                    "question": question,
                    "response": answer,
                    "retrieved_chunks": len(chunks),
                }
            )

        return pd.DataFrame(outputs)
