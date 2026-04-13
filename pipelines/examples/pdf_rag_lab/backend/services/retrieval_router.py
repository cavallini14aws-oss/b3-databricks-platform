from __future__ import annotations

from dataclasses import dataclass

from pipelines.examples.pdf_rag_lab.backend.services.text_normalization import normalize_text


@dataclass
class RetrievalIntent:
    mode: str
    document_hint: str | None = None
    document_hints: list[str] | None = None
    is_introductory: bool = False


COMPARE_MARKERS = [
    "compare",
    "comparar",
    "convergem",
    "divergem",
    "diferencas",
    "diferenças",
    "semelhancas",
    "semelhanças",
]

SINGLE_DOC_MARKERS = [
    "no pdf sobre",
    "no documento sobre",
    "no pdf",
    "no documento",
    "apenas o pdf",
    "somente o pdf",
]

INTRO_MARKERS = [
    "introdu",
    "tema principal",
    "resuma",
    "primeiras paginas",
    "primeiras páginas",
    "conteudo introdutorio",
    "conteúdo introdutório",
]


def _is_introductory_question(normalized_question: str) -> bool:
    return any(marker in normalized_question for marker in INTRO_MARKERS)


def detect_intent(question: str, document_alias_map: dict[str, list[str]]) -> RetrievalIntent:
    q = normalize_text(question)
    is_introductory = _is_introductory_question(q)

    matched_titles: list[str] = []
    for canonical_title, aliases in document_alias_map.items():
        for alias in aliases:
            if alias and alias in q:
                matched_titles.append(canonical_title)
                break

    matched_titles = sorted(set(matched_titles))

    for marker in COMPARE_MARKERS:
        if marker in q:
            if len(matched_titles) >= 2:
                return RetrievalIntent(
                    mode="compare_documents",
                    document_hints=matched_titles[:2],
                    is_introductory=is_introductory,
                )
            return RetrievalIntent(
                mode="compare_documents",
                document_hints=matched_titles or None,
                is_introductory=is_introductory,
            )

    if len(matched_titles) == 1:
        return RetrievalIntent(
            mode="single_document",
            document_hint=matched_titles[0],
            is_introductory=is_introductory,
        )

    if len(matched_titles) >= 2:
        return RetrievalIntent(
            mode="compare_documents",
            document_hints=matched_titles[:2],
            is_introductory=is_introductory,
        )

    for marker in SINGLE_DOC_MARKERS:
        if marker in q:
            return RetrievalIntent(mode="single_document", is_introductory=is_introductory)

    return RetrievalIntent(mode="broad_summary", is_introductory=is_introductory)
