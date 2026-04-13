from __future__ import annotations

import re

from pipelines.examples.pdf_rag_lab.backend.services.text_normalization import normalize_text


NOISE_PATTERNS = [
    r"^\d+\s*",
    r"\bcarl\s+g\s+jung\b",
    r"\bc\.?\s*g\.?\s+jung\b",
    r"\bcarl\s+gustav\s+jung\b",
    r"\bcarl\b",
    r"\bgustav\b",
    r"\bcg\b",
    r"\bc\.g\.\b",
    r"\bjung\b",
    r"\bos\b",
    r"\bo\b",
    r"\ba\b",
    r"\be\b",
    r"\(1\)",
]


def build_document_aliases(document_title_normalized: str) -> list[str]:
    aliases = {document_title_normalized}

    simplified = document_title_normalized
    for pattern in NOISE_PATTERNS:
        simplified = re.sub(pattern, " ", simplified)

    simplified = re.sub(r"\s+", " ", simplified).strip()
    simplified = normalize_text(simplified)

    if simplified:
        aliases.add(simplified)

    return sorted(alias for alias in aliases if alias)
