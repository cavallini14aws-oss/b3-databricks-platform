from __future__ import annotations

import hashlib
import re
import unicodedata


def normalize_text(value: str) -> str:
    value = value or ""
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower()
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_document_title(filename: str) -> str:
    base = filename.rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base)
    return normalize_text(base)


def stable_chunk_hash(document_name: str, page_number: int, chunk_text: str) -> str:
    raw = f"{document_name}|{page_number}|{normalize_text(chunk_text)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
