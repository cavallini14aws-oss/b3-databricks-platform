from __future__ import annotations

import re


def estimate_ocr_noise_score(text: str) -> float:
    if not text:
        return 1.0

    total = len(text)
    weird_chars = len(re.findall(r"[�呸<>\\\\/_]{1,}", text))
    broken_words = len(re.findall(r"\b\w*[0-9][A-Za-zÀ-ÿ]*\b", text))
    punctuation_runs = len(re.findall(r"[\"'.,;:!?()\[\]\-]{3,}", text))
    isolated_symbols = len(re.findall(r"\b[^A-Za-zÀ-ÿ0-9\s]{1,2}\b", text))
    upper_noise = len(re.findall(r"\b[A-ZÀ-Ý]{4,}\b", text))

    score = (
        weird_chars * 2.0 +
        broken_words * 1.5 +
        punctuation_runs * 1.5 +
        isolated_symbols * 1.0 +
        upper_noise * 0.5
    ) / max(total, 1)

    return score


def has_minimum_text_density(text: str, min_alpha_ratio: float = 0.55) -> bool:
    if not text:
        return False

    alpha_count = len(re.findall(r"[A-Za-zÀ-ÿ]", text))
    return (alpha_count / max(len(text), 1)) >= min_alpha_ratio


def is_low_quality_ocr(text: str, threshold: float = 0.06) -> bool:
    if not has_minimum_text_density(text):
        return True
    return estimate_ocr_noise_score(text) >= threshold
