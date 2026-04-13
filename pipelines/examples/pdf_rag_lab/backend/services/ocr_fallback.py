from __future__ import annotations

import io

import fitz
from PIL import Image
import pytesseract


def render_pdf_page_to_image(pdf_path: str, page_index: int, zoom: float = 2.0) -> Image.Image:
    doc = fitz.open(pdf_path)
    page = doc.load_page(page_index)
    matrix = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(io.BytesIO(pix.tobytes("png")))
    doc.close()
    return image


def extract_text_with_ocr(
    pdf_path: str,
    page_index: int,
    lang: str = "eng",
    timeout_seconds: int = 12,
) -> str:
    image = render_pdf_page_to_image(pdf_path, page_index)
    try:
        text = pytesseract.image_to_string(image, lang=lang, timeout=timeout_seconds)
    except RuntimeError:
        return ""
    return text.strip()
