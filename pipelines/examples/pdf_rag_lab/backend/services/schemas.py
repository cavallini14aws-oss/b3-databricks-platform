from pydantic import BaseModel


class ChatRequest(BaseModel):
    question: str
    top_k: int | None = None


class RetrievedChunk(BaseModel):
    document_id: str
    document_name: str | None = None
    document_title_normalized: str | None = None
    catalog_document_id: str | None = None
    catalog_display_name: str | None = None
    chunk_id: str
    page_number: int | None = None
    score: float
    chunk_text: str


class ChatResponse(BaseModel):
    answer: str
    chunks: list[RetrievedChunk]
    retrieval_mode: str | None = None
    document_filter_applied: str | None = None
    document_filters_applied: list[str] | None = None


class UploadResponse(BaseModel):
    message: str
    file_name: str


class HealthResponse(BaseModel):
    status: str
