from pydantic import BaseModel


class ChatRequest(BaseModel):
    question: str
    top_k: int | None = None


class RetrievedChunk(BaseModel):
    document_id: str
    chunk_id: str
    page_number: int | None = None
    score: float
    chunk_text: str


class ChatResponse(BaseModel):
    answer: str
    chunks: list[RetrievedChunk]


class UploadResponse(BaseModel):
    message: str
    file_name: str


class HealthResponse(BaseModel):
    status: str
