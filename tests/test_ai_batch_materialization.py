from pipelines.template.ai.batch.chunk_documents import _chunk_text
from pipelines.template.ai.batch.build_embeddings import _fake_embedding


def test_chunk_text_returns_chunks():
    text = "abcdefghij" * 100
    chunks = _chunk_text(text, chunk_size=100, chunk_overlap=20)

    assert len(chunks) > 1
    assert all(isinstance(chunk, str) for chunk in chunks)
    assert all(len(chunk) <= 100 for chunk in chunks)


def test_fake_embedding_is_deterministic():
    emb1 = _fake_embedding("texto de teste")
    emb2 = _fake_embedding("texto de teste")
    emb3 = _fake_embedding("texto diferente")

    assert emb1 == emb2
    assert emb1 != emb3
    assert len(emb1) == 8
