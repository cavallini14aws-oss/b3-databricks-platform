def build_ai_operational_report(
    *,
    project: str,
    chunks_count: int,
    embeddings_count: int,
    index_count: int,
) -> dict:
    coverage = 0.0
    if chunks_count > 0:
        coverage = round(embeddings_count / chunks_count, 4)

    return {
        "project": project,
        "chunks_count": chunks_count,
        "embeddings_count": embeddings_count,
        "index_count": index_count,
        "embedding_coverage": coverage,
        "status": "OK" if coverage >= 1.0 else "WARN",
    }
