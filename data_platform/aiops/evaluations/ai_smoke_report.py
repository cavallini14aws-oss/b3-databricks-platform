def build_ai_smoke_report(
    *,
    project: str,
    knowledge_count: int,
    chunks_count: int,
    embeddings_count: int,
    index_count: int,
    rag_eval_count: int,
) -> dict:
    checks = {
        "knowledge_present": knowledge_count > 0,
        "chunks_present": chunks_count > 0,
        "embeddings_present": embeddings_count > 0,
        "index_present": index_count > 0,
        "rag_eval_present": rag_eval_count > 0,
    }

    status = "SUCCESS" if all(checks.values()) else "FAILED"

    return {
        "project": project,
        "knowledge_count": knowledge_count,
        "chunks_count": chunks_count,
        "embeddings_count": embeddings_count,
        "index_count": index_count,
        "rag_eval_count": rag_eval_count,
        "checks": checks,
        "status": status,
    }
