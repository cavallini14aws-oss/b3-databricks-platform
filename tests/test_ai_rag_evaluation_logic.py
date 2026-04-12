from data_platform.aiops.evaluations.ai_operational_report import build_ai_operational_report


def test_rag_evaluation_logic_matches_counts():
    report = build_ai_operational_report(
        project="clientes",
        chunks_count=50,
        embeddings_count=50,
        index_count=50,
    )

    assert report["project"] == "clientes"
    assert report["chunks_count"] == 50
    assert report["embeddings_count"] == 50
    assert report["index_count"] == 50
    assert report["status"] == "OK"
