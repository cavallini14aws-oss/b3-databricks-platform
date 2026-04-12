from data_platform.aiops.evaluations.ai_smoke_report import build_ai_smoke_report


def test_ai_smoke_report_success():
    report = build_ai_smoke_report(
        project="clientes",
        knowledge_count=10,
        chunks_count=20,
        embeddings_count=20,
        index_count=20,
        rag_eval_count=1,
    )

    assert report["status"] == "SUCCESS"
    assert all(report["checks"].values())


def test_ai_smoke_report_failed():
    report = build_ai_smoke_report(
        project="clientes",
        knowledge_count=10,
        chunks_count=20,
        embeddings_count=0,
        index_count=0,
        rag_eval_count=0,
    )

    assert report["status"] == "FAILED"
    assert report["checks"]["knowledge_present"] is True
    assert report["checks"]["embeddings_present"] is False
