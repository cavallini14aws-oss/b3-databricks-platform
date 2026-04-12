from data_platform.aiops.evaluations.ai_operational_report import build_ai_operational_report


def test_ai_operational_report_ok():
    report = build_ai_operational_report(
        project="clientes",
        chunks_count=100,
        embeddings_count=100,
        index_count=100,
    )

    assert report["status"] == "OK"
    assert report["embedding_coverage"] == 1.0


def test_ai_operational_report_warn():
    report = build_ai_operational_report(
        project="clientes",
        chunks_count=100,
        embeddings_count=80,
        index_count=80,
    )

    assert report["status"] == "WARN"
    assert report["embedding_coverage"] == 0.8
