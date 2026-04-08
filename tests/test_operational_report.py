from types import SimpleNamespace

from data_platform.mlops.operational_report import (
    DOC_ARTIFACTS,
    ENTRYPOINT_ARTIFACTS,
    PIPELINE_ARTIFACTS,
    build_mlops_operational_report,
)


def test_operational_report_artifact_lists_not_empty():
    assert len(DOC_ARTIFACTS) > 0
    assert len(ENTRYPOINT_ARTIFACTS) > 0
    assert len(PIPELINE_ARTIFACTS) > 0


def test_build_mlops_operational_report_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.operational_report.build_mlops_readiness_report",
        lambda **kwargs: {
            "project": "clientes",
            "env": "dev",
            "schema_mlops": "clientes_mlops",
            "readiness_score": 1.0,
            "ready_items": 10,
            "total_items": 10,
            "checks": {
                "model_registry": {
                    "table_name": "clientes_mlops.tb_model_registry",
                    "exists": True,
                }
            },
        },
    )

    monkeypatch.setattr(
        "data_platform.mlops.operational_report._artifact_exists",
        lambda path: True,
    )

    report = build_mlops_operational_report(
        spark=object(),
        project="clientes",
        use_catalog=False,
    )

    assert report["project"] == "clientes"
    assert report["env"] == "dev"
    assert report["schema_mlops"] == "clientes_mlops"
    assert report["readiness_score"] == 1.0
    assert report["docs_ready"] == len(DOC_ARTIFACTS)
    assert report["entrypoints_ready"] == len(ENTRYPOINT_ARTIFACTS)
    assert report["pipelines_ready"] == len(PIPELINE_ARTIFACTS)
    assert report["table_checks"]["model_registry"]["exists"] is True
