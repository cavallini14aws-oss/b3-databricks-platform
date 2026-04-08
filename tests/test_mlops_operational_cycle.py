from types import SimpleNamespace

from data_platform.mlops.run_mlops_operational_cycle import build_operational_cycle_summary


def test_build_operational_cycle_summary_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.run_mlops_operational_cycle.build_mlops_readiness_report",
        lambda **kwargs: {
            "project": "clientes",
            "env": "dev",
            "schema_mlops": "clientes_mlops",
            "readiness_score": 1.0,
            "ready_items": 10,
            "total_items": 10,
        },
    )

    summary = build_operational_cycle_summary(
        spark=object(),
        project="clientes",
        use_catalog=False,
    )

    assert summary["project"] == "clientes"
    assert summary["env"] == "dev"
    assert summary["schema_mlops"] == "clientes_mlops"
    assert summary["readiness_score"] == 1.0
    assert summary["cycles"] == ["drift", "postprod", "retraining"]
