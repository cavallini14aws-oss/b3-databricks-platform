from types import SimpleNamespace

from data_platform.mlops.readiness_report import (
    READINESS_ITEMS,
    build_mlops_readiness_report,
)


def test_readiness_items_not_empty():
    assert len(READINESS_ITEMS) > 0


def test_build_mlops_readiness_report_returns_expected_shape(monkeypatch):
    existing_tables = {
        "clientes_mlops.tb_model_registry",
        "clientes_mlops.tb_model_deployments",
        "clientes_mlops.tb_model_evaluation",
    }

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            schema_mlops="clientes_mlops",
            qualified_schema=lambda schema: schema,
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    class FakeCatalog:
        @staticmethod
        def tableExists(name):
            return name in existing_tables

    class FakeSpark:
        catalog = FakeCatalog()

    monkeypatch.setattr(
        "data_platform.mlops.readiness_report.get_context",
        lambda project, use_catalog: fake_ctx,
    )

    report = build_mlops_readiness_report(
        spark=FakeSpark(),
        project="clientes",
        use_catalog=False,
    )

    assert report["project"] == "clientes"
    assert report["env"] == "dev"
    assert report["schema_mlops"] == "clientes_mlops"
    assert report["ready_items"] == 3
    assert report["total_items"] == len(READINESS_ITEMS)
    assert report["readiness_score"] == round(3 / len(READINESS_ITEMS), 4)
    assert report["checks"]["model_registry"]["exists"] is True
    assert report["checks"]["drift_monitoring"]["exists"] is False
