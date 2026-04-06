from types import SimpleNamespace

import pytest

from data_platform.governance.promote_and_deploy_ml import (
    validate_promotion_path,
    promote_and_deploy_ml,
)
from data_platform.governance.rollback import prepare_rollback_request
from data_platform.mlops.deployments import get_active_model_for_env


class FakeFilteredResult:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def orderBy(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def collect(self):
        return self._rows


class FakeSparkNoTable:
    class catalog:
        @staticmethod
        def tableExists(name):
            return False


class FakeSparkWithTable:
    def __init__(self, rows):
        self._rows = rows

        class Catalog:
            @staticmethod
            def tableExists(name):
                return True

        self.catalog = Catalog()

    def table(self, name):
        return FakeFilteredResult(self._rows)


def test_validate_promotion_path_accepts_valid_paths():
    validate_promotion_path("dev", "hml")
    validate_promotion_path("hml", "prd")


def test_validate_promotion_path_blocks_invalid_path():
    with pytest.raises(ValueError, match="Caminho de promocao invalido"):
        validate_promotion_path("dev", "prd")


def test_get_active_model_for_env_returns_none_when_table_does_not_exist():
    result = get_active_model_for_env(
        spark=FakeSparkNoTable(),
        model_name="clientes_status_classifier",
        target_env="hml",
        project="clientes",
        use_catalog=False,
    )

    assert result is None


def test_get_active_model_for_env_returns_expected_payload(monkeypatch):
    fake_row = {
        "model_name": "clientes_status_classifier",
        "model_version": "v123",
        "artifact_path": "/tmp/model",
        "source_env": "dev",
        "target_env": "hml",
        "deployment_status": "DEPLOYED_HML",
        "run_id": "run-1",
        "event_timestamp": "2026-04-06T00:00:00",
    }

    monkeypatch.setattr(
        "data_platform.mlops.deployments.get_active_model_deployment",
        lambda **kwargs: fake_row,
    )

    result = get_active_model_for_env(
        spark=object(),
        model_name="clientes_status_classifier",
        target_env="hml",
        project="clientes",
        use_catalog=False,
    )

    assert result is not None
    assert result["model_name"] == "clientes_status_classifier"
    assert result["model_version"] == "v123"
    assert result["artifact_path"] == "/tmp/model"
    assert result["target_env"] == "hml"
    assert result["deployment_status"] == "DEPLOYED_HML"


def test_promote_and_deploy_ml_returns_early_when_model_already_active(monkeypatch):
    entry = {
        "model_version": "v123",
        "artifact_path": "/tmp/model",
        "status": "DEPLOYED_PRD",
        "run_id": "run-1",
    }

    active = {
        "model_version": "v123",
        "is_active": True,
        "deployment_status": "DEPLOYED_PRD",
    }

    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.resolve_model_entry",
        lambda spark, request: entry,
    )
    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.get_active_model_deployment",
        lambda **kwargs: active,
    )

    result = promote_and_deploy_ml(
        spark=object(),
        model_name="clientes_status_classifier",
        source_env="hml",
        target_env="prd",
        model_version="v123",
        project="clientes",
        use_catalog=False,
    )

    assert result["model_version"] == "v123"
    assert result["target_env"] == "prd"
    assert result["status"] == "DEPLOYED_PRD"
    assert result["message"] == "model already active in target environment"


def test_prepare_rollback_request_fails_when_no_active_deployment(monkeypatch):
    monkeypatch.setattr(
        "data_platform.governance.rollback.get_active_model_deployment",
        lambda **kwargs: None,
    )

    with pytest.raises(ValueError, match="Nenhum deployment ativo encontrado"):
        prepare_rollback_request(
            spark=object(),
            model_name="clientes_status_classifier",
            target_env="prd",
            project="clientes",
            use_catalog=False,
        )
