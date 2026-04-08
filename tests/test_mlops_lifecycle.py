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

    governance_statuses = []

    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.log_governance_run",
        lambda **kwargs: governance_statuses.append(kwargs["status"]),
    )
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
    assert governance_statuses == ["STARTED", "SUCCESS"]


def test_prepare_rollback_request_fails_when_no_active_deployment(monkeypatch):
    governance_statuses = []

    monkeypatch.setattr(
        "data_platform.governance.rollback.log_governance_run",
        lambda **kwargs: governance_statuses.append(kwargs["status"]),
    )
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

    assert governance_statuses == ["STARTED", "ERROR"]


def test_promote_and_deploy_ml_blocks_invalid_status(monkeypatch):
    entry = {
        "model_version": "v999",
        "artifact_path": "/tmp/model",
        "status": "INVALID_STATUS",
        "run_id": "run-x",
    }

    governance_statuses = []

    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.log_governance_run",
        lambda **kwargs: governance_statuses.append(kwargs["status"]),
    )
    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.resolve_model_entry",
        lambda spark, request: entry,
    )
    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.get_active_model_deployment",
        lambda **kwargs: None,
    )

    with pytest.raises(ValueError, match="Status invalido para promocao"):
        promote_and_deploy_ml(
            spark=object(),
            model_name="clientes_status_classifier",
            source_env="hml",
            target_env="prd",
            model_version="v999",
            project="clientes",
            use_catalog=False,
        )

    assert governance_statuses == ["STARTED", "ERROR"]


def test_promote_and_deploy_ml_idempotent_path_does_not_activate(monkeypatch):
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

    activation_calls = {"count": 0}
    governance_statuses = []

    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.log_governance_run",
        lambda **kwargs: governance_statuses.append(kwargs["status"]),
    )
    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.resolve_model_entry",
        lambda spark, request: entry,
    )
    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.get_active_model_deployment",
        lambda **kwargs: active,
    )
    monkeypatch.setattr(
        "data_platform.governance.promote_and_deploy_ml.activate_model_deployment",
        lambda **kwargs: activation_calls.__setitem__("count", activation_calls["count"] + 1),
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

    assert result["message"] == "model already active in target environment"
    assert activation_calls["count"] == 0
    assert governance_statuses == ["STARTED", "SUCCESS"]


def test_prepare_rollback_request_executes_when_previous_candidate_exists(monkeypatch):
    active = {
        "model_name": "clientes_status_classifier",
        "model_version": "new-version",
        "artifact_path": "/tmp/new-model",
        "source_env": "hml",
        "target_env": "prd",
        "run_id": "run-new",
    }

    previous_candidate = {
        "model_name": "clientes_status_classifier",
        "model_version": "old-version",
        "artifact_path": "/tmp/old-model",
        "source_env": "hml",
        "target_env": "prd",
        "run_id": "run-old",
    }

    activation_calls = {"count": 0}
    status_updates = []

    governance_statuses = []

    monkeypatch.setattr(
        "data_platform.governance.rollback.log_governance_run",
        lambda **kwargs: governance_statuses.append(kwargs["status"]),
    )
    monkeypatch.setattr(
        "data_platform.governance.rollback.get_active_model_deployment",
        lambda **kwargs: active,
    )
    monkeypatch.setattr(
        "data_platform.governance.rollback._get_previous_candidate",
        lambda **kwargs: previous_candidate,
    )
    monkeypatch.setattr(
        "data_platform.governance.rollback.activate_model_deployment",
        lambda **kwargs: activation_calls.__setitem__("count", activation_calls["count"] + 1),
    )
    monkeypatch.setattr(
        "data_platform.governance.rollback.update_model_status",
        lambda **kwargs: status_updates.append(kwargs),
    )

    result = prepare_rollback_request(
        spark=object(),
        model_name="clientes_status_classifier",
        target_env="prd",
        project="clientes",
        use_catalog=False,
    )

    assert result["action"] == "ROLLBACK_EXECUTED"
    assert result["rolled_back_from_version"] == "new-version"
    assert result["rolled_back_to_version"] == "old-version"
    assert result["artifact_path"] == "/tmp/old-model"
    assert activation_calls["count"] == 1
    assert len(status_updates) == 1
    assert status_updates[0]["model_version"] == "new-version"
    assert governance_statuses == ["STARTED", "SUCCESS"]


def test_run_clientes_ml_end_to_end_executes_observable_ml_flow(monkeypatch):
    from pipelines.examples.clientes.ml.run_clientes_ml_end_to_end import (
        run_clientes_ml_end_to_end,
    )

    call_order = []
    captured = {}

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            use_catalog=False,
            schema_mlops="clientes_mlops",
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_context",
        lambda project, use_catalog: fake_ctx,
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_with_observability",
        lambda **kwargs: kwargs["fn"](SimpleNamespace(info=lambda *args, **kwargs: None)),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_prepare_clientes_training_dataset",
        lambda **kwargs: call_order.append("prepare_training"),
    )

    def fake_train(**kwargs):
        call_order.append("train")
        return "model-version-123"

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_train_clientes_model",
        fake_train,
    )

    def fake_evaluate(**kwargs):
        call_order.append("evaluate")
        captured["evaluate_model_version"] = kwargs["model_version"]

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_evaluate_clientes_model",
        fake_evaluate,
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_prepare_clientes_scoring_dataset",
        lambda **kwargs: call_order.append("prepare_scoring"),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_scoring_dataset_table",
        lambda **kwargs: "clientes_feature.tb_clientes_scoring_dataset_v2",
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_model_artifact_path",
        lambda **kwargs: "/Volumes/workspace/customer_intelligence/vol_ml_artifacts/clientes_status_classifier/model-version-123",
    )

    def fake_batch_inference(**kwargs):
        call_order.append("batch_inference")
        captured["batch_model_version"] = kwargs["model_version"]
        captured["batch_input_table"] = kwargs["input_table"]
        captured["batch_output_table"] = kwargs["output_table"]
        captured["batch_target_env"] = kwargs["target_env"]
        captured["batch_artifact_path"] = kwargs["artifact_path"]

    smoke_statuses = []

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.log_smoke_run",
        lambda **kwargs: smoke_statuses.append(kwargs["status"]),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_batch_inference",
        fake_batch_inference,
    )

    run_clientes_ml_end_to_end(
        spark=object(),
        project="clientes",
        use_catalog=False,
        config_path="config/clientes_ml_pipeline.yml",
        forced_run_id="runner-test",
    )

    assert call_order == [
        "prepare_training",
        "train",
        "evaluate",
        "prepare_scoring",
        "batch_inference",
    ]
    assert captured["evaluate_model_version"] == "model-version-123"
    assert captured["batch_model_version"] == "model-version-123"
    assert captured["batch_input_table"] == "clientes_feature.tb_clientes_scoring_dataset_v2"
    assert captured["batch_output_table"] == "clientes_mlops.tb_clientes_status_classifier_smoke_predictions_dev"
    assert captured["batch_target_env"] == "dev"
    assert captured["batch_artifact_path"] == "/Volumes/workspace/customer_intelligence/vol_ml_artifacts/clientes_status_classifier/model-version-123"
    assert smoke_statuses == ["STARTED", "SUCCESS"]


def test_run_clientes_ml_end_to_end_skip_train_requires_existing_model_version(monkeypatch):
    from pipelines.examples.clientes.ml.run_clientes_ml_end_to_end import (
        run_clientes_ml_end_to_end,
    )

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            use_catalog=False,
            schema_mlops="clientes_mlops",
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_context",
        lambda project, use_catalog: fake_ctx,
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_with_observability",
        lambda **kwargs: kwargs["fn"](SimpleNamespace(info=lambda *args, **kwargs: None)),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_prepare_clientes_training_dataset",
        lambda **kwargs: None,
    )

    with pytest.raises(ValueError, match="existing_model_version deve ser informado"):
        run_clientes_ml_end_to_end(
            spark=object(),
            project="clientes",
            use_catalog=False,
            config_path="config/clientes_ml_pipeline.yml",
            skip_train=True,
            existing_model_version=None,
            forced_run_id="runner-test-skip-train-missing-version",
        )


def test_run_clientes_ml_end_to_end_skip_train_reuses_existing_model_version(monkeypatch):
    from pipelines.examples.clientes.ml.run_clientes_ml_end_to_end import (
        run_clientes_ml_end_to_end,
    )

    call_order = []
    captured = {}

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            use_catalog=False,
            schema_mlops="clientes_mlops",
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_context",
        lambda project, use_catalog: fake_ctx,
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_with_observability",
        lambda **kwargs: kwargs["fn"](SimpleNamespace(info=lambda *args, **kwargs: None)),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_prepare_clientes_training_dataset",
        lambda **kwargs: call_order.append("prepare_training"),
    )

    def fake_train(**kwargs):
        call_order.append("train")
        return "should-not-be-used"

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_train_clientes_model",
        fake_train,
    )

    def fake_evaluate(**kwargs):
        call_order.append("evaluate")
        captured["evaluate_model_version"] = kwargs["model_version"]

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_evaluate_clientes_model",
        fake_evaluate,
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_prepare_clientes_scoring_dataset",
        lambda **kwargs: call_order.append("prepare_scoring"),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_scoring_dataset_table",
        lambda **kwargs: "clientes_feature.tb_clientes_scoring_dataset_v2",
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.get_model_artifact_path",
        lambda **kwargs: "/Volumes/workspace/customer_intelligence/vol_ml_artifacts/clientes_status_classifier/model-version-existing",
    )

    smoke_statuses = []

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.log_smoke_run",
        lambda **kwargs: smoke_statuses.append(kwargs["status"]),
    )

    def fake_batch_inference(**kwargs):
        call_order.append("batch_inference")
        captured["batch_model_version"] = kwargs["model_version"]
        captured["batch_artifact_path"] = kwargs["artifact_path"]

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_batch_inference",
        fake_batch_inference,
    )

    run_clientes_ml_end_to_end(
        spark=object(),
        project="clientes",
        use_catalog=False,
        config_path="config/clientes_ml_pipeline.yml",
        skip_train=True,
        existing_model_version="model-version-existing",
        forced_run_id="runner-test-skip-train",
    )

    assert call_order == [
        "prepare_training",
        "evaluate",
        "prepare_scoring",
        "batch_inference",
    ]
    assert captured["evaluate_model_version"] == "model-version-existing"
    assert captured["batch_model_version"] == "model-version-existing"
    assert captured["batch_artifact_path"] == "/Volumes/workspace/customer_intelligence/vol_ml_artifacts/clientes_status_classifier/model-version-existing"
    assert smoke_statuses == ["STARTED", "SUCCESS"]
