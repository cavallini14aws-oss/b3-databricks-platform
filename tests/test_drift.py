import pytest
pytestmark = pytest.mark.heavy

import pytest

from pyspark.sql import Row

from data_platform.mlops.drift import (
    DRIFT_MONITORING_SCHEMA,
    _build_latest_feature_baseline_map,
    _build_latest_prediction_baseline_map,
    classify_drift,
    compute_relative_diff,
    resolve_drift_status,
)


def test_drift_monitoring_schema_has_expected_fields():
    field_names = [field.name for field in DRIFT_MONITORING_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "target_env",
        "run_id",
        "monitoring_type",
        "entity_name",
        "metric_name",
        "baseline_value",
        "current_value",
        "absolute_diff",
        "relative_diff",
        "drift_status",
    ]


def test_compute_relative_diff_with_non_zero_baseline():
    absolute_diff, relative_diff = compute_relative_diff(10.0, 12.0)

    assert absolute_diff == 2.0
    assert relative_diff == 0.2


def test_compute_relative_diff_with_zero_baseline():
    absolute_diff, relative_diff = compute_relative_diff(0.0, 0.25)

    assert absolute_diff == 0.25
    assert relative_diff == 0.25


def test_classify_drift_ok():
    assert classify_drift(0.05) == "OK"


def test_classify_drift_warning():
    assert classify_drift(0.15) == "WARNING"


def test_classify_drift_critical():
    assert classify_drift(0.35) == "CRITICAL"


def test_resolve_drift_status_with_missing_baseline():
    assert resolve_drift_status(None, None) == "BASELINE_MISSING"


def test_resolve_drift_status_with_real_baseline():
    assert resolve_drift_status(1.0, 0.05) == "OK"


def test_build_latest_prediction_baseline_map_keeps_first_latest_value():
    rows = [
        {"prediction_value": 1.0, "prediction_rate": 0.8},
        {"prediction_value": 1.0, "prediction_rate": 0.7},
        {"prediction_value": 0.0, "prediction_rate": 0.2},
    ]

    result = _build_latest_prediction_baseline_map(rows)

    assert result == {
        "1.0": 0.8,
        "0.0": 0.2,
    }


def test_build_latest_feature_baseline_map_keeps_first_latest_value():
    rows = [
        {
            "feature_name": "tem_file",
            "null_rate": 0.0,
            "distinct_count": 2.0,
            "mean_value": 0.5,
        },
        {
            "feature_name": "tem_file",
            "null_rate": 0.1,
            "distinct_count": 3.0,
            "mean_value": 0.6,
        },
        {
            "feature_name": "qtd_registros",
            "null_rate": 0.0,
            "distinct_count": 1.0,
            "mean_value": 1.0,
        },
    ]

    result = _build_latest_feature_baseline_map(rows)

    assert result == {
        "tem_file": {
            "null_rate": 0.0,
            "distinct_count": 2.0,
            "mean_value": 0.5,
        },
        "qtd_registros": {
            "null_rate": 0.0,
            "distinct_count": 1.0,
            "mean_value": 1.0,
        },
    }


def test_log_drift_records_emits_alert_events_when_enabled(monkeypatch):
    from types import SimpleNamespace
    from data_platform.mlops.drift import log_drift_records

    emitted = []

    class FakeWriter:
        def mode(self, *args, **kwargs):
            return self

        def option(self, *args, **kwargs):
            return self

        def saveAsTable(self, *args, **kwargs):
            return None

    class FakeDataFrame:
        @property
        def write(self):
            return FakeWriter()

    class FakeCatalog:
        @staticmethod
        def tableExists(name):
            return True

    class FakeSpark:
        catalog = FakeCatalog()

        def sql(self, query):
            return None

        def createDataFrame(self, rows, schema=None):
            return FakeDataFrame()

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            schema_mlops="clientes_mlops",
            qualified_schema=lambda schema: schema,
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    monkeypatch.setattr(
        "data_platform.mlops.drift.get_context",
        lambda project, use_catalog: fake_ctx,
    )
    monkeypatch.setattr(
        "data_platform.mlops.drift.emit_alert_events_from_drift",
        lambda **kwargs: emitted.extend(kwargs["drift_rows"]),
    )

    rows = [
        Row(
            event_timestamp=None,
            env="dev",
            project="clientes",
            model_name="clientes_status_classifier",
            model_version="v123",
            target_env="prd",
            run_id="run-1",
            monitoring_type="prediction",
            entity_name="1.0",
            metric_name="prediction_rate",
            baseline_value=0.2,
            current_value=0.95,
            absolute_diff=0.75,
            relative_diff=3.75,
            drift_status="CRITICAL",
        )
    ]

    log_drift_records(
        spark=FakeSpark(),
        rows=rows,
        project="clientes",
        use_catalog=False,
        emit_alerts=True,
        alert_severity_min="WARNING",
    )

    assert len(emitted) == 1
    assert emitted[0]["drift_status"] == "CRITICAL"
    assert emitted[0]["metric_name"] == "prediction_rate"


def test_log_drift_records_does_not_emit_alerts_when_disabled(monkeypatch):
    from types import SimpleNamespace
    from data_platform.mlops.drift import log_drift_records

    emitted = []

    class FakeWriter:
        def mode(self, *args, **kwargs):
            return self

        def option(self, *args, **kwargs):
            return self

        def saveAsTable(self, *args, **kwargs):
            return None

    class FakeDataFrame:
        @property
        def write(self):
            return FakeWriter()

    class FakeCatalog:
        @staticmethod
        def tableExists(name):
            return True

    class FakeSpark:
        catalog = FakeCatalog()

        def sql(self, query):
            return None

        def createDataFrame(self, rows, schema=None):
            return FakeDataFrame()

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            schema_mlops="clientes_mlops",
            qualified_schema=lambda schema: schema,
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    monkeypatch.setattr(
        "data_platform.mlops.drift.get_context",
        lambda project, use_catalog: fake_ctx,
    )
    monkeypatch.setattr(
        "data_platform.mlops.drift.emit_alert_events_from_drift",
        lambda **kwargs: emitted.extend(kwargs["drift_rows"]),
    )

    rows = [
        Row(
            event_timestamp=None,
            env="dev",
            project="clientes",
            model_name="clientes_status_classifier",
            model_version="v123",
            target_env="prd",
            run_id="run-1",
            monitoring_type="prediction",
            entity_name="1.0",
            metric_name="prediction_rate",
            baseline_value=0.2,
            current_value=0.95,
            absolute_diff=0.75,
            relative_diff=3.75,
            drift_status="CRITICAL",
        )
    ]

    log_drift_records(
        spark=FakeSpark(),
        rows=rows,
        project="clientes",
        use_catalog=False,
        emit_alerts=False,
        alert_severity_min="WARNING",
    )

    assert emitted == []
