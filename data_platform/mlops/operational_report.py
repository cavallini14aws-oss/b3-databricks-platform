from pathlib import Path

from data_platform.mlops.readiness_report import build_mlops_readiness_report


DOC_ARTIFACTS = [
    "docs/runbooks/ml_promotion_runbook.md",
    "docs/runbooks/ml_rollback_runbook.md",
    "docs/runbooks/ml_degradation_runbook.md",
    "docs/checklists/ml_promotion_checklist.md",
    "docs/checklists/ml_rollback_checklist.md",
    "docs/checklists/ml_degradation_checklist.md",
]

ENTRYPOINT_ARTIFACTS = [
    "data_platform/mlops/show_readiness_report.py",
    "data_platform/mlops/run_mlops_drift_cycle.py",
    "data_platform/mlops/run_mlops_postprod_cycle.py",
    "data_platform/mlops/run_mlops_retraining_cycle.py",
    "data_platform/mlops/run_mlops_operational_cycle.py",
]

PIPELINE_ARTIFACTS = [
    "pipelines/examples/clientes/ml/run_clientes_ml_end_to_end.py",
    "pipelines/examples/clientes/ml/run_clientes_retraining.py",
    "pipelines/examples/clientes/ml/evaluate_clientes_postprod.py",
    "pipelines/examples/clientes/ml/prepare_clientes_postprod_labels.py",
]


def _artifact_exists(path_str: str) -> bool:
    return Path(path_str).exists()


def build_mlops_operational_report(
    spark,
    *,
    project: str = "clientes",
    use_catalog: bool = False,
) -> dict:
    readiness = build_mlops_readiness_report(
        spark=spark,
        project=project,
        use_catalog=use_catalog,
    )

    docs_status = {path: _artifact_exists(path) for path in DOC_ARTIFACTS}
    entrypoints_status = {path: _artifact_exists(path) for path in ENTRYPOINT_ARTIFACTS}
    pipelines_status = {path: _artifact_exists(path) for path in PIPELINE_ARTIFACTS}

    docs_ready = sum(1 for exists in docs_status.values() if exists)
    entrypoints_ready = sum(1 for exists in entrypoints_status.values() if exists)
    pipelines_ready = sum(1 for exists in pipelines_status.values() if exists)

    return {
        "project": readiness["project"],
        "env": readiness["env"],
        "schema_mlops": readiness["schema_mlops"],
        "readiness_score": readiness["readiness_score"],
        "ready_items": readiness["ready_items"],
        "total_items": readiness["total_items"],
        "docs_ready": docs_ready,
        "docs_total": len(DOC_ARTIFACTS),
        "entrypoints_ready": entrypoints_ready,
        "entrypoints_total": len(ENTRYPOINT_ARTIFACTS),
        "pipelines_ready": pipelines_ready,
        "pipelines_total": len(PIPELINE_ARTIFACTS),
        "docs_status": docs_status,
        "entrypoints_status": entrypoints_status,
        "pipelines_status": pipelines_status,
        "table_checks": readiness["checks"],
    }
