from pathlib import Path


REQUIRED_PIPELINE_ARTIFACTS = [
    "config/pipelines_registry.yml",
    "config/clientes_ml_pipeline.yml",
    "pipelines/examples/clientes/ml/run_clientes_ml_end_to_end.py",
    "pipelines/examples/clientes/ml/run_clientes_retraining.py",
    "pipelines/examples/clientes/ml/evaluate_clientes_postprod.py",
]


def validate_pipeline_registry_artifacts() -> dict:
    missing = [path for path in REQUIRED_PIPELINE_ARTIFACTS if not Path(path).exists()]

    return {
        "valid": len(missing) == 0,
        "required_artifacts": list(REQUIRED_PIPELINE_ARTIFACTS),
        "missing_artifacts": missing,
    }
