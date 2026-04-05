import argparse
from dataclasses import dataclass

from b3_platform.core.context import get_context
from b3_platform.mlops.registry import get_latest_valid_model_entry


@dataclass
class PromotionRequest:
    model_name: str
    source_env: str
    target_env: str
    model_version: str | None = None
    project: str = "clientes"
    use_catalog: bool = False


def validate_promotion_path(source_env: str, target_env: str) -> None:
    valid_paths = {
        ("dev", "hml"),
        ("hml", "prd"),
    }

    if (source_env, target_env) not in valid_paths:
        raise ValueError(
            f"Caminho de promocao invalido: {source_env} -> {target_env}. "
            "Permitidos: dev->hml, hml->prd"
        )


def resolve_model_entry(
    spark,
    request: PromotionRequest,
):
    if request.model_version:
        from b3_platform.mlops.registry import get_model_registry_entry

        return get_model_registry_entry(
            spark=spark,
            model_name=request.model_name,
            model_version=request.model_version,
            project=request.project,
            use_catalog=request.use_catalog,
        )

    return get_latest_valid_model_entry(
        spark=spark,
        model_name=request.model_name,
        project=request.project,
        use_catalog=request.use_catalog,
    )


def promote_and_deploy_ml(
    spark,
    model_name: str,
    source_env: str,
    target_env: str,
    model_version: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
):
    validate_promotion_path(source_env, target_env)

    request = PromotionRequest(
        model_name=model_name,
        source_env=source_env,
        target_env=target_env,
        model_version=model_version,
        project=project,
        use_catalog=use_catalog,
    )

    entry = resolve_model_entry(
        spark=spark,
        request=request,
    )

    resolved_model_version = entry["model_version"]
    artifact_path = entry["artifact_path"]
    status = entry["status"]

    if not artifact_path:
        raise ValueError(
            f"Modelo sem artifact_path valido: "
            f"model_name={model_name}, model_version={resolved_model_version}"
        )

    if status != "TRAINED":
        raise ValueError(
            f"Status invalido para promocao: "
            f"model_name={model_name}, model_version={resolved_model_version}, status={status}"
        )

    print(f"Promotion request accepted")
    print(f"model_name={model_name}")
    print(f"resolved_model_version={resolved_model_version}")
    print(f"artifact_path={artifact_path}")
    print(f"source_env={source_env}")
    print(f"target_env={target_env}")

    # Aqui fica o gancho para deploy/promocao real em ambiente futuro.
    return {
        "model_name": model_name,
        "model_version": resolved_model_version,
        "artifact_path": artifact_path,
        "source_env": source_env,
        "target_env": target_env,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote and deploy ML model")

    parser.add_argument("--model-name", required=True)
    parser.add_argument("--source-env", required=True, choices=["dev", "hml"])
    parser.add_argument("--target-env", required=True, choices=["hml", "prd"])
    parser.add_argument("--model-version", required=False, default=None)
    parser.add_argument("--project", required=False, default="clientes")
    parser.add_argument("--use-catalog", action="store_true")

    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        raise RuntimeError(
            "PySpark indisponivel para promote_and_deploy_ml. "
            "Execute este comando em ambiente com Spark/PySpark."
        ) from e

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    result = promote_and_deploy_ml(
        spark=spark,
        model_name=args.model_name,
        source_env=args.source_env,
        target_env=args.target_env,
        model_version=args.model_version,
        project=args.project,
        use_catalog=args.use_catalog,
    )

    print("Promotion result:")
    for k, v in result.items():
        print(f"{k}={v}")


if __name__ == "__main__":
    main()
