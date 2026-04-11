import json

from data_platform.mlops.readiness_report import build_mlops_readiness_report


def build_operational_cycle_summary(
    spark,
    *,
    project: str = "clientes",
    use_catalog: bool = False,
) -> dict:
    report = build_mlops_readiness_report(
        spark=spark,
        project=project,
        use_catalog=use_catalog,
    )

    return {
        "project": report["project"],
        "env": report["env"],
        "schema_mlops": report["schema_mlops"],
        "readiness_score": report["readiness_score"],
        "ready_items": report["ready_items"],
        "total_items": report["total_items"],
        "cycles": [
            "drift",
            "postprod",
            "retraining",
        ],
    }


def main():
    try:
        spark  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "Spark session não encontrada. Execute este entrypoint em ambiente Databricks "
            "ou injete a variável global 'spark'."
        ) from exc

    summary = build_operational_cycle_summary(
        spark=spark,  # type: ignore[name-defined]
        project="clientes",
        use_catalog=False,
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
