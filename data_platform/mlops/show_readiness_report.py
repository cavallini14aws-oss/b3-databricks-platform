import json

from data_platform.mlops.readiness_report import build_mlops_readiness_report


def main():
    try:
        spark  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "Spark session não encontrada. Execute este entrypoint em ambiente Databricks "
            "ou injete a variável global 'spark'."
        ) from exc

    report = build_mlops_readiness_report(
        spark=spark,  # type: ignore[name-defined]
        project="clientes",
        use_catalog=False,
    )

    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
