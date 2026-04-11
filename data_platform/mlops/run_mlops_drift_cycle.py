import json

from data_platform.mlops.readiness_report import build_mlops_readiness_report
from pyspark.sql import SparkSession


def main():
    try:
        active_spark = spark  # type: ignore[name-defined]
    except NameError:
        active_spark = SparkSession.builder.getOrCreate()

    report = build_mlops_readiness_report(
        spark=active_spark,
        project="clientes",
        use_catalog=False,
    )

    print(json.dumps({
        "cycle": "drift",
        "project": report["project"],
        "env": report["env"],
        "readiness_score": report["readiness_score"],
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
