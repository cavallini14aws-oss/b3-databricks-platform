from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.core.context import get_context
from data_platform.mlops.model_states import VALID_MODEL_STATES, ARTIFACT_VALID_STATES


REGISTRY_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("algorithm", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("artifact_path", T.StringType(), True),
    ]
)


def validate_model_state(status: str) -> None:
    if status not in VALID_MODEL_STATES:
        raise ValueError(
            f"Status de modelo invalido: {status}. "
            f"Permitidos: {sorted(VALID_MODEL_STATES)}"
        )


def register_model(
    spark,
    model_name: str,
    model_version: str,
    algorithm: str,
    run_id: str,
    status: str,
    artifact_path: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    validate_model_state(status)

    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_registry")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        algorithm=algorithm,
        run_id=run_id,
        status=status,
        artifact_path=artifact_path,
    )

    df = spark.createDataFrame([row], schema=REGISTRY_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def get_model_registry_entry(
    spark,
    model_name: str,
    model_version: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_registry")

    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Tabela de registry nao encontrada: {table_name}")

    rows = (
        spark.table(table_name)
        .filter(
            (F.col("model_name") == model_name) &
            (F.col("model_version") == model_version)
        )
        .orderBy(F.col("event_timestamp").desc())
        .limit(1)
        .collect()
    )

    if not rows:
        raise ValueError(
            f"Modelo nao encontrado no registry: "
            f"model_name={model_name}, model_version={model_version}"
        )

    return rows[0]


def get_model_artifact_path(
    spark,
    model_name: str,
    model_version: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    row = get_model_registry_entry(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        project=project,
        use_catalog=use_catalog,
    )

    artifact_path = row["artifact_path"]
    status = row["status"]

    if not artifact_path:
        raise ValueError(
            f"Artifact path ausente no registry para model_name={model_name}, "
            f"model_version={model_version}. "
            "Essa versao pode ser antiga ou o treino pode ter falhado antes de persistir o modelo."
        )

    if status not in ARTIFACT_VALID_STATES:
        raise ValueError(
            f"Status inesperado no registry para model_name={model_name}, "
            f"model_version={model_version}: {status}. "
            f"Esperado um dos estados validos para artifact: {sorted(ARTIFACT_VALID_STATES)}"
        )

    return artifact_path


def get_latest_valid_model_entry(
    spark,
    model_name: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_registry")

    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Tabela de registry nao encontrada: {table_name}")

    rows = (
        spark.table(table_name)
        .filter(
            (F.col("model_name") == model_name) &
            (F.col("status").isin(list(ARTIFACT_VALID_STATES))) &
            (F.col("artifact_path").isNotNull()) &
            (F.length(F.trim(F.col("artifact_path"))) > 0)
        )
        .orderBy(F.col("event_timestamp").desc())
        .limit(1)
        .collect()
    )

    if not rows:
        raise ValueError(
            f"Nenhum modelo valido encontrado para model_name={model_name}"
        )

    return rows[0]


def get_latest_valid_model_version(
    spark,
    model_name: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    row = get_latest_valid_model_entry(
        spark=spark,
        model_name=model_name,
        project=project,
        use_catalog=use_catalog,
    )
    return row["model_version"]


def get_latest_model_status(
    spark,
    model_name: str,
    model_version: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    row = get_model_registry_entry(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        project=project,
        use_catalog=use_catalog,
    )
    return row["status"]


def update_model_status(
    spark,
    model_name: str,
    model_version: str,
    status: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    validate_model_state(status)

    entry = get_model_registry_entry(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        project=project,
        use_catalog=use_catalog,
    )

    artifact_path = entry["artifact_path"]
    algorithm = entry["algorithm"]
    run_id = entry["run_id"]

    register_model(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        algorithm=algorithm,
        run_id=run_id,
        status=status,
        artifact_path=artifact_path,
        project=project,
        use_catalog=use_catalog,
    )
