from pyspark.sql import functions as F

from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger


def run_ingest_knowledge(
    *,
    spark,
    project: str,
    use_catalog: bool | None,
    config_path: str,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> str:
    cfg = load_yaml_config(config_path)
    ctx = get_context(project=project, use_catalog=use_catalog)

    logger = PlatformLogger(
        component="ingest_knowledge",
        env=ctx.env,
        project=ctx.project,
        run_id=forced_run_id,
    )

    source_path = cfg["knowledge"]["source_path"]
    source_format = cfg["knowledge"].get("format", "parquet")
    id_column = cfg["knowledge"]["id_column"]
    text_column = cfg["knowledge"]["text_column"]
    metadata_columns = cfg["knowledge"].get("metadata_columns", [])

    target_table = f"{ctx.naming.silver_schema}.tb_{project}_ai_knowledge"

    logger.info(f"Lendo knowledge source_path={source_path} format={source_format}")

    df = spark.read.format(source_format).load(source_path)

    required_columns = [id_column, text_column]
    for col_name in required_columns:
        if col_name not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente na knowledge base: {col_name}")

    available_metadata = [c for c in metadata_columns if c in df.columns]

    df_out = df.select(
        F.col(id_column).cast("string").alias("document_id"),
        F.col(text_column).cast("string").alias("document_text"),
        *[F.col(c).cast("string").alias(c) for c in available_metadata],
    ).withColumn("project", F.lit(project))

    logger.info(f"Persistindo knowledge target_table={target_table}")
    df_out.write.mode("overwrite").saveAsTable(target_table)

    logger.info(f"Knowledge ingest concluído target_table={target_table}")
    return target_table
