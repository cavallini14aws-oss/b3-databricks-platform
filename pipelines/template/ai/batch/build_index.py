from pyspark.sql import functions as F

from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger


def run_build_index(
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
        component="build_index",
        env=ctx.env,
        project=ctx.project,
        run_id=forced_run_id,
    )

    source_table = f"{ctx.naming.silver_schema}.{cfg['embeddings'].get('output_table', f'tb_{project}_ai_embeddings')}"
    target_table = f"{ctx.naming.silver_schema}.{cfg['index'].get('output_table', f'tb_{project}_ai_index')}"

    logger.info(f"Lendo embeddings source_table={source_table}")
    df = spark.table(source_table)

    df_out = df.select(
        F.col("document_id"),
        F.col("chunk_id"),
        F.col("chunk_index"),
        F.col("embedding"),
        F.col("project"),
        F.lit(cfg["index"].get("index_type", "vector")).alias("index_type"),
    )

    logger.info(f"Persistindo índice target_table={target_table}")
    df_out.write.mode("overwrite").saveAsTable(target_table)

    logger.info(f"Índice vetorial concluído target_table={target_table}")
    return target_table
