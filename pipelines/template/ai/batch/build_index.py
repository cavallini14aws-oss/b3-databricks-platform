from pyspark.sql import functions as F

from data_platform.aiops.retrieval.storage import (
    ai_local_dataset_path,
    is_local_ai_mode,
)
from data_platform.aiops.retrieval.table_names import resolve_silver_schema
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

    local_mode = is_local_ai_mode(cfg)
    source = (
        ai_local_dataset_path(cfg, project, "embeddings")
        if local_mode
        else f"{resolve_silver_schema(ctx)}.{cfg['embeddings'].get('output_table', f'tb_{project}_ai_embeddings')}"
    )
    target = (
        ai_local_dataset_path(cfg, project, "index")
        if local_mode
        else f"{resolve_silver_schema(ctx)}.{cfg['index'].get('output_table', f'tb_{project}_ai_index')}"
    )

    logger.info(f"Lendo embeddings source={source}")
    df = spark.read.parquet(source) if local_mode else spark.table(source)

    df_out = df.select(
        F.col("document_id"),
        F.col("chunk_id"),
        F.col("chunk_index"),
        F.col("embedding"),
        F.col("project"),
        F.lit(cfg["index"].get("index_type", "vector")).alias("index_type"),
    )

    if local_mode:
        logger.info(f"Persistindo índice target_path={target}")
        df_out.write.mode("overwrite").parquet(target)
    else:
        logger.info(f"Persistindo índice target_table={target}")
        df_out.write.mode("overwrite").saveAsTable(target)

    logger.info(f"Índice vetorial concluído target={target}")
    return target
