from pyspark.sql import functions as F

from data_platform.aiops.retrieval.storage import (
    ai_local_dataset_path,
    is_local_ai_mode,
)
from data_platform.aiops.retrieval.table_names import ai_table_fqn, resolve_silver_schema
from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger


def run_evaluate_rag(
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
        component="evaluate_rag",
        env=ctx.env,
        project=ctx.project,
        run_id=forced_run_id,
    )

    local_mode = is_local_ai_mode(cfg)
    if local_mode:
        chunks_source = ai_local_dataset_path(cfg, project, "chunks")
        embeddings_source = ai_local_dataset_path(cfg, project, "embeddings")
        index_source = ai_local_dataset_path(cfg, project, "index")
        target = ai_local_dataset_path(cfg, project, "rag_eval")
    else:
        silver_schema = resolve_silver_schema(ctx)
        chunks_source = ai_table_fqn(ctx, project, "chunks")
        embeddings_source = f"{silver_schema}.{cfg['embeddings'].get('output_table', f'tb_{project}_ai_embeddings')}"
        index_source = f"{silver_schema}.{cfg['index'].get('output_table', f'tb_{project}_ai_index')}"
        target = f"{silver_schema}.{cfg['evaluation'].get('output_table', f'tb_{project}_ai_rag_eval')}"

    logger.info("Calculando métricas básicas da trilha RAG")

    chunks_df = spark.read.parquet(chunks_source) if local_mode else spark.table(chunks_source)
    embeddings_df = spark.read.parquet(embeddings_source) if local_mode else spark.table(embeddings_source)
    index_df = spark.read.parquet(index_source) if local_mode else spark.table(index_source)

    chunks_count = chunks_df.count()
    embeddings_count = embeddings_df.count()
    index_count = index_df.count()

    coverage = 0.0
    if chunks_count > 0:
        coverage = round(embeddings_count / chunks_count, 4)

    df_out = spark.createDataFrame(
        [(
            project,
            chunks_count,
            embeddings_count,
            index_count,
            coverage,
            cfg["evaluation"].get("evaluation_type", "rag_basic"),
        )],
        schema=[
            "project",
            "chunks_count",
            "embeddings_count",
            "index_count",
            "embedding_coverage",
            "evaluation_type",
        ],
    ).withColumn("status", F.when(F.col("embedding_coverage") >= 1.0, F.lit("OK")).otherwise(F.lit("WARN")))

    if local_mode:
        logger.info(f"Persistindo avaliação RAG target_path={target}")
        df_out.write.mode("overwrite").parquet(target)
    else:
        logger.info(f"Persistindo avaliação RAG target_table={target}")
        df_out.write.mode("overwrite").saveAsTable(target)

    logger.info(f"Avaliação RAG concluída target={target}")
    return target
