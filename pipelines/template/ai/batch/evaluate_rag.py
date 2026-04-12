from pyspark.sql import functions as F

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

    chunks_table = f"{ctx.naming.silver_schema}.tb_{project}_ai_chunks"
    embeddings_table = f"{ctx.naming.silver_schema}.{cfg['embeddings'].get('output_table', f'tb_{project}_ai_embeddings')}"
    index_table = f"{ctx.naming.silver_schema}.{cfg['index'].get('output_table', f'tb_{project}_ai_index')}"
    target_table = f"{ctx.naming.silver_schema}.{cfg['evaluation'].get('output_table', f'tb_{project}_ai_rag_eval')}"

    logger.info("Calculando métricas básicas da trilha RAG")

    chunks_count = spark.table(chunks_table).count()
    embeddings_count = spark.table(embeddings_table).count()
    index_count = spark.table(index_table).count()

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

    logger.info(f"Persistindo avaliação RAG target_table={target_table}")
    df_out.write.mode("overwrite").saveAsTable(target_table)

    logger.info(f"Avaliação RAG concluída target_table={target_table}")
    return target_table
