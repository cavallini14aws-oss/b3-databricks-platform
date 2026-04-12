import hashlib

from pyspark.sql import Row, types as T

from data_platform.aiops.retrieval.storage import (
    ai_local_dataset_path,
    is_local_ai_mode,
)
from data_platform.aiops.retrieval.table_names import ai_table_fqn, resolve_silver_schema
from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger


def _fake_embedding(text: str, dims: int = 8) -> list[float]:
    digest = hashlib.sha256((text or "").encode("utf-8")).digest()
    values = []
    for i in range(dims):
        byte_val = digest[i]
        values.append(round(byte_val / 255.0, 6))
    return values


def run_build_embeddings(
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
        component="build_embeddings",
        env=ctx.env,
        project=ctx.project,
        run_id=forced_run_id,
    )

    local_mode = is_local_ai_mode(cfg)
    source = (
        ai_local_dataset_path(cfg, project, "chunks")
        if local_mode
        else ai_table_fqn(ctx, project, "chunks")
    )

    configured_output = cfg["embeddings"].get("output_table", f"tb_{project}_ai_embeddings")
    target = (
        ai_local_dataset_path(cfg, project, "embeddings")
        if local_mode
        else f"{resolve_silver_schema(ctx)}.{configured_output}"
    )

    logger.info(f"Lendo chunks source={source}")
    df = spark.read.parquet(source) if local_mode else spark.table(source)

    rows = df.select("document_id", "chunk_id", "chunk_index", "chunk_text", "project").collect()

    output_rows = []
    for row in rows:
        output_rows.append(
            Row(
                document_id=row["document_id"],
                chunk_id=row["chunk_id"],
                chunk_index=row["chunk_index"],
                embedding=_fake_embedding(row["chunk_text"]),
                embedding_model=cfg["embeddings"].get("model_name", "placeholder"),
                project=row["project"],
            )
        )

    schema = T.StructType([
        T.StructField("document_id", T.StringType(), False),
        T.StructField("chunk_id", T.StringType(), False),
        T.StructField("chunk_index", T.IntegerType(), False),
        T.StructField("embedding", T.ArrayType(T.FloatType()), False),
        T.StructField("embedding_model", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
    ])

    df_out = spark.createDataFrame(output_rows, schema=schema)

    if local_mode:
        logger.info(f"Persistindo embeddings target_path={target}")
        df_out.write.mode("overwrite").parquet(target)
    else:
        logger.info(f"Persistindo embeddings target_table={target}")
        df_out.write.mode("overwrite").saveAsTable(target)

    logger.info(f"Embeddings concluídos target={target}")
    return target
