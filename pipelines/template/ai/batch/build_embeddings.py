import hashlib

from pyspark.sql import Row, types as T

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

    source_table = f"{ctx.naming.silver_schema}.tb_{project}_ai_chunks"
    configured_output = cfg["embeddings"].get("output_table", f"tb_{project}_ai_embeddings")
    target_table = f"{ctx.naming.silver_schema}.{configured_output}"

    logger.info(f"Lendo chunks source_table={source_table}")
    df = spark.table(source_table)

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

    logger.info(f"Persistindo embeddings target_table={target_table}")
    df_out.write.mode("overwrite").saveAsTable(target_table)

    logger.info(f"Embeddings concluídos target_table={target_table}")
    return target_table
