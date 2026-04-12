from pyspark.sql import Row, functions as F, types as T

from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger


def _chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    if not text:
        return []

    chunks = []
    start = 0
    step = max(1, chunk_size - chunk_overlap)

    while start < len(text):
        chunk = text[start:start + chunk_size]
        if chunk:
            chunks.append(chunk)
        start += step

    return chunks


def run_chunk_documents(
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
        component="chunk_documents",
        env=ctx.env,
        project=ctx.project,
        run_id=forced_run_id,
    )

    source_table = f"{ctx.naming.silver_schema}.tb_{project}_ai_knowledge"
    target_table = f"{ctx.naming.silver_schema}.tb_{project}_ai_chunks"

    chunk_size = cfg["chunking"].get("chunk_size", 800)
    chunk_overlap = cfg["chunking"].get("chunk_overlap", 120)

    logger.info(
        f"Lendo knowledge source_table={source_table} chunk_size={chunk_size} chunk_overlap={chunk_overlap}"
    )

    df = spark.table(source_table)

    rows = df.select("document_id", "document_text", "project").collect()

    output_rows = []
    for row in rows:
        chunks = _chunk_text(row["document_text"], chunk_size, chunk_overlap)
        for idx, chunk in enumerate(chunks):
            output_rows.append(
                Row(
                    document_id=row["document_id"],
                    chunk_id=f'{row["document_id"]}_{idx}',
                    chunk_index=idx,
                    chunk_text=chunk,
                    project=row["project"],
                )
            )

    schema = T.StructType([
        T.StructField("document_id", T.StringType(), False),
        T.StructField("chunk_id", T.StringType(), False),
        T.StructField("chunk_index", T.IntegerType(), False),
        T.StructField("chunk_text", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
    ])

    df_out = spark.createDataFrame(output_rows, schema=schema)

    logger.info(f"Persistindo chunks target_table={target_table}")
    df_out.write.mode("overwrite").saveAsTable(target_table)

    logger.info(f"Chunking concluído target_table={target_table}")
    return target_table
