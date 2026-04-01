from pyspark.sql import functions as F

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger


def run_ingest_file_clientes(
    spark,
    file_path: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="ingest_file_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    bronze_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_bronze}.raw_clientes_file"
        if use_catalog
        else f"{ctx.naming.schema_bronze}.raw_clientes_file"
    )

    logger.info(f"Iniciando ingestão por arquivo: {file_path}")
    logger.info(f"bronze_table={bronze_table}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_bronze}")

    df = (
        spark.read
        .option("header", True)
        .csv(file_path)
        .withColumn("source_type", F.lit("file"))
        .withColumn("source_path", F.lit(file_path))
        .withColumn("ingestion_date", F.current_timestamp())
        .withColumn("update_date", F.current_timestamp())
        .withColumn("run_id", F.lit(logger.run_id))
    )

    df.write.mode("overwrite").saveAsTable(bronze_table)

    logger.info(f"Ingestão por arquivo finalizada com sucesso: {bronze_table}")