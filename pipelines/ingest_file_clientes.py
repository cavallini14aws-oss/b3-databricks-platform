from pyspark.sql import functions as F

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger
from b3_platform.pipeline_runner import run_with_observability


def run_ingest_file_clientes(
    spark,
    df_input,
    source_path: str = "inline_csv_memory",
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="ingest_file_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = base_logger.run_id

    def _run(logger: PlatformLogger):
        bronze_table = (
            f"{ctx.naming.catalog}.{ctx.naming.schema_bronze}.raw_clientes_file"
            if use_catalog
            else f"{ctx.naming.schema_bronze}.raw_clientes_file"
        )

        logger.info("Iniciando ingestão por arquivo")
        logger.info(f"source_path={source_path}")
        logger.info(f"bronze_table={bronze_table}")

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_bronze}")

        df = (
            df_input
            .withColumn("source_type", F.lit("file"))
            .withColumn("source_path", F.lit(source_path))
            .withColumn("source_name", F.lit(None).cast("string"))
            .withColumn("ingestion_date", F.current_timestamp())
            .withColumn("update_date", F.current_timestamp())
            .withColumn("run_id", F.lit(logger.run_id))
        )

        df = df.select(
            "id_cliente",
            "nome",
            "segmento",
            "status",
            "source_type",
            "source_path",
            "source_name",
            "ingestion_date",
            "update_date",
            "run_id",
        )

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)

        logger.info(f"Ingestão por arquivo finalizada com sucesso: {bronze_table}")

    run_with_observability(
        spark=spark,
        component="ingest_file_clientes",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
    )