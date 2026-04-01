from pyspark.sql import Row
from pyspark.sql import functions as F

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger
from b3_platform.pipeline_runner import run_with_observability


def run_ingest_table_clientes(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="ingest_table_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = base_logger.run_id

    def _run(logger: PlatformLogger):
        bronze_table = (
            f"{ctx.naming.catalog}.{ctx.naming.schema_bronze}.raw_clientes_table"
            if use_catalog
            else f"{ctx.naming.schema_bronze}.raw_clientes_table"
        )

        logger.info("Iniciando ingestão por tabela/origem simulada")
        logger.info(f"bronze_table={bronze_table}")

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_bronze}")

        rows = [
            Row(id_cliente=10, nome="Daniel", segmento="PF", status="ATIVO"),
            Row(id_cliente=11, nome="Erika", segmento="PJ", status="ATIVO"),
            Row(id_cliente=12, nome="Fabio", segmento="PF", status="INATIVO"),
            Row(id_cliente=100, nome="Gabi_TABELA", segmento="PF", status="ATIVO"),
        ]

        df = (
            spark.createDataFrame(rows)
            .withColumn("source_type", F.lit("table"))
            .withColumn("source_name", F.lit("source_clientes_simulada"))
            .withColumn("source_path", F.lit(None).cast("string"))
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

        logger.info(f"Ingestão por tabela finalizada com sucesso: {bronze_table}")

    run_with_observability(
        spark=spark,
        component="ingest_table_clientes",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
    )