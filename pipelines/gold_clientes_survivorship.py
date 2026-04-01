from pyspark.sql import functions as F
from pyspark.sql.window import Window

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger
from b3_platform.pipeline_runner import run_with_observability


def run_gold_clientes_survivorship(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="gold_clientes_survivorship",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = base_logger.run_id

    def _run(logger: PlatformLogger):
        silver_table = f"{ctx.naming.schema_silver}.tb_clientes_consolidado"
        gold_table = f"{ctx.naming.schema_gold}.tb_clientes_survivorship"

        logger.info("Iniciando gold com survivorship")
        logger.info(f"silver_table={silver_table}")
        logger.info(f"gold_table={gold_table}")

        source_priority = (
            F.when(F.col("source_type") == "table", F.lit(1))
             .when(F.col("source_type") == "file", F.lit(2))
             .otherwise(F.lit(99))
        )

        window_spec = Window.partitionBy("id_cliente").orderBy(
            source_priority.asc(),
            F.col("update_date").desc_nulls_last(),
            F.col("dt_processamento").desc_nulls_last(),
        )

        df = (
            spark.table(silver_table)
            .filter(F.col("status") == "ATIVO")
            .withColumn("source_priority", source_priority)
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num", "source_priority")
        )

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_gold}")

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)

        logger.info(f"Gold survivorship criada com sucesso: {gold_table}")

    run_with_observability(
        spark=spark,
        component="gold_clientes_survivorship",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
    )