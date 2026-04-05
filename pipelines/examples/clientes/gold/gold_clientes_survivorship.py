from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.orchestration.pipeline_runner import run_with_observability


def run_gold_clientes_survivorship(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="gold_clientes_survivorship",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        silver_table = (
            f"{ctx.naming.catalog}.{ctx.naming.schema_silver}.tb_clientes_consolidado"
            if use_catalog
            else f"{ctx.naming.schema_silver}.tb_clientes_consolidado"
        )

        gold_table = (
            f"{ctx.naming.catalog}.{ctx.naming.schema_gold}.tb_clientes_survivorship"
            if use_catalog
            else f"{ctx.naming.schema_gold}.tb_clientes_survivorship"
        )

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
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )