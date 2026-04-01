from pyspark.sql import functions as F

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger
from b3_platform.pipeline_runner import run_with_observability


def run_silver_consolidado_clientes(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="silver_consolidado_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        bronze_file = f"{ctx.naming.schema_bronze}.raw_clientes_file"
        bronze_table = f"{ctx.naming.schema_bronze}.raw_clientes_table"
        silver_table = f"{ctx.naming.schema_silver}.tb_clientes_consolidado"

        logger.info("Iniciando silver")
        logger.info(f"bronze_file={bronze_file}")
        logger.info(f"bronze_table={bronze_table}")
        logger.info(f"silver_table={silver_table}")

        df_file = spark.table(bronze_file)
        df_table = spark.table(bronze_table)

        df_union = df_file.unionByName(df_table, allowMissingColumns=True)

        df_final = (
            df_union
            .withColumn("nome", F.trim(F.col("nome")))
            .withColumn("segmento", F.upper(F.col("segmento")))
            .withColumn("status", F.upper(F.col("status")))
            .withColumn("dt_processamento", F.current_timestamp())
        )

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_silver}")

        df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

        logger.info(f"Sucesso silver: {silver_table}")

    run_with_observability(
        spark=spark,
        component="silver_consolidado_clientes",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )