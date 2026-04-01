from pyspark.sql import Row

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger


def run_demo_pipeline(spark, project: str = "clientes", use_catalog: bool = False) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="demo_pipeline",
        env=ctx.env,
        project=ctx.project,
    )

    logger.info("Iniciando pipeline demo")
    logger.info(f"catalog={ctx.naming.catalog}")
    logger.info(f"schema_silver={ctx.naming.schema_silver}")
    logger.info(f"demo_table={ctx.naming.demo_table}")

    rows = [
        Row(id_cliente=1, nome="Ana", segmento="PF"),
        Row(id_cliente=2, nome="Bruno", segmento="PJ"),
    ]
    df = spark.createDataFrame(rows)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_silver}")
    df.write.mode("overwrite").saveAsTable(ctx.naming.demo_table)

    logger.info(f"Tabela gravada com sucesso: {ctx.naming.demo_table}")
    logger.info("Fim do pipeline demo")
