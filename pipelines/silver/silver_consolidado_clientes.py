from pyspark.sql import functions as F

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger


def run_silver_consolidado_clientes(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="silver_consolidado_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    bronze_file = f"{ctx.naming.schema_bronze}.raw_clientes_file"
    bronze_table = f"{ctx.naming.schema_bronze}.raw_clientes_table"
    silver_table = f"{ctx.naming.schema_silver}.tb_clientes_consolidado"

    logger.info("Iniciando consolidação silver")
    logger.info(f"bronze_file={bronze_file}")
    logger.info(f"bronze_table={bronze_table}")
    logger.info(f"silver_table={silver_table}")

    df_file = spark.table(bronze_file)
    df_table = spark.table(bronze_table)

    df_union = df_file.unionByName(df_table)

    df_final = (
        df_union
        .withColumn("nome", F.trim(F.col("nome")))
        .withColumn("segmento", F.upper(F.col("segmento")))
        .withColumn("status", F.upper(F.col("status")))
        .withColumn("dt_processamento", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_silver}")
    df_final.write.mode("overwrite").saveAsTable(silver_table)

    logger.info(f"Silver consolidado criado com sucesso: {silver_table}")