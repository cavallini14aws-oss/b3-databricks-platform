from pyspark.sql import Row
from pyspark.sql import functions as F

from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger


def run_bronze_clientes(spark, project: str = "clientes", use_catalog: bool = False) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="bronze_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    bronze_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_bronze}.raw_clientes"
        if use_catalog
        else f"{ctx.naming.schema_bronze}.raw_clientes"
    )

    logger.info("Iniciando carga bronze")
    logger.info(f"bronze_table={bronze_table}")

    rows = [
        Row(id_cliente=1, nome="Ana", segmento="PF", status="ATIVO"),
        Row(id_cliente=2, nome="Bruno", segmento="PJ", status="INATIVO"),
        Row(id_cliente=3, nome="Carla", segmento="PF", status="ATIVO"),
    ]

    df = spark.createDataFrame(rows).withColumn("dt_ingestao", F.current_timestamp())

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_bronze}")
    df.write.mode("overwrite").saveAsTable(bronze_table)

    logger.info(f"Bronze gravada com sucesso: {bronze_table}")


def run_silver_clientes(spark, project: str = "clientes", use_catalog: bool = False) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="silver_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    bronze_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_bronze}.raw_clientes"
        if use_catalog
        else f"{ctx.naming.schema_bronze}.raw_clientes"
    )
    silver_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_silver}.tb_clientes"
        if use_catalog
        else f"{ctx.naming.schema_silver}.tb_clientes"
    )

    logger.info("Iniciando transformação silver")
    logger.info(f"bronze_table={bronze_table}")
    logger.info(f"silver_table={silver_table}")

    df = (
        spark.table(bronze_table)
        .withColumn("nome", F.trim(F.col("nome")))
        .withColumn("segmento", F.upper(F.col("segmento")))
        .withColumn("status", F.upper(F.col("status")))
        .withColumn("dt_processamento", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_silver}")
    df.write.mode("overwrite").saveAsTable(silver_table)

    logger.info(f"Silver gravada com sucesso: {silver_table}")


def run_gold_clientes(spark, project: str = "clientes", use_catalog: bool = False) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="gold_clientes",
        env=ctx.env,
        project=ctx.project,
    )

    silver_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_silver}.tb_clientes"
        if use_catalog
        else f"{ctx.naming.schema_silver}.tb_clientes"
    )
    gold_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_gold}.tb_clientes_ativos"
        if use_catalog
        else f"{ctx.naming.schema_gold}.tb_clientes_ativos"
    )

    logger.info("Iniciando transformação gold")
    logger.info(f"silver_table={silver_table}")
    logger.info(f"gold_table={gold_table}")

    df = (
        spark.table(silver_table)
        .filter(F.col("status") == "ATIVO")
        .select(
            "id_cliente",
            "nome",
            "segmento",
            "status",
            "dt_ingestao",
            "dt_processamento",
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_gold}")
    df.write.mode("overwrite").saveAsTable(gold_table)

    logger.info(f"Gold gravada com sucesso: {gold_table}")


def run_medallion_clientes(spark, project: str = "clientes", use_catalog: bool = False) -> None:
    run_bronze_clientes(spark, project=project, use_catalog=use_catalog)
    run_silver_clientes(spark, project=project, use_catalog=use_catalog)
    run_gold_clientes(spark, project=project, use_catalog=use_catalog)
