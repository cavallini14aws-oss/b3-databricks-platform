from pyspark.sql import functions as F

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger


def run_gold_clientes_ativos(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="gold_clientes_ativos",
        env=ctx.env,
        project=ctx.project,
    )

    silver_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_silver}.tb_clientes_consolidado"
        if use_catalog
        else f"{ctx.naming.schema_silver}.tb_clientes_consolidado"
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
            "source_type",
            "source_path",
            "source_name",
            "run_id",
            "ingestion_date",
            "update_date",
            "dt_processamento",
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_gold}")

    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)

    logger.info(f"Gold criada com sucesso: {gold_table}")