from pyspark.sql import functions as F

from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.orchestration.pipeline_runner import run_with_observability


def run_gold_clientes_ativos(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="gold_clientes_ativos",
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

    run_with_observability(
        spark=spark,
        component="gold_clientes_ativos",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )