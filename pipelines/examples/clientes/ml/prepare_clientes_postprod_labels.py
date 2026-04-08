from pyspark.sql import functions as F

from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.mlops.datasets import get_postprod_labels_table
from data_platform.orchestration.pipeline_runner import run_with_observability


def run_prepare_clientes_postprod_labels(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="prepare_clientes_postprod_labels",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        source_table = (
            f"{ctx.naming.catalog}.{ctx.naming.schema_silver}.tb_clientes_consolidado"
            if use_catalog
            else f"{ctx.naming.schema_silver}.tb_clientes_consolidado"
        )

        target_table = get_postprod_labels_table(
            project=project,
            use_catalog=use_catalog,
        )

        mlops_schema = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {mlops_schema}")

        logger.info(f"source_table={source_table}")
        logger.info(f"target_table={target_table}")

        df = spark.table(source_table)

        labels_df = (
            df.groupBy("id_cliente")
            .agg(
                F.max(
                    F.when(F.col("status") == "ATIVO", F.lit(1)).otherwise(F.lit(0))
                ).alias("tem_ativo")
            )
            .withColumn(
                "label_real",
                F.when(F.col("tem_ativo") == 1, F.lit(1.0)).otherwise(F.lit(0.0))
            )
            .withColumn("label_snapshot_ts", F.current_timestamp())
            .select("id_cliente", "label_real", "label_snapshot_ts")
        )

        labels_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

        logger.info(f"Postprod labels criados com sucesso: {target_table}")

    run_with_observability(
        spark=spark,
        component="prepare_clientes_postprod_labels",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
