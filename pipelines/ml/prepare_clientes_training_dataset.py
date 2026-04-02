from pyspark.sql import functions as F

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.orchestration.pipeline_runner import run_with_observability


def run_prepare_clientes_training_dataset(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="prepare_clientes_training_dataset",
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

        target_table = get_training_dataset_table(
            project=project,
            use_catalog=use_catalog,
        )

        feature_schema = ctx.naming.qualified_schema(ctx.naming.schema_feature)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {feature_schema}")

        logger.info(f"source_table={source_table}")
        logger.info(f"target_table={target_table}")

        df = (
            spark.table(source_table)
            .select("id_cliente", "segmento", "source_type", "status")
            .withColumn(
                "label",
                F.when(F.col("status") == "ATIVO", F.lit(1)).otherwise(F.lit(0))
            )
        )

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

        logger.info(f"Training dataset criado com sucesso: {target_table}")

    run_with_observability(
        spark=spark,
        component="prepare_clientes_training_dataset",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
