from pyspark.sql import Window
from pyspark.sql import functions as F

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.orchestration.pipeline_runner import run_with_observability


def run_prepare_clientes_training_dataset(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    dataset_version: str = "v2",
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
            version=dataset_version,
        )

        feature_schema = ctx.naming.qualified_schema(ctx.naming.schema_feature)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {feature_schema}")

        logger.info(f"source_table={source_table}")
        logger.info(f"target_table={target_table}")
        logger.info(f"dataset_version={dataset_version}")

        df = spark.table(source_table)

        if dataset_version == "v1":
            dataset_df = (
                df
                .select("id_cliente", "segmento", "source_type", "status")
                .withColumn(
                    "label",
                    F.when(F.col("status") == "ATIVO", F.lit(1.0)).otherwise(F.lit(0.0))
                )
                .withColumn(
                    "dataset_split",
                    F.when((F.col("id_cliente") % 3) == 0, F.lit("test")).otherwise(F.lit("train"))
                )
            )
        else:
            status_agg = (
                df.groupBy("id_cliente")
                .agg(
                    F.max(F.when(F.col("status") == "ATIVO", F.lit(1)).otherwise(F.lit(0))).alias("tem_ativo"),
                    F.count(F.lit(1)).alias("qtd_registros"),
                    F.max(F.when(F.col("source_type") == "file", F.lit(1)).otherwise(F.lit(0))).alias("tem_file"),
                    F.max(F.when(F.col("source_type") == "table", F.lit(1)).otherwise(F.lit(0))).alias("tem_table"),
                )
            )

            segmento_rank = (
                df.groupBy("id_cliente", "segmento")
                .agg(F.count(F.lit(1)).alias("qtd"))
            )

            segmento_window = Window.partitionBy("id_cliente").orderBy(F.desc("qtd"), F.asc("segmento"))

            segmento_dominante = (
                segmento_rank
                .withColumn("rn", F.row_number().over(segmento_window))
                .filter(F.col("rn") == 1)
                .select(
                    "id_cliente",
                    F.col("segmento").alias("segmento_dominante"),
                )
            )

            source_rank = (
                df.groupBy("id_cliente", "source_type")
                .agg(F.count(F.lit(1)).alias("qtd"))
            )

            source_window = Window.partitionBy("id_cliente").orderBy(F.desc("qtd"), F.asc("source_type"))

            source_dominante = (
                source_rank
                .withColumn("rn", F.row_number().over(source_window))
                .filter(F.col("rn") == 1)
                .select(
                    "id_cliente",
                    F.col("source_type").alias("source_type_dominante"),
                )
            )

            dataset_df = (
                status_agg
                .join(segmento_dominante, on="id_cliente", how="left")
                .join(source_dominante, on="id_cliente", how="left")
                .withColumn(
                    "status_final",
                    F.when(F.col("tem_ativo") == 1, F.lit("ATIVO")).otherwise(F.lit("INATIVO"))
                )
                .withColumn(
                    "label",
                    F.when(F.col("status_final") == "ATIVO", F.lit(1.0)).otherwise(F.lit(0.0))
                )
                .withColumn(
                    "dataset_split",
                    F.when((F.col("id_cliente") % 3) == 0, F.lit("test")).otherwise(F.lit("train"))
                )
                .select(
                    "id_cliente",
                    "segmento_dominante",
                    "source_type_dominante",
                    "status_final",
                    "label",
                    "qtd_registros",
                    "tem_file",
                    "tem_table",
                    "dataset_split",
                )
            )

        dataset_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

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
