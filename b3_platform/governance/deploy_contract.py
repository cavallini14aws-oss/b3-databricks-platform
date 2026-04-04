from dataclasses import dataclass
from datetime import datetime, timezone

from b3_platform.core.context import get_context
from b3_platform.core.job_config import load_job_config


@dataclass(frozen=True)
class DeploymentContract:
    source_env: str
    target_env: str
    target_cluster_key: str
    target_workspace_root: str
    target_timeout_seconds: int
    target_max_retries: int


def build_deployment_contract(
    source_env: str,
    target_env: str,
) -> DeploymentContract:
    target_job_config = load_job_config(target_env)

    return DeploymentContract(
        source_env=source_env,
        target_env=target_env,
        target_cluster_key=target_job_config.cluster_key,
        target_workspace_root=target_job_config.workspace_root,
        target_timeout_seconds=target_job_config.default_timeout_seconds,
        target_max_retries=target_job_config.max_retries,
    )


def log_deployment_contract(
    spark,
    model_name: str,
    model_version: str,
    deployment_contract: DeploymentContract,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    from pyspark.sql import Row
    from pyspark.sql import types as T

    DEPLOYMENT_CONTRACT_SCHEMA = T.StructType(
        [
            T.StructField("event_timestamp", T.TimestampType(), False),
            T.StructField("env", T.StringType(), False),
            T.StructField("project", T.StringType(), False),
            T.StructField("model_name", T.StringType(), False),
            T.StructField("model_version", T.StringType(), False),
            T.StructField("source_env", T.StringType(), False),
            T.StructField("target_env", T.StringType(), False),
            T.StructField("target_cluster_key", T.StringType(), False),
            T.StructField("target_workspace_root", T.StringType(), False),
            T.StructField("target_timeout_seconds", T.IntegerType(), False),
            T.StructField("target_max_retries", T.IntegerType(), False),
            T.StructField("run_id", T.StringType(), False),
        ]
    )

    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_deployment_contract",
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        source_env=deployment_contract.source_env,
        target_env=deployment_contract.target_env,
        target_cluster_key=deployment_contract.target_cluster_key,
        target_workspace_root=deployment_contract.target_workspace_root,
        target_timeout_seconds=int(deployment_contract.target_timeout_seconds),
        target_max_retries=int(deployment_contract.target_max_retries),
        run_id=run_id,
    )

    df = spark.createDataFrame([row], schema=DEPLOYMENT_CONTRACT_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
