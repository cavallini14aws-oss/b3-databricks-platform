from dataclasses import dataclass
from datetime import datetime, timezone

from b3_platform.core.context import get_context
from b3_platform.core.job_config import JobConfig, PromotionRule


@dataclass(frozen=True)
class PromotionDecision:
    approved: bool
    reason: str


def _find_rule(
    job_config: JobConfig,
    source_env: str,
    target_env: str,
) -> PromotionRule | None:
    for rule in job_config.promotion_rules:
        if rule.source_env == source_env and rule.target_env == target_env:
            return rule
    return None


def evaluate_ml_promotion(
    job_config: JobConfig,
    source_env: str,
    target_env: str,
    accuracy: float | None,
    f1: float | None,
    auc: float | None,
    tests_passed: bool = True,
    manual_approval: bool = False,
) -> PromotionDecision:
    rule = _find_rule(job_config, source_env=source_env, target_env=target_env)

    if rule is None:
        return PromotionDecision(
            approved=False,
            reason=(
                f"Promotion blocked: no promotion rule configured for "
                f"{source_env} -> {target_env}."
            ),
        )

    if rule.require_tests_passed and not tests_passed:
        return PromotionDecision(
            approved=False,
            reason="Promotion blocked: automated tests did not pass.",
        )

    if rule.requires_approval and not manual_approval:
        return PromotionDecision(
            approved=False,
            reason="Promotion blocked: manual approval is required.",
        )

    if rule.require_quality_gates:
        gates = job_config.ml_quality_gates

        if accuracy is None or accuracy < gates.minimum_accuracy:
            return PromotionDecision(
                approved=False,
                reason=(
                    f"Promotion blocked: accuracy {accuracy} below "
                    f"minimum {gates.minimum_accuracy}."
                ),
            )

        if f1 is None or f1 < gates.minimum_f1:
            return PromotionDecision(
                approved=False,
                reason=(
                    f"Promotion blocked: f1 {f1} below minimum {gates.minimum_f1}."
                ),
            )

        if auc is None or auc < gates.minimum_auc:
            return PromotionDecision(
                approved=False,
                reason=(
                    f"Promotion blocked: auc {auc} below minimum {gates.minimum_auc}."
                ),
            )

    return PromotionDecision(
        approved=True,
        reason="Promotion approved.",
    )


def log_promotion_decision(
    spark,
    model_name: str,
    model_version: str,
    decision: PromotionDecision,
    run_id: str,
    source_env: str,
    target_env: str | None,
    accuracy: float | None,
    f1: float | None,
    auc: float | None,
    tests_passed: bool,
    manual_approval: bool,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    from pyspark.sql import Row
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    PROMOTION_DECISION_SCHEMA = T.StructType(
        [
            T.StructField("event_timestamp", T.TimestampType(), False),
            T.StructField("env", T.StringType(), False),
            T.StructField("project", T.StringType(), False),
            T.StructField("model_name", T.StringType(), False),
            T.StructField("model_version", T.StringType(), False),
            T.StructField("source_env", T.StringType(), False),
            T.StructField("target_env", T.StringType(), True),
            T.StructField("approved", T.BooleanType(), False),
            T.StructField("reason", T.StringType(), False),
            T.StructField("accuracy", T.DoubleType(), True),
            T.StructField("f1", T.DoubleType(), True),
            T.StructField("auc", T.DoubleType(), True),
            T.StructField("tests_passed", T.BooleanType(), False),
            T.StructField("manual_approval", T.BooleanType(), False),
            T.StructField("run_id", T.StringType(), False),
        ]
    )

    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_promotion_decision",
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    if spark.catalog.tableExists(table_name):
        exists = (
            spark.table(table_name)
            .filter(F.col("run_id") == run_id)
            .filter(F.col("model_name") == model_name)
            .filter(F.col("model_version") == model_version)
            .filter(F.col("source_env") == source_env)
            .filter(F.col("target_env") == target_env)
            .limit(1)
            .count() > 0
        )

        if exists:
            return

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        source_env=source_env,
        target_env=target_env,
        approved=bool(decision.approved),
        reason=decision.reason,
        accuracy=float(accuracy) if accuracy is not None else None,
        f1=float(f1) if f1 is not None else None,
        auc=float(auc) if auc is not None else None,
        tests_passed=bool(tests_passed),
        manual_approval=bool(manual_approval),
        run_id=run_id,
    )

    df = spark.createDataFrame([row], schema=PROMOTION_DECISION_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
