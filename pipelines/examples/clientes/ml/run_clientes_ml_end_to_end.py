from data_platform.context import get_context
try:
    from data_platform.orchestration.observability import run_with_observability
except ImportError:
    def run_with_observability(**kwargs):
        fn = kwargs["fn"]
        dummy_logger = type("DummyLogger", (), {"info": lambda *a, **k: None})()
        return fn(dummy_logger)


def run_prepare_clientes_training_dataset(**kwargs):
    return None


def run_train_clientes_model(**kwargs):
    return None


def run_evaluate_clientes_model(**kwargs):
    return None


def run_prepare_clientes_scoring_dataset(**kwargs):
    return None


def get_scoring_dataset_table(**kwargs):
    return None


def get_model_artifact_path(**kwargs):
    return None


def log_smoke_run(**kwargs):
    return None


def run_batch_inference(**kwargs):
    return None


def run_clientes_ml_end_to_end(
    spark,
    project="clientes",
    use_catalog=False,
    config_path="config/clientes_ml_pipeline.yml",
    skip_train=False,
    existing_model_version=None,
    forced_run_id=None,
):
    ctx = get_context(project=project, use_catalog=use_catalog)

    def _runner(logger):
        run_prepare_clientes_training_dataset(
            spark=spark,
            ctx=ctx,
            config_path=config_path,
            logger=logger,
            run_id=forced_run_id,
        )

        if skip_train:
            if not existing_model_version:
                raise ValueError("existing_model_version deve ser informado")
            model_version = existing_model_version
        else:
            model_version = run_train_clientes_model(
                spark=spark,
                ctx=ctx,
                config_path=config_path,
                logger=logger,
                run_id=forced_run_id,
            )

        run_evaluate_clientes_model(
            spark=spark,
            ctx=ctx,
            config_path=config_path,
            model_version=model_version,
            logger=logger,
            run_id=forced_run_id,
        )

        run_prepare_clientes_scoring_dataset(
            spark=spark,
            ctx=ctx,
            config_path=config_path,
            logger=logger,
            run_id=forced_run_id,
        )

        input_table = get_scoring_dataset_table(
            spark=spark,
            ctx=ctx,
            config_path=config_path,
        )

        output_table = ctx.naming.qualified_table(
            ctx.naming.schema_mlops,
            f"tb_clientes_status_classifier_smoke_predictions_{ctx.env}",
        )

        artifact_path = get_model_artifact_path(
            spark=spark,
            ctx=ctx,
            model_name="clientes_status_classifier",
            model_version=model_version,
        )

        log_smoke_run(
            spark=spark,
            ctx=ctx,
            model_name="clientes_status_classifier",
            model_version=model_version,
            status="STARTED",
            run_id=forced_run_id,
        )

        run_batch_inference(
            spark=spark,
            ctx=ctx,
            model_name="clientes_status_classifier",
            model_version=model_version,
            input_table=input_table,
            output_table=output_table,
            target_env=ctx.env,
            artifact_path=artifact_path,
            logger=logger,
            run_id=forced_run_id,
        )

        log_smoke_run(
            spark=spark,
            ctx=ctx,
            model_name="clientes_status_classifier",
            model_version=model_version,
            status="SUCCESS",
            run_id=forced_run_id,
        )

        return {"model_version": model_version}

    return run_with_observability(
        project=project,
        env=ctx.env,
        component="clientes_ml_end_to_end",
        fn=_runner,
        forced_run_id=forced_run_id,
    )
