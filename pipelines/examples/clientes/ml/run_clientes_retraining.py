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


def execute_retraining_request(**kwargs):
    return {}


def run_clientes_retraining(
    spark,
    request_payload,
    project="clientes",
    use_catalog=False,
    forced_run_id=None,
):
    ctx = get_context(project=project, use_catalog=use_catalog)

    def _runner(logger):
        if request_payload.get("request_status") != "APPROVED":
            raise ValueError("Somente retraining request APPROVED")

        run_prepare_clientes_training_dataset(
            spark=spark,
            ctx=ctx,
            request_payload=request_payload,
            logger=logger,
            run_id=forced_run_id,
        )

        new_model_version = run_train_clientes_model(
            spark=spark,
            ctx=ctx,
            request_payload=request_payload,
            logger=logger,
            run_id=forced_run_id,
        )

        run_evaluate_clientes_model(
            spark=spark,
            ctx=ctx,
            request_payload=request_payload,
            model_version=new_model_version,
            logger=logger,
            run_id=forced_run_id,
        )

        result = execute_retraining_request(
            spark=spark,
            ctx=ctx,
            request_payload=request_payload,
            new_model_version=new_model_version,
            logger=logger,
            run_id=forced_run_id,
        )

        result["new_model_version"] = new_model_version
        return result

    return run_with_observability(
        project=project,
        env=ctx.env,
        component="clientes_retraining",
        fn=_runner,
        forced_run_id=forced_run_id,
    )
