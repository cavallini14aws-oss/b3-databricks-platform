from data_platform.core.config_loader import load_yaml_config
from data_platform.context import get_context
try:
    from data_platform.orchestration.observability import run_with_observability
except ImportError:
    def run_with_observability(**kwargs):
        fn = kwargs["fn"]
        dummy_logger = type("DummyLogger", (), {"info": lambda *a, **k: None})()
        return fn(dummy_logger)


def run_ingest_knowledge(**kwargs):
    return None


def run_chunk_documents(**kwargs):
    return None


def run_build_embeddings(**kwargs):
    return None


def run_build_index(**kwargs):
    return None


def run_evaluate_rag(**kwargs):
    return None


def run_clientes_ai_end_to_end(
    spark,
    project="clientes",
    use_catalog=False,
    config_path="config/clientes_ai_pipeline.yml",
    forced_run_id=None,
):
    cfg = load_yaml_config(config_path)
    ctx = get_context(project=project, use_catalog=use_catalog)

    def _runner(logger):
        run_ingest_knowledge(spark=spark, ctx=ctx, config=cfg, logger=logger, run_id=forced_run_id)

        if cfg.get("chunking", {}).get("enabled", False):
            run_chunk_documents(spark=spark, ctx=ctx, config=cfg, logger=logger, run_id=forced_run_id)

        if cfg.get("embeddings", {}).get("enabled", False):
            run_build_embeddings(spark=spark, ctx=ctx, config=cfg, logger=logger, run_id=forced_run_id)

        if cfg.get("index", {}).get("enabled", False):
            run_build_index(spark=spark, ctx=ctx, config=cfg, logger=logger, run_id=forced_run_id)

        if cfg.get("evaluation", {}).get("enabled", False):
            run_evaluate_rag(spark=spark, ctx=ctx, config=cfg, logger=logger, run_id=forced_run_id)

    return run_with_observability(
        project=project,
        env=ctx.env,
        component="clientes_ai_end_to_end",
        fn=_runner,
        forced_run_id=forced_run_id,
    )
