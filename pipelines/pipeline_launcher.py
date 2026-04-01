from b3_platform.orchestration.pipeline_registry import resolve_pipeline_callable


def run_pipeline(
    spark,
    registry_path: str,
    pipeline_name: str,
) -> None:
    pipeline_callable, definition = resolve_pipeline_callable(
        registry_path=registry_path,
        pipeline_name=pipeline_name,
    )

    config_path = definition.get("config_path")
    if not config_path:
        raise ValueError(f"Pipeline '{pipeline_name}' sem config_path no registry")

    pipeline_callable(
        spark=spark,
        config_path=config_path,
    )
