from pathlib import Path

from data_platform.aiops.retrieval.storage import (
    ai_local_base_path,
    is_local_ai_mode,
)
from data_platform.aiops.retrieval.table_names import resolve_silver_schema
from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context


def validate_ai_storage_target(
    *,
    spark,
    project: str,
    config_path: str,
    use_catalog: bool | None = None,
) -> dict:
    cfg = load_yaml_config(config_path)
    ctx = get_context(project=project, use_catalog=use_catalog)

    local_mode = is_local_ai_mode(cfg)

    if local_mode:
        base_path = Path(ai_local_base_path(cfg, project))
        base_path.mkdir(parents=True, exist_ok=True)

        probe_file = base_path / ".write_probe"
        probe_file.write_text("ok", encoding="utf-8")
        probe_file.unlink()

        return {
            "status": "ALLOW",
            "storage_mode": "path",
            "target": str(base_path),
            "details": "base path existe e é gravável",
        }

    schema = resolve_silver_schema(ctx)
    probe_table = f"{schema}.zz_ai_storage_probe_{project}"

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        spark.sql(f"CREATE TABLE {probe_table} (id STRING)")
        spark.sql(f"INSERT INTO {probe_table} VALUES ('ok')")
        count = spark.table(probe_table).count()
        spark.sql(f"DROP TABLE IF EXISTS {probe_table}")

        if count != 1:
            return {
                "status": "BLOCK",
                "storage_mode": "table",
                "target": schema,
                "details": "probe table criada mas count inesperado",
            }

        return {
            "status": "ALLOW",
            "storage_mode": "table",
            "target": schema,
            "details": "schema existe e é gravável",
        }

    except Exception as exc:
        return {
            "status": "BLOCK",
            "storage_mode": "table",
            "target": schema,
            "details": str(exc),
        }
