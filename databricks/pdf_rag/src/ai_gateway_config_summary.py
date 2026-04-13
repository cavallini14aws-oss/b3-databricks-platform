from __future__ import annotations

from pathlib import Path
import yaml


def main() -> None:
    path = Path("databricks/pdf_rag/config/pdf_rag_ai_gateway.yml")
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))["ai_gateway"]

    print("[INFO] AI Gateway config")
    print(f"[INFO] endpoint_name_env={cfg['endpoint_name_env']}")
    print(f"[INFO] enabled={cfg['enabled']}")
    print(f"[INFO] usage_tracking={cfg['usage_tracking']}")
    print(f"[INFO] inference_tables_enabled={cfg['inference_tables_enabled']}")
    print(f"[INFO] inference_table_catalog={cfg['inference_table_catalog']}")
    print(f"[INFO] inference_table_schema={cfg['inference_table_schema']}")
    print(f"[INFO] inference_table_prefix={cfg['inference_table_prefix']}")


if __name__ == "__main__":
    main()
