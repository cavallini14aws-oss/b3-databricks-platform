from __future__ import annotations

import os
import tempfile

import mlflow
import pandas as pd
import yaml
from pathlib import Path

from databricks.pdf_rag.src.custom_pyfunc_model import PdfRagCustomPyFunc


def load_config() -> dict:
    path = Path("databricks/pdf_rag/config/pdf_rag_custom_serving.yml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))["custom_serving"]


def main() -> None:
    cfg = load_config()
    registered_model_name = cfg["uc_model_name"]

    example_input = pd.DataFrame([{"prompt": "Responda apenas OK."}])

    with mlflow.start_run():
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=PdfRagCustomPyFunc(),
            input_example=example_input,
            registered_model_name=registered_model_name,
        )

    print(f"[OK] custom pyfunc model logged and registered: {registered_model_name}")


if __name__ == "__main__":
    main()
