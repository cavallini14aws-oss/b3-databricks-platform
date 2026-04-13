from __future__ import annotations

import mlflow.pyfunc
import pandas as pd


class PdfRagCustomPyFunc(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        outputs = []
        for _, row in model_input.iterrows():
            prompt = row.get("prompt", "")
            outputs.append(
                {
                    "response": f"[placeholder-custom-serving] {prompt}",
                }
            )
        return pd.DataFrame(outputs)
