from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from databricks.pdf_rag.src.custom_pyfunc_model import PdfRagCustomPyFunc


def main() -> None:
    model = PdfRagCustomPyFunc()

    df = pd.DataFrame(
        [
            {
                "question": "Do que trata o Livro Vermelho de Jung?",
                "num_results": 3,
                "max_context_chars": 3000,
                "max_tokens": 300,
                "temperature": 0.1,
            }
        ]
    )

    result = model.predict(context=None, model_input=df)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
