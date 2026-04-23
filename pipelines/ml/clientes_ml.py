"""Workload wrapper para clientes_ml."""

from pipelines.examples.clientes.ml.run_clientes_ml_end_to_end import run_clientes_ml_end_to_end

__all__ = ["run_clientes_ml_end_to_end"]

if __name__ == "__main__":
    raise SystemExit("Use o job runner da plataforma para invocar run_clientes_ml_end_to_end.")
