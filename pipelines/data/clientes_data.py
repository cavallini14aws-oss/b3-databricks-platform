"""Workload wrapper para clientes_data."""

from pipelines.medallion_clientes import run_medallion_clientes

__all__ = ["run_medallion_clientes"]

if __name__ == "__main__":
    raise SystemExit("Use o job runner da plataforma para invocar run_medallion_clientes.")
