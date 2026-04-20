import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_rag_mlflow")
logger.setLevel(logging.INFO)

def get_env() -> str:
    return (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"
    target_table = f"{catalog}.slv.tb_exemplo_rag_mlflow_chunks"

    logger.info("Iniciando exemplo RAG MLflow | target=%s", target_table)
    logger.info("Ponto de integração esperado: experiment tracking, registry e serving via MLflow")

    df = spark.createDataFrame(
        [
            ("doc_1", "manual.pdf", "Trecho com trilha MLflow", 0, "mlflow"),
        ],
        ["id_documento", "nm_arquivo", "ds_chunk", "nr_chunk", "ds_variant"],
    )

    df = df.withColumn("ingestion_date", F.current_timestamp()) \
           .withColumn("update_date", F.current_timestamp())

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo RAG MLflow concluído | linhas=%s", df.count())

if __name__ == "__main__":
    main()
