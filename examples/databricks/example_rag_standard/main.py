import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_rag_standard")
logger.setLevel(logging.INFO)

def get_env() -> str:
    return (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"

    source_volume = f"/Volumes/{catalog}/raw/documentos"
    target_table = f"{catalog}.slv.tb_exemplo_rag_chunks"

    logger.info("Iniciando exemplo RAG standard | volume=%s | target=%s", source_volume, target_table)

    df = spark.createDataFrame(
        [
            ("doc_1", "contrato.pdf", "Trecho do documento 1", 0),
            ("doc_1", "contrato.pdf", "Trecho do documento 1 - parte 2", 1),
        ],
        ["id_documento", "nm_arquivo", "ds_chunk", "nr_chunk"],
    )

    df = df.withColumn("ingestion_date", F.current_timestamp()) \
           .withColumn("update_date", F.current_timestamp())

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo RAG standard concluído | linhas=%s", df.count())

if __name__ == "__main__":
    main()
