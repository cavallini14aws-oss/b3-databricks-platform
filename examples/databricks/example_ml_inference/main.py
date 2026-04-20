import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_ml_inference")
logger.setLevel(logging.INFO)

def get_env() -> str:
    return (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"
    target_table = f"{catalog}.ml.tb_exemplo_inference"

    logger.info("Iniciando exemplo ML inference | target=%s", target_table)

    df = spark.createDataFrame(
        [
            ("1", 0.93, "alto"),
            ("2", 0.21, "baixo"),
        ],
        ["id_cliente", "vl_score", "ds_faixa_score"],
    )

    df = df.withColumn("ingestion_date", F.current_timestamp()) \
           .withColumn("update_date", F.current_timestamp())

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo ML inference concluído | linhas=%s", df.count())

if __name__ == "__main__":
    main()
