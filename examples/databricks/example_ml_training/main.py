import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_ml_training")
logger.setLevel(logging.INFO)

def get_env() -> str:
    return (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"
    target_table = f"{catalog}.ml.tb_exemplo_training_runs"

    logger.info("Iniciando exemplo ML training | target=%s", target_table)

    df = spark.createDataFrame(
        [
            ("run_001", "modelo_x", 0.91, 0.88),
        ],
        ["id_run", "nm_modelo", "vl_metric_auc", "vl_metric_f1"],
    )

    df = df.withColumn("ingestion_date", F.current_timestamp()) \
           .withColumn("update_date", F.current_timestamp())

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo ML training concluído | linhas=%s", df.count())

if __name__ == "__main__":
    main()
