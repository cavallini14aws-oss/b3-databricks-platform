import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_data_gold")
logger.setLevel(logging.INFO)

def get_env() -> str:
    env = (os.getenv("LOCAL_ENV", "") or "").strip()
    return env or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"

    source_table = f"{catalog}.slv.tb_exemplo_silver"
    target_table = f"{catalog}.gld.tb_exemplo_gold"

    logger.info("Iniciando exemplo Gold | source=%s | target=%s", source_table, target_table)

    df = spark.table(source_table)

    df_out = df.groupBy("dt_referencia").agg(
        F.countDistinct("id_cliente").alias("qt_clientes"),
        F.current_timestamp().alias("ingestion_date"),
        F.current_timestamp().alias("update_date"),
    )

    df_out.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo Gold concluído com sucesso | linhas=%s", df_out.count())

if __name__ == "__main__":
    main()
