import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_data_silver")
logger.setLevel(logging.INFO)

def get_env() -> str:
    env = (os.getenv("LOCAL_ENV", "") or "").strip()
    return env or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"

    source_table = f"{catalog}.brz.tb_exemplo_bronze"
    target_table = f"{catalog}.slv.tb_exemplo_silver"

    logger.info("Iniciando exemplo Silver | source=%s | target=%s", source_table, target_table)

    df = spark.table(source_table)

    df_out = df.select(
        F.col("id_cliente_origem").alias("id_cliente"),
        F.trim(F.col("nm_cliente")).alias("nm_cliente"),
        F.lower(F.trim(F.col("ds_email"))).alias("ds_email"),
        F.regexp_replace(F.col("nr_telefone"), "[^0-9]", "").alias("nr_telefone"),
        F.col("dt_referencia"),
        F.current_timestamp().alias("ingestion_date"),
        F.current_timestamp().alias("update_date"),
    )

    df_out.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo Silver concluído com sucesso | linhas=%s", df_out.count())

if __name__ == "__main__":
    main()
