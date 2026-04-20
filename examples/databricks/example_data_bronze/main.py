# Exemplo oficial Bronze
# O dev deve alterar:
# - nome da origem
# - regra de leitura
# - mapeamento de colunas
# - regra de escrita
# O dev não deve quebrar:
# - logging
# - padrão de escrita
# - governança associada à tabela

import logging
import os
from pyspark.sql import functions as F

logger = logging.getLogger("example_data_bronze")
logger.setLevel(logging.INFO)

def get_env() -> str:
    env = (os.getenv("LOCAL_ENV", "") or "").strip()
    return env or "dev"

def main():
    env = get_env()
    catalog = f"clientes_{env}"
    target_table = f"{catalog}.brz.tb_exemplo_bronze"

    logger.info("Iniciando exemplo Bronze | env=%s | target=%s", env, target_table)

    df = spark.createDataFrame(
        [
            ("1", "Maria da Silva", "maria@email.com", "11999999999", "2026-04-20"),
            ("2", "João Souza", "joao@email.com", "11888888888", "2026-04-20"),
        ],
        ["id_cliente_origem", "nm_cliente", "ds_email", "nr_telefone", "dt_referencia"],
    )

    df = df.withColumn("ingestion_date", F.current_timestamp()) \
           .withColumn("update_date", F.current_timestamp())

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    logger.info("Exemplo Bronze concluído com sucesso | linhas=%s", df.count())

if __name__ == "__main__":
    main()
