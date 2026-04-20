# Exemplo de referência:
# Caso o dataset de treino inclua PII em features, o cadastro de masking
# deve ser feito antes da exposição da tabela técnica.

import os

env = (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"
controle = f"{env}.datahub_bronze.anonimization"
catalog = f"clientes_{env}"

# Exemplo ilustrativo de coluna sensível em dataset técnico
for tabela, coluna in [("tb_exemplo_training_dataset", "nm_cliente")]:
    spark.sql(f"""
    MERGE INTO {controle} AS tgt
    USING (
      SELECT
        '{catalog}' AS catalog,
        'ml' AS schema,
        '{tabela}' AS tabela,
        '{coluna}' AS coluna
    ) src
    ON tgt.catalog = src.catalog
       AND tgt.schema = src.schema
       AND tgt.tabela = src.tabela
       AND tgt.coluna = src.coluna
    WHEN MATCHED THEN UPDATE SET
      tgt.anonimizar = true,
      tgt.pass_masking = true,
      tgt.grant_view_dados = array("GRUPO_AUTORIZADO"),
      tgt.acesso_a_tabela = array("GRUPO_AUTORIZADO"),
      tgt.tipo_de_acesso = 'SELECT',
      tgt.owner = array("OWNER_EXEMPLO")
    WHEN NOT MATCHED THEN INSERT (
      catalog, schema, tabela, coluna,
      anonimizar, pass_masking,
      grant_view_dados, acesso_a_tabela, tipo_de_acesso, owner
    )
    VALUES (
      src.catalog, src.schema, src.tabela, src.coluna,
      true, true,
      array("GRUPO_AUTORIZADO"), array("GRUPO_AUTORIZADO"), 'SELECT', array("OWNER_EXEMPLO")
    )
    """)
