# Exemplo didático de cadastro na tabela de controle de anonimização.
# Baseado no padrão real da plataforma:
# - tabela de controle central
# - grant_view_dados
# - owner
# - pass_masking
# - anonimizar

import os

env = (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"
controle = f"{env}.datahub_bronze.anonimization"
catalog = f"clientes_{env}"

colunas_sensiveis = [
    ("tb_exemplo_bronze", "nm_cliente"),
    ("tb_exemplo_bronze", "ds_email"),
    ("tb_exemplo_bronze", "nr_telefone"),
]

for tabela, coluna in colunas_sensiveis:
    spark.sql(f"""
    MERGE INTO {controle} AS tgt
    USING (
      SELECT
        '{catalog}' AS catalog,
        'brz' AS schema,
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
