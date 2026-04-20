import os

def get_env() -> str:
    env = (os.getenv("LOCAL_ENV", "") or "").strip()
    return env or "dev"

env = get_env()
tabela = f"clientes_{env}.brz.tb_exemplo_bronze"

spark.sql(f"""
ALTER TABLE {tabela}
SET TAGS (
  'periodicidade'           = 'diario',
  'sla'                     = 'N/A',
  'data_owner'              = 'OWNER_EXEMPLO',
  'data_steward'            = 'STEWARD_EXEMPLO',
  'tempo_retencao_dados'    = 'N/A',
  'particionada'            = 'False',
  'classificacao_seguranca' = 'Interno',
  'pii'                     = 'True',
  'dominio_negocio'         = 'cadastro_cliente',
  'sensivel'                = 'sim',
  'descricao_tabela'        = 'Exemplo oficial de tabela Bronze com governança'
)
""")

tags_colunas = {
    "id_cliente_origem": ("não", "interno", "aberto"),
    "nm_cliente": ("sim", "confidencial", "mascarado"),
    "ds_email": ("sim", "confidencial", "mascarado"),
    "nr_telefone": ("sim", "confidencial", "mascarado"),
    "dt_referencia": ("não", "interno", "aberto"),
    "ingestion_date": ("não", "interno", "aberto"),
    "update_date": ("não", "interno", "aberto"),
}

for coluna, (pii, classificacao, mascaramento) in tags_colunas.items():
    spark.sql(f"""
    ALTER TABLE {tabela}
    ALTER COLUMN {coluna}
    SET TAGS (
      'pii' = '{pii}',
      'classificacao' = '{classificacao}',
      'mascaramento' = '{mascaramento}'
    )
    """)
