import os

env = (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"
tabela = f"clientes_{env}.gld.tb_exemplo_gold"

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
  'pii'                     = 'False',
  'dominio_negocio'         = 'cadastro_cliente',
  'sensivel'                = 'nao',
  'descricao_tabela'        = 'Exemplo oficial de tabela Gold com governança'
)
""")

tags_colunas = {
    "dt_referencia": ("não", "interno", "aberto"),
    "qt_clientes": ("não", "interno", "aberto"),
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
