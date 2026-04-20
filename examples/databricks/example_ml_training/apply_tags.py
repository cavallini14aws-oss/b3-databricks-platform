import os

env = (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"
tabela = f"clientes_{env}.ml.tb_exemplo_training_runs"

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
  'dominio_negocio'         = 'machine_learning',
  'sensivel'                = 'nao',
  'descricao_tabela'        = 'Exemplo oficial de tracking técnico de treino com governança'
)
""")

tags_colunas = {
    "id_run": ("não", "interno", "aberto"),
    "nm_modelo": ("não", "interno", "aberto"),
    "vl_metric_auc": ("não", "interno", "aberto"),
    "vl_metric_f1": ("não", "interno", "aberto"),
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
