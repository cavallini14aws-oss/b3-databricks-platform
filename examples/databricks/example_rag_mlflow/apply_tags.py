import os

env = (os.getenv("LOCAL_ENV", "") or "").strip() or "dev"
tabela = f"clientes_{env}.slv.tb_exemplo_rag_mlflow_chunks"

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
  'dominio_negocio'         = 'rag_documentos',
  'sensivel'                = 'sim',
  'descricao_tabela'        = 'Exemplo oficial de chunks de RAG com MLflow e governança'
)
""")

tags_colunas = {
    "id_documento": ("não", "interno", "aberto"),
    "nm_arquivo": ("não", "interno", "aberto"),
    "ds_chunk": ("sim", "confidencial", "controlado"),
    "nr_chunk": ("não", "interno", "aberto"),
    "ds_variant": ("não", "interno", "aberto"),
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
