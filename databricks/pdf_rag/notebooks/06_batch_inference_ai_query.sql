-- Databricks SQL / Notebook SQL cell
-- Exemplo base para batch inference com ai_query sobre chunks já persistidos em Delta

SELECT
  chunk_id,
  document_id,
  ai_query(
    endpoint => '${DATABRICKS_FOUNDATION_ENDPOINT}',
    request => CONCAT(
      'Resuma em uma frase o conteúdo deste trecho: ',
      chunk_text
    )
  ) AS chunk_summary
FROM main.pdf_rag.pdf_chunks
LIMIT 20;
