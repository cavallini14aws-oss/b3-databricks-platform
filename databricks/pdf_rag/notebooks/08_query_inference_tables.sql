-- Databricks SQL / Notebook SQL cell
-- Exemplo base de consulta em inference tables habilitadas via AI Gateway

SELECT
  request_id,
  event_time,
  status_code,
  request,
  response
FROM main.pdf_rag_monitoring.pdf_rag_payload
ORDER BY event_time DESC
LIMIT 50;
