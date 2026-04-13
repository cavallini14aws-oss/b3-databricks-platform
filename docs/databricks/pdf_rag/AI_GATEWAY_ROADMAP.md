# PDF RAG - AI Gateway Roadmap

## Objetivo
Habilitar governança e monitoramento do endpoint oficial de geração usado pelo PDF RAG.

## Itens
- Criar endpoint oficial no Databricks
- Habilitar AI Gateway
- Configurar AI Gateway-enabled inference tables
- Persistir logs em Unity Catalog Delta
- Usar as tabelas para monitoramento, avaliação e auditoria

## Requisitos
- Endpoint já criado e funcional
- Unity Catalog habilitado
- Permissão de CREATE TABLE no catalog/schema das inference tables
- Permissão de gerenciamento no endpoint

## Fluxo operacional
1. Abrir o endpoint no Databricks
2. Editar AI Gateway
3. Ativar inference tables
4. Selecionar catalog/schema
5. Validar geração da tabela payload
6. Consultar a tabela via SQL e notebooks

## Observações
- Preferir AI Gateway-enabled inference tables ao caminho legado
- O nome da tabela pode usar o padrão `<catalog>.<schema>.<endpoint-name>_payload`
- Payloads muito grandes podem não ser logados integralmente
