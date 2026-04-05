# Table Specs

## Objetivo
Padronizar a definição declarativa de tabelas na plataforma.

## Estrutura
- templates/: exemplos e modelos de referência
- projects/: specs reais por domínio/projeto

## Regra de uso
O desenvolvedor deve criar novas tabelas declarativas apenas em:
- data_platform/table_specs/projects/<dominio>/

## O que é obrigatório na spec
- catalog_name ou None
- schema_name
- table_name
- table_description
- columns com comment em todas as colunas
- table_properties mínimas Delta
- table_tags com descricao_tabela
- column_tags com pii e classificacao para todas as colunas

## O que a plataforma faz
- valida a spec
- cria catalog/schema/table conforme flags
- aplica owner se informado
- aplica tags de tabela
- aplica tags de coluna
- registra histórico técnico

## O que não fazer
- não criar spec em templates para uso real
- não hardcodar domínio na plataforma
- não alterar framework para encaixar uma tabela específica
