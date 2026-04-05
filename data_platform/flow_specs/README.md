# Flow Specs

## Objetivo
Padronizar a definição declarativa de fluxos na plataforma.

## Estrutura
- templates/: exemplos e modelos de referência
- projects/: flow specs reais por domínio/projeto

## Regra de uso
O desenvolvedor deve criar novos fluxos apenas em:
- data_platform/flow_specs/projects/<dominio>/

## O que é obrigatório no FLOW_SPEC
- flow_name
- flow_type: data | ml | llm
- project
- domain
- description
- entrypoint
- callable_name
- tags:
  - owner
  - criticality
  - schedule

## O que a plataforma faz
- valida o flow spec
- carrega o callable
- executa o fluxo
- prepara o terreno para geração futura de registry/jobs/bundle

## O que não fazer
- não criar fluxo real em templates
- não editar framework para encaixar um caso isolado
- não criar YAML manual fora do padrão da plataforma
