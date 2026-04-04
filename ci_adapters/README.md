# CI Adapters

## Objetivo
Permitir troca de provider de CI/CD sem reescrever a plataforma.

## Fonte de verdade
A ativação dos providers fica em:
- config/ci_providers.yml

## Providers suportados
- github_actions
- azure_devops
- bitbucket
- aws

## Regra
- github_actions ligado por padrão
- demais desligados, mas gerados e prontos
- troca de provider deve ocorrer apenas alterando flags no config

## Como usar
1. editar `config/ci_providers.yml`
2. rodar gerador:
   - python -m b3_platform.orchestration.generate_ci_adapters
3. consumir o adapter desejado na esteira do cliente

## Governança recomendada
- dev: deploy permitido
- hml: controlado
- prd: approval obrigatório + credencial isolada
