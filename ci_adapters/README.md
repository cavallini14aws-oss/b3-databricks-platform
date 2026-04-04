# CI Adapters

## Objetivo
Permitir troca de provider de CI/CD sem reescrever a plataforma.

## Fonte de verdade
- providers ativos: `config/ci_providers.yml`
- contrato de secrets: `config/ci_secrets_contract.yml`

## Providers suportados
- github_actions
- azure_devops
- bitbucket
- aws

## Regra
- github_actions ligado por padrão
- demais desligados, mas prontos
- troca de provider deve ocorrer apenas alterando flags no config

## Como usar
1. editar `config/ci_providers.yml`
2. validar provider ativo
3. validar contrato de secrets
4. gerar adapters

## Comandos úteis
### Mostrar contrato de secrets
- `python -m b3_platform.orchestration.show_ci_secrets_contract --provider github_actions`

### Validar secrets do ambiente atual
- `python -m b3_platform.orchestration.validate_ci_secrets --provider github_actions`

### Gerar adapters
- `python -m b3_platform.orchestration.generate_ci_adapters`
- `python -m b3_platform.orchestration.generate_ci_adapters --write-all true`

## Governança recomendada
- dev: deploy permitido
- hml: controlado
- prd: approval obrigatório + credencial isolada
