# Runtime Install Governance

## Regra da plataforma
- DEV: manual pip install permitido
- HML: manual pip install somente com exceção aprovada
- PRD: manual pip install somente com exceção aprovada

## Canais aprovados em HML/PRD
- wheel oficial da plataforma
- requirements/runtime oficial aprovado

## Canais sujeitos a exceção
- pip install manual em workflow
- %pip install em notebook operacional
- dbutils.library.installPyPI
- python -m pip install em scripts operacionais

## Exceção
Qualquer exceção em HML/PRD deve estar registrada em:
`docs/platform_governance/runtime_install_exceptions.yml`

## Objetivo
Garantir consistência de runtime entre ambientes e evitar drift de dependência.
