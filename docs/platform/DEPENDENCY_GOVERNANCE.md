# Dependency Governance

## Regras

1. DEV local pode usar venv local.
2. HML e PRD nao aceitam pip install manual.
3. HML e PRD nao aceitam %pip install.
4. Dependencias de runtime devem ser versionadas em requirements-runtime.txt.
5. Dependencias de desenvolvimento minimo devem ficar em requirements-dev-min.txt.
6. requirements-dev.txt pode compor runtime + dev apenas para conveniencia local.
7. Promocao para HML/PRD deve ocorrer via artifact versionado e processo controlado.
8. Ninguem deve instalar dependencias manualmente pela UI do Databricks em ambientes oficiais.

## Modelo operacional

- DEV:
  - venv local
  - testes locais
  - experimentacao

- HML:
  - deploy controlado
  - sem install manual
  - smoke obrigatorio

- PRD:
  - apenas promocao de artifact validado
  - sem install manual
  - sem mudanca manual de libraries

## Excecoes

Se uma dependencia especial nao existir no runtime:
1. validar em DEV
2. validar compatibilidade com Databricks Runtime
3. promover de forma declarativa/versionada
4. nunca instalar manualmente em HML/PRD
