# Política de Arquivos Locais Sensíveis

## Objetivo
Definir como tratar arquivos locais de ambiente e segredos durante o desenvolvimento local, evitando que esses arquivos contaminem pacotes, ZIPs de revisão ou integrações com ambiente oficial.

## Arquivos considerados sensíveis
Padrões locais sensíveis:
- `.env.*.local`
- `.secrets.*.local`
- `.secrets.*.oauth.local`

## Regras
1. Esses arquivos podem existir localmente para desenvolvimento.
2. Esses arquivos não devem ser considerados parte do pacote limpo do projeto.
3. Esses arquivos não devem ser incluídos em ZIP de revisão, pacote de integração ou entrega para ambiente oficial.
4. Quando necessário, devem existir arquivos `.example` equivalentes sem valores reais.
5. Se qualquer arquivo local sensível contiver valor real, avaliar rotação antes de qualquer compartilhamento indevido.

## Estratégia operacional
- `bin/audit-sensitive-local-files` lista os arquivos locais sensíveis encontrados.
- `bin/check-clean-packaging repo` reprova se esses arquivos existirem no workspace.
- `bin/create-local-sensitive-templates` gera arquivos `.example` a partir da convenção oficial, sem copiar valores reais.

## Decisão de plataforma
O ambiente oficial deve ser alimentado por contratos, secret scopes e integrações oficiais.
Nunca por arquivos locais reais versionados ou empacotados.
