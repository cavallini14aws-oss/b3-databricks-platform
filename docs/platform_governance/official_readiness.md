# Official Readiness Governance

## Objetivo
Garantir que qualquer projeto da plataforma só rode em ambiente official quando:
- storage estiver pronto
- grants estiverem declarados
- identidade estiver definida
- compute estiver compatível
- naming estiver coerente

## Itens obrigatórios
### Storage
- mode
- catalog
- schema
- volume_catalog
- volume_schema
- volume_name
- volume_path

### Grants
- permissões mínimas de tabela
- permissões mínimas de volume

### Identidade
- tipo de executor permitido
- grupo esperado

### Compute
- modos permitidos
- runtime mínimo
- UC access quando aplicável

### Naming
- catalog esperado
- schema esperado
- volume schema esperado

## Regra
Nenhuma execução official deve avançar se o contract de readiness estiver incompleto.
