# SQL conventions

## create_tables
Arquivos de bootstrap estrutural.
Usar para criação inicial de tabelas e schemas base.
Preferir `CREATE TABLE IF NOT EXISTS`.

## alter_tables
Arquivos de evolução estrutural.
Usar para mudança controlada de schema.
Nomear com sequência:
- 001_...
- 002_...
- 003_...

## views
Arquivos de criação e manutenção de views.
Usar `CREATE OR REPLACE VIEW` quando fizer sentido.
