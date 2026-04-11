# Production Onboarding Checklist

## GitHub
- [ ] Branch protection habilitada
- [ ] Required status checks configurados
- [ ] PR gate obrigatório antes de merge
- [ ] Deploy HML/PRD apenas via workflow
- [ ] Environments `dev`, `hml`, `prd` configurados
- [ ] Approvals por environment configurados

## Databricks
- [ ] Host real definido
- [ ] Token ou service principal configurado
- [ ] Profile name definido
- [ ] Workspace path validado
- [ ] Policies de jobs/compute definidas
- [ ] ACLs por ambiente definidas

## Secrets
- [ ] DATABRICKS_HOST
- [ ] DATABRICKS_TOKEN
- [ ] DATABRICKS_PROFILE_NAME
- [ ] SMTP secrets
- [ ] Slack webhook secrets
- [ ] Teams webhook secrets
- [ ] quaisquer segredos adicionais do cliente

## Operação
- [ ] DEV smoke aprovado
- [ ] HML rehearsal aprovado
- [ ] blockers HML zerados
- [ ] blockers PRD zerados
- [ ] gate final retornando ALLOW
- [ ] artifact/version guard aprovado
- [ ] rollback path documentado

## Promotion Readiness
- [ ] activation_preflight = ALLOW/WARN aceitável para ambiente alvo
- [ ] go_no_go_policy = GO
- [ ] pr_merge_gate = ALLOW
