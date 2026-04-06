# ML Rollback Checklist

## Antes do rollback

- [ ] ambiente alvo confirmado
- [ ] modelo ativo atual identificado
- [ ] versão anterior elegível confirmada
- [ ] `artifact_path` da versão anterior confirmado
- [ ] histórico de deployments revisado
- [ ] evidência do estado pré-rollback salva

## Durante o rollback

- [ ] função `prepare_rollback_request(...)` executada
- [ ] retorno `ROLLBACK_EXECUTED` confirmado
- [ ] versão anterior reativada confirmada
- [ ] versão atual marcada como `INACTIVE`

## Depois do rollback

- [ ] `get_active_model_for_env(...)` revalidado
- [ ] `tb_model_deployments` inspecionada
- [ ] batch inference executado após rollback
- [ ] saída pós-rollback validada
- [ ] evidências anexadas
