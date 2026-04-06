# ML Promotion Checklist

## Antes da promoção

- [ ] modelo correto identificado
- [ ] `model_version` confirmada
- [ ] `artifact_path` válido confirmado
- [ ] avaliação concluída com sucesso
- [ ] decisão de promotion aprovada
- [ ] ambiente de origem confirmado
- [ ] ambiente de destino confirmado
- [ ] deployment atual do target conhecido
- [ ] tabela de scoring disponível
- [ ] evidências da versão anterior salvas

## Durante a promoção

- [ ] função `promote_and_deploy_ml(...)` executada
- [ ] retorno da função salvo
- [ ] status final esperado confirmado (`DEPLOYED_HML` ou `DEPLOYED_PRD`)
- [ ] ativo do target revalidado

## Depois da promoção

- [ ] `get_active_model_for_env(...)` validado
- [ ] `tb_model_deployments` inspecionada
- [ ] batch inference executado com `target_env` correto
- [ ] saída materializada com sucesso
- [ ] evidências anexadas
