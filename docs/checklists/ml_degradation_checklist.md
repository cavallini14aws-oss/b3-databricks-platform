# ML Degradation Checklist

## Detecção
- [ ] houve alert event relevante
- [ ] severidade foi classificada
- [ ] drift foi confirmado
- [ ] postprod metrics foram consultadas quando disponíveis

## Diagnóstico
- [ ] versão atual do modelo foi identificada
- [ ] histórico recente de promotion foi revisado
- [ ] risco de regressão de versão foi avaliado
- [ ] hipótese de quebra de dado/pipeline foi considerada

## Decisão
- [ ] manter observação
- [ ] executar rollback
- [ ] abrir retraining request
- [ ] aprovar ou rejeitar retraining request

## Evidências
- [ ] tabelas consultadas foram registradas
- [ ] versão alvo foi registrada
- [ ] reason foi documentado
- [ ] status final foi persistido

## Pós-ação
- [ ] verificar novos alert events
- [ ] verificar postprod metrics
- [ ] verificar smoke / readiness
