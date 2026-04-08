# ML HML Rehearsal Runbook

## Objetivo
Validar o comportamento do bloco ML/MLOps em HML antes de PRD.

## Fluxos obrigatórios
1. treino
2. avaliação
3. promotion
4. deploy
5. smoke
6. drift crítico simulado
7. alert event
8. notification delivery
9. post-production evaluation
10. abertura de retraining request
11. aprovação manual
12. execução de retraining
13. validação pós-retraining
14. readiness / operational report

## Critérios de aprovação
- nenhum erro crítico de execução
- alerting funcional
- postprod persistido
- retraining request aberto corretamente
- runner operacional consistente
- blockers de go-live revisados
