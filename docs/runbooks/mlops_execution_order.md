# MLOps Execution Order

## Ordem oficial dos ciclos operacionais
1. drift_cycle
2. postprod_cycle
3. retraining_cycle
4. operational_cycle

## Objetivo
Evitar execução fora de ordem e reduzir inconsistência operacional.

## Regra
- `drift_cycle` deve rodar primeiro
- `postprod_cycle` depende da visão atual do ambiente
- `retraining_cycle` deve considerar sinais já produzidos
- `operational_cycle` consolida o estado final
