# ML Degradation Runbook

## Objetivo
Definir o fluxo operacional para lidar com degradação de modelo em produção.

## Fontes primárias de detecção
- `tb_model_drift_monitoring`
- `tb_ml_alert_events`
- `tb_model_postprod_metrics`
- `tb_model_retraining_requests`

## Severidades
### WARNING
Indica desvio moderado. Exige investigação e acompanhamento.

### CRITICAL
Indica risco alto ou degradação confirmada. Exige ação operacional imediata.

## Papéis
### MLOps
- analisar drift
- analisar alertas
- abrir ou acompanhar retraining request
- coordenar execução da esteira

### Plataforma
- garantir disponibilidade dos jobs e canais
- apoiar falhas de execução, secrets, webhooks e tabelas

### Negócio
- validar impacto real
- apoiar decisão de threshold, aceitação ou rejeição de reação

## Fluxo operacional
1. verificar alert event mais recente
2. confirmar severidade
3. verificar postprod metrics quando disponível
4. decidir entre:
   - observação
   - rollback
   - retraining
5. registrar decisão
6. acompanhar pós-ação

## Critérios de decisão
### Drift WARNING
- manter monitoramento
- não abrir retraining automaticamente
- investigar fonte do desvio

### Drift CRITICAL
- abrir retraining request
- avaliar necessidade de rollback
- verificar impacto em predictions e postprod

### Postprod degradation CRITICAL
- abrir retraining request
- considerar rollback se houver versão estável anterior
- priorizar revisão do threshold e da causa

## Quando considerar rollback
- degradação começou logo após promotion
- existe versão anterior estável
- evidência aponta regressão de versão

## Quando considerar retraining
- drift persistente
- degradação real em produção
- mudança de comportamento do negócio
- conceito original envelheceu

## Evidências mínimas
- alert event
- métricas de drift
- métricas de postprod
- status do retraining request
- versão atual e versões anteriores relevantes

## Tabelas principais
- `tb_model_registry`
- `tb_model_deployments`
- `tb_model_drift_monitoring`
- `tb_ml_alert_events`
- `tb_model_postprod_metrics`
- `tb_model_retraining_requests`

## Saídas esperadas
- incidente classificado
- ação definida
- trilha auditável preservada
