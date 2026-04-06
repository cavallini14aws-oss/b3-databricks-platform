# ML Promotion Runbook

## Objetivo

Executar promoção de modelo entre ambientes de forma controlada, com evidência operacional e validação do deployment ativo.

## Escopo validado

Este runbook cobre:

- promoção `dev -> hml`
- promoção `hml -> prd`
- ativação real do modelo no ambiente alvo
- validação do deployment ativo
- validação de batch inference usando o deployment ativo

## Pré-requisitos

Antes da promoção, confirmar:

- modelo registrado com `artifact_path` válido
- modelo com status promovível
- avaliação concluída com sucesso
- decisão de promotion aprovada
- ambiente alvo conhecido (`hml` ou `prd`)
- tabela de deployments existente e operacional
- tabela de scoring dataset disponível para batch inference

## Estados promovíveis atualmente aceitos

- `TRAINED`
- `EVALUATED`
- `PROMOTION_APPROVED`
- `PROMOTED_HML`
- `PROMOTED_PRD`
- `DEPLOYED_HML`
- `DEPLOYED_PRD`
- `ROLLED_BACK`

## Comando operacional em notebook / Databricks

    from data_platform.governance.promote_and_deploy_ml import promote_and_deploy_ml

    result = promote_and_deploy_ml(
        spark=spark,
        model_name="clientes_status_classifier",
        source_env="dev",
        target_env="hml",
        model_version="MODEL_VERSION",
        project="clientes",
        use_catalog=False,
    )

    result

Para `prd`:

    result = promote_and_deploy_ml(
        spark=spark,
        model_name="clientes_status_classifier",
        source_env="hml",
        target_env="prd",
        model_version="MODEL_VERSION",
        project="clientes",
        use_catalog=False,
    )

## Validação pós-promoção

### 1. Confirmar retorno da função

Esperado:

- `model_name`
- `model_version`
- `artifact_path`
- `source_env`
- `target_env`
- `status` final (`DEPLOYED_HML` ou `DEPLOYED_PRD`)

### 2. Confirmar ativo por ambiente

    from data_platform.mlops.deployments import get_active_model_for_env

    active = get_active_model_for_env(
        spark=spark,
        model_name="clientes_status_classifier",
        target_env="hml",
        project="clientes",
        use_catalog=False,
    )

    active

### 3. Conferir histórico de deployments

    display(
        spark.sql("""
            SELECT
                event_timestamp,
                model_name,
                model_version,
                target_env,
                deployment_status,
                is_active,
                notes
            FROM clientes_mlops.tb_model_deployments
            WHERE model_name = 'clientes_status_classifier'
              AND target_env = 'hml'
            ORDER BY event_timestamp DESC
        """)
    )

## Validação funcional com batch inference

Após promotion, validar inferência usando o deployment ativo:

    from pipelines.template.ml.batch.batch_inference import run_batch_inference

    run_batch_inference(
        spark=spark,
        input_table="clientes_feature.tb_clientes_scoring_dataset_v2",
        output_table="clientes_mlops.tb_clientes_status_predictions_hml",
        model_name="clientes_status_classifier",
        target_env="hml",
        project="clientes",
        use_catalog=False,
    )

Para `prd`, trocar:

- `target_env="prd"`
- `output_table="clientes_mlops.tb_clientes_status_predictions_prd"`

## Comportamentos esperados

### Promotion normal

- nova versão ativa no ambiente alvo
- versão anterior marcada como `INACTIVE`
- registry atualizado com `PROMOTION_APPROVED`, `PROMOTED_*`, `DEPLOYED_*`

### Promotion idempotente

Se a mesma versão já estiver ativa no ambiente alvo, esperado:

    {
      "message": "model already active in target environment"
    }

Sem nova troca de ativo.

## Falhas comuns

### Status inválido para promoção

Causa:
- status fora de `PROMOTABLE_SOURCE_STATES`

Ação:
- verificar status atual no registry
- confirmar se a versão correta foi passada

### Modelo sem `artifact_path`

Causa:
- treino antigo / registry inconsistente

Ação:
- retreinar e registrar versão com artifact persistido

### Nenhum ativo encontrado no target de inference

Causa:
- promotion não executada no ambiente alvo

Ação:
- validar `tb_model_deployments`
- confirmar `target_env`

## Evidências mínimas a capturar

- retorno da função de promotion
- ativo atual por ambiente
- histórico de deployments
- evidência de batch inference concluída
