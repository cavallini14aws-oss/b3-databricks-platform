# ML Rollback Runbook

## Objetivo

Reverter o modelo ativo de um ambiente para a versão anterior elegível, com troca real de ativo e validação operacional.

## Escopo validado

Este runbook cobre rollback real em:

- `hml`
- `prd`

## Pré-requisitos

Antes do rollback, confirmar:

- existe deployment ativo no `target_env`
- existe pelo menos uma versão anterior elegível no histórico do mesmo `target_env`
- tabela `tb_model_deployments` está acessível
- modelo anterior possui `artifact_path` válido

## Condição obrigatória

Rollback só funciona se houver histórico suficiente no ambiente alvo.

Exemplo:
- se existe apenas uma versão implantada em `prd`, rollback deve falhar
- se existe uma versão ativa e uma anterior inativa elegível, rollback deve executar

## Comando operacional

    from data_platform.governance.rollback import prepare_rollback_request

    result = prepare_rollback_request(
        spark=spark,
        model_name="clientes_status_classifier",
        target_env="prd",
        project="clientes",
        use_catalog=False,
    )

    result

## Resultado esperado

Exemplo:

    {
        "action": "ROLLBACK_EXECUTED",
        "model_name": "clientes_status_classifier",
        "rolled_back_from_version": "VERSAO_ATIVA",
        "rolled_back_to_version": "VERSAO_ANTERIOR",
        "artifact_path": "/Volumes/...",
        "target_env": "prd",
        "status": "ROLLED_BACK",
    }

## Validação pós-rollback

### 1. Confirmar ativo atual por ambiente

    from data_platform.mlops.deployments import get_active_model_for_env

    active = get_active_model_for_env(
        spark=spark,
        model_name="clientes_status_classifier",
        target_env="prd",
        project="clientes",
        use_catalog=False,
    )

    active

Esperado:
- versão anterior reativada
- `deployment_status = ROLLED_BACK_ACTIVE`

### 2. Conferir histórico de deployments

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
              AND target_env = 'prd'
            ORDER BY event_timestamp DESC
        """)
    )

Esperado:
- versão nova desativada com `INACTIVE`
- versão anterior reativada com `ROLLED_BACK_ACTIVE`

## Validação funcional após rollback

Executar batch inference usando o mesmo `target_env`:

    from pipelines.template.ml.batch.batch_inference import run_batch_inference

    run_batch_inference(
        spark=spark,
        input_table="clientes_feature.tb_clientes_scoring_dataset_v2",
        output_table="clientes_mlops.tb_clientes_status_predictions_prd",
        model_name="clientes_status_classifier",
        target_env="prd",
        project="clientes",
        use_catalog=False,
    )

## Falhas comuns

### Nenhum deployment ativo encontrado

Causa:
- ambiente ainda não recebeu promotion

Ação:
- promover primeiro
- validar `tb_model_deployments`

### Nenhuma versão anterior elegível encontrada

Causa:
- só existe uma versão implantada no ambiente alvo

Ação:
- implantar uma segunda versão antes de testar rollback

## Evidências mínimas a capturar

- retorno da função de rollback
- ativo atual após rollback
- histórico de deployments
- evidência de inference após rollback
