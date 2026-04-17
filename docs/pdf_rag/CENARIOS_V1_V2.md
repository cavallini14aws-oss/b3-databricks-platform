# CENÁRIOS V1 E V2 DO PDF RAG

## 1. Visão geral

O projeto passa a manter dois cenários complementares para o PDF RAG:

- V1 Basic
- V2 MLflow

Esses cenários não competem.

A V1 é a referência funcional.

A V2 será a referência servível e governada.

## 2. Cenário 1: V1 Basic

### O que é

A V1 Basic é a versão funcional e operacional do PDF RAG Databricks-first.

Ela demonstra o fluxo completo de RAG usando notebooks, Delta, Vector Search e Foundation Models.

### Objetivo

Servir como blueprint funcional para:

- ingestão de PDFs
- auto-discovery
- ingestão incremental
- chunking
- persistência em Delta
- Vector Search
- query vetorial
- resposta RAG PT-BR
- smoke E2E

### Quando usar

Use a V1 para:

- onboarding técnico
- entendimento da arquitetura
- debugging
- ajustes de chunking
- ajustes de retrieval
- validação de fluxo
- demonstração funcional

### Para quem serve

A V1 serve principalmente para:

- data engineers
- platform engineers
- desenvolvedores que precisam entender o fluxo
- pessoas estudando arquitetura RAG em Databricks

## 3. Cenário 2: V2 MLflow

### O que é

A V2 MLflow será uma versão apartada que reaproveita a lógica central da V1, mas transforma a inferência em um ativo empacotado, registrável e servível.

### Objetivo

Servir como blueprint para:

- empacotamento com mlflow.pyfunc
- registro de modelo
- versionamento
- inferência padronizada
- serving endpoint
- governança de deploy
- validação de produção

### Quando usar

Use a V2 para:

- registrar modelo
- testar predict local/Databricks
- publicar serving
- validar inferência governada
- preparar promoção para HML/PRD

### Para quem serve

A V2 serve principalmente para:

- ML engineers
- MLOps engineers
- arquitetos de plataforma
- times responsáveis por serving e operação de modelos

## 4. Relação entre V1 e V2

A V1 não substitui a V2.

A V2 não invalida a V1.

A relação correta é:

V1 = entender e operar o fluxo
V2 = empacotar e servir o fluxo

## 5. O que a V2 pode reaproveitar da V1

A V2 pode reaproveitar:

- lógica de recuperação de contexto
- configuração de endpoints
- resolução de ambiente
- estratégia de Foundation Models
- contrato de entrada e saída
- conhecimento validado sobre Vector Search

## 6. O que a V2 não deve fazer

A V2 não deve:

- destruir a V1
- mover notebooks da V1 sem necessidade
- alterar o comportamento já validado do smoke E2E
- misturar serving MLflow com notebooks básicos
- tratar rate limit do Free Edition como bug da arquitetura

## 7. Regra de ouro

V1 = referência funcional
V2 = referência MLflow/serving

## 8. Ordem correta de evolução

A ordem correta é:

1. congelar a documentação estratégica da V1
2. documentar promotion path
3. documentar relação V1/V2
4. abrir branch nova para V2
5. criar árvore apartada da V2
6. implementar pyfunc
7. criar smoke de predict
8. evoluir para registro e serving

## 9. Estrutura esperada da V2

A estrutura exata ainda será definida na branch da V2, mas deve ser apartada da V1.

Exemplo conceitual:

databricks/pdf_rag_v2_mlflow/

ou:

src/pdf_rag_v2_mlflow/

A decisão final deve considerar o layout real do repositório antes da implementação.

## 10. Resultado esperado

Com V1 e V2 coexistindo, o projeto passa a ter dois blueprints:

- um blueprint funcional e didático
- um blueprint enterprise, empacotado e servível
