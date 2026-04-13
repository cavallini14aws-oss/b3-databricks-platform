# PDF RAG - Custom Model Serving Roadmap

## Quando usar este caminho
Use Mosaic AI Model Serving customizado somente quando Foundation Model APIs não forem suficientes.

## Casos típicos
- modelo pyfunc próprio
- pipeline de inferência custom
- múltiplos served entities
- fallback entre entidades
- controle fino do endpoint
- segredos/variáveis de ambiente no endpoint

## Sequência
1. Registrar modelo pyfunc em Unity Catalog
2. Criar endpoint customizado
3. Validar smoke do endpoint
4. Opcional: ligar AI Gateway no endpoint custom
5. Validar inference tables

## Observações
- O modelo deve estar registrado em Unity Catalog ou workspace registry
- O endpoint pode ser criado via UI, REST API ou SDK
- Para recursos externos, usar secrets-based environment variables no endpoint
