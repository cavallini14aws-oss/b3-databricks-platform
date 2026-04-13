# PDF RAG Lab - Official Inference Strategy

## Phase 1 official target

Official target path for the pilot:

- Real-time inference: Databricks Foundation Model APIs or foundation model serving endpoints
- Batch inference: Databricks AI Functions using ai_query
- Governance and monitoring: AI Gateway-enabled inference tables
- Endpoint telemetry: deferred to the custom serving endpoint phase only

## Operational principle

Ollama is not removed from code yet, but it is disabled operationally for the official path.

The official path is Databricks-native.

## Why this strategy

- Foundation Model APIs are the fastest Databricks-native path for hosted models
- ai_query is the preferred general-purpose function for batch and endpoint inference workflows
- AI Gateway and inference tables provide governance and observability
- Endpoint telemetry is reserved for custom model serving endpoint scenarios
