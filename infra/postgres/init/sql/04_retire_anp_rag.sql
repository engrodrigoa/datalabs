-- ADR 0020: the ANP RAG (fuel price assistant) was retired; the LC 214 RAG is the lab's only AI product.
-- Idempotent cleanup for volumes created before it: on a fresh volume every statement is a no-op. Applied like every
-- DDL file (Postgres entrypoint, dag_setup_infrastructure, make db-bootstrap). It can be deleted once no volume from
-- before ADR 0020 is left.
--
-- Scope, checked against the LC 214 before writing it: pipeline 'rag' in the audit tables was written only by
-- dag_rag_anp (the LC 214 writes 'lc214'), and app 'anp_assistant' in ai.llm_call only by the ANP assistant
-- (the LC 214 writes 'lc214_generate'). Nothing else in the ai schema is touched.

-- vector store and evaluation history of the ANP RAG
DROP TABLE IF EXISTS ai.rag_context_anp;
DROP TABLE IF EXISTS ai.rag_eval_metrics;
DROP TABLE IF EXISTS ai.rag_eval_summary;

-- its step runs and health checks, so the Grafana "last run of every step" and check tables stop listing a product
-- that no longer exists: build_context / evaluate_retrieval (pipeline 'rag') and health_checks_rag, which the runner
-- records under pipeline 'obs' (dag_helpers.health_checks): hence the dag_id
DELETE FROM audit.pipeline_step_runs WHERE pipeline = 'rag' OR dag_id = 'dag_rag_anp';
DELETE FROM audit.data_quality_checks WHERE pipeline = 'rag' OR dag_id = 'dag_rag_anp';

-- its LLM calls (ADR 0018). ai.llm_call is created by 07_llm_usage.sql, after this file: guarded on a fresh volume
DO $$
BEGIN
    IF to_regclass('ai.llm_call') IS NOT NULL THEN
        DELETE FROM ai.llm_call WHERE app = 'anp_assistant';
    END IF;
END $$;
