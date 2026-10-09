import os
import sys
import time
import uuid
import json
import statistics
from warnings import filterwarnings

import psycopg2
from sqlalchemy import text

from sentence_transformers import SentenceTransformer

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.env_loader import CONSTRING
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger, log_event
from pipelines.observability import current_step


from pipelines.commons.ai.rag_query_router import carregar_dimensoes, recuperar_contexto

logger = get_logger("rag04_eval_retrieval")

# ==========================================
# TARGETS & CONFIG
# ==========================================
SCHEMA = "ai"
TABELA_CONTEXTO = "rag_context_anp"
TABELA_METRICAS = "rag_eval_metrics"
TABELA_RESUMO = "rag_eval_summary"
MODELO_NOME = "intfloat/multilingual-e5-base"
TOP_K = 5

VERSAO_PIPELINE = os.getenv("RAG_PIPELINE_VERSION", "v2_router")

# Quality gate: the evaluation fails the task (and alerts) when the RAG regresses
MIN_ROUTING_ACC = float(os.getenv("RAG_MIN_ROUTING_ACC", "0.9"))
MIN_VALUE_ACC = float(os.getenv("RAG_MIN_VALUE_ACC", "0.9"))

_JOIN = """FROM gold.ft_anp_combustiveis a
    JOIN gold.dm_postos b ON b.id_posto_sk = a.id_posto_sk
    JOIN gold.dm_produtos c ON c.id_produto_sk = a.id_produto_sk"""

# ==========================================
# GOLDEN SET
# ==========================================

GOLDEN_SET = [
    # --- vetorial: perguntas pontuais, sem agregacao ---
    {"pergunta": "qual o valor da gasolina no posto ipiranga do centro de anapolis?",
     "tipo": "pontual", "metodo_esperado": "vetorial", "municipio_esperado": "ANAPOLIS"},
    {"pergunta": "qual o preco do etanol em Aparecida de Goiania?",
     "tipo": "pontual", "metodo_esperado": "vetorial", "municipio_esperado": "APARECIDA DE GOIANIA"},
    {"pergunta": "qual o cnpj da revenda Posto Sao Francisco do posto em Anapolis?",
     "tipo": "pontual_nome", "metodo_esperado": "vetorial", "cnpj_esperado": "25080920000198"},
    {"pergunta": "qual e o cnpj do posto sao francisco ltda em anapols - go ?",
     "tipo": "pontual_typo", "metodo_esperado": "vetorial", "cnpj_esperado": "25080920000198"},

    # --- identificador exato (CNPJ) ---
    {"pergunta": "qual e o posto com cnpj 25080920000198 ?",
     "tipo": "identificador", "metodo_esperado": "sql_detalhe", "cnpj_esperado": "25080920000198"},
    {"pergunta": "qual o valor do diesel s10 do cnpj 25080920000198 em Anapolis - GO?",
     "tipo": "identificador", "metodo_esperado": "sql_detalhe", "cnpj_esperado": "25080920000198"},

    # --- agregadas (verdade independente em SQL) ---
    {"pergunta": "qual o valor maximo da gasolina do cnpj 25080920000198?",
     "tipo": "agregada_max", "metodo_esperado": "sql_agregado", "cnpj_esperado": "25080920000198",
     "sql_verdade": f"SELECT MAX(a.valor_venda) {_JOIN} WHERE b.cnpj = '25080920000198' AND c.produto = 'GASOLINA'"},
    {"pergunta": "quantas revendas existem em Anapolis?",
     "tipo": "agregada_count", "metodo_esperado": "sql_agregado",
     "sql_verdade": "SELECT COUNT(DISTINCT cnpj) FROM gold.dm_postos WHERE estado_sigla = 'GO' AND municipio = 'ANAPOLIS'"},
    {"pergunta": "qual o preco medio do etanol em Anapolis?",
     "tipo": "agregada_avg", "metodo_esperado": "sql_agregado",
     "sql_verdade": f"SELECT AVG(a.valor_venda) {_JOIN} WHERE b.estado_sigla = 'GO' AND b.municipio = 'ANAPOLIS' AND c.produto = 'ETANOL'"},
    {"pergunta": "qual o menor preco de gasolina em Goiania?",
     "tipo": "agregada_min", "metodo_esperado": "sql_agregado",
     "sql_verdade": f"SELECT MIN(a.valor_venda) {_JOIN} WHERE b.estado_sigla = 'GO' AND b.municipio = 'GOIANIA' AND c.produto = 'GASOLINA'"},
    {"pergunta": "quais postos vendem diesel s10 em Goiania?",
     "tipo": "agregada_lista", "metodo_esperado": "sql_agregado",
     "sql_verdade": f"SELECT COUNT(DISTINCT b.cnpj) {_JOIN} WHERE b.estado_sigla = 'GO' AND b.municipio = 'GOIANIA' AND c.produto = 'DIESEL S10'"},


    {"pergunta": "quantos postos existem em Sao Paulo?",
     "tipo": "negativa", "metodo_esperado": "vetorial", "deveria_ter_contexto": False},
]

# ==========================================
# DDL
# ==========================================
DDL_METRICAS = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABELA_METRICAS} (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    executado_em TIMESTAMP NOT NULL DEFAULT now(),
    pergunta TEXT NOT NULL,
    tipo_consulta TEXT NOT NULL,
    top_k INT NOT NULL,
    similaridade_media DOUBLE PRECISION,
    similaridade_min DOUBLE PRECISION,
    similaridade_max DOUBLE PRECISION,
    taxa_redundancia DOUBLE PRECISION,
    cnpjs_distintos_retornados INT,
    total_postos_esperado_municipio INT,
    cobertura_municipio DOUBLE PRECISION,
    precision_at_k DOUBLE PRECISION,
    latencia_retrieval_ms DOUBLE PRECISION,
    observacoes TEXT
);
"""

DDL_RESUMO = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABELA_RESUMO} (
    run_id UUID PRIMARY KEY,
    executado_em TIMESTAMP NOT NULL DEFAULT now(),
    modelo_embedding TEXT,
    top_k INT,
    total_perguntas INT,
    total_registros_contexto INT,
    municipios_distintos_contexto INT,
    bandeiras_distintas_contexto INT,
    similaridade_media_geral DOUBLE PRECISION,
    taxa_redundancia_media DOUBLE PRECISION,
    latencia_media_ms DOUBLE PRECISION,
    plano_execucao_usa_indice BOOLEAN,
    plano_execucao_detalhe TEXT
);
"""

# evolucao do schema sem quebrar as tabelas/runs ja existentes (v1)
COLUNAS_NOVAS_METRICAS = [
    ("versao_pipeline", "TEXT"), ("metodo_esperado", "TEXT"), ("metodo_obtido", "TEXT"),
    ("acerto_roteamento", "BOOLEAN"), ("valor_obtido", "DOUBLE PRECISION"),
    ("valor_esperado", "DOUBLE PRECISION"), ("acerto_valor", "BOOLEAN"),
]
COLUNAS_NOVAS_RESUMO = [
    ("versao_pipeline", "TEXT"), ("acuracia_roteamento", "DOUBLE PRECISION"),
    ("acuracia_valor", "DOUBLE PRECISION"), ("registros_gold_go", "INT"),
    ("paridade_contexto_gold", "BOOLEAN"),
]

CAMPOS_METRICAS = [
    "pergunta", "tipo_consulta", "top_k", "similaridade_media", "similaridade_min", "similaridade_max",
    "taxa_redundancia", "cnpjs_distintos_retornados", "total_postos_esperado_municipio",
    "cobertura_municipio", "precision_at_k", "latencia_retrieval_ms", "observacoes",
    "versao_pipeline", "metodo_esperado", "metodo_obtido", "acerto_roteamento",
    "valor_obtido", "valor_esperado", "acerto_valor",
]
CAMPOS_RESUMO = [
    "modelo_embedding", "top_k", "total_perguntas", "total_registros_contexto",
    "municipios_distintos_contexto", "bandeiras_distintas_contexto", "similaridade_media_geral",
    "taxa_redundancia_media", "latencia_media_ms", "plano_execucao_usa_indice", "plano_execucao_detalhe",
    "versao_pipeline", "acuracia_roteamento", "acuracia_valor", "registros_gold_go", "paridade_contexto_gold",
]


# ==========================================
# HELPERS
# ==========================================
def garantir_tabelas(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL_METRICAS))
        conn.execute(text(DDL_RESUMO))
        for col, tipo in COLUNAS_NOVAS_METRICAS:
            conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABELA_METRICAS} ADD COLUMN IF NOT EXISTS {col} {tipo}"))
        for col, tipo in COLUNAS_NOVAS_RESUMO:
            conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABELA_RESUMO} ADD COLUMN IF NOT EXISTS {col} {tipo}"))


def contar_postos_no_municipio(engine, municipio: str) -> int:

    query = text("""
        SELECT COUNT(DISTINCT b.cnpj) FROM gold.dm_postos b
        WHERE b.estado_sigla = 'GO' AND UPPER(b.municipio) = UPPER(:municipio)
    """)
    with engine.connect() as conn:
        return int(conn.execute(query, {"municipio": municipio}).scalar() or 0)


def estatisticas_contexto(engine) -> dict:
    query = text(f"""
        SELECT COUNT(*), COUNT(DISTINCT metadata->>'municipio')
        FROM {SCHEMA}.{TABELA_CONTEXTO}
    """)
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()
    return {"total_registros": int(row[0]), "municipios_distintos": int(row[1])}


def bandeiras_distintas_gold(engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(
            "SELECT COUNT(DISTINCT bandeira) FROM gold.dm_postos WHERE estado_sigla = 'GO'"
        )).scalar() or 0)


def registros_gold_go(engine) -> int:

    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) {_JOIN} WHERE b.estado_sigla = 'GO'")).scalar() or 0)


def plano_execucao_usa_indice(conn_pg, vetor, top_k: int):
    cur = conn_pg.cursor()
    cur.execute(f"""
        EXPLAIN ANALYZE
        SELECT chunk_text, metadata, 1 - (embedding <=> %s::vector) AS similaridade
        FROM {SCHEMA}.{TABELA_CONTEXTO}
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
    """, (vetor, vetor, top_k))
    plano = "\n".join(r[0] for r in cur.fetchall())
    cur.close()
    return ("Index Scan" in plano) or ("Index Only Scan" in plano), plano


def _meta(m) -> dict:
    if isinstance(m, dict):
        return m
    try:
        return json.loads(m)
    except Exception:
        return {}


def _iguais(a, b) -> bool:
    return a is not None and b is not None and abs(float(a) - float(b)) < 0.005


def avaliar_pergunta(engine, modelo_ia, dims, item: dict, top_k: int) -> dict:
    r = dict.fromkeys(CAMPOS_METRICAS)
    r.update(pergunta=item["pergunta"], tipo_consulta=item["tipo"], top_k=top_k,
             versao_pipeline=VERSAO_PIPELINE, metodo_esperado=item.get("metodo_esperado"))

    inicio = time.perf_counter()
    resultados, metodo, _ = recuperar_contexto(item["pergunta"], modelo_ia, engine, dims, top_k)
    r["latencia_retrieval_ms"] = (time.perf_counter() - inicio) * 1000
    r["metodo_obtido"] = metodo
    if item.get("metodo_esperado"):
        r["acerto_roteamento"] = (metodo == item["metodo_esperado"])

    metas = [_meta(m) for _, m, _ in resultados]
    cnpjs = [m.get("cnpj") for m in metas if m.get("cnpj")]

    if item.get("cnpj_esperado"):
        r["precision_at_k"] = 1.0 if item["cnpj_esperado"] in cnpjs else 0.0

    if metodo == "vetorial" and resultados:
        sims = [float(s) for _, _, s in resultados]
        r["similaridade_media"], r["similaridade_min"], r["similaridade_max"] = statistics.mean(sims), min(sims), max(sims)
        r["cnpjs_distintos_retornados"] = len(set(cnpjs))
        r["taxa_redundancia"] = 1 - (len(set(cnpjs)) / len(cnpjs)) if cnpjs else None
        if item.get("municipio_esperado"):
            total = contar_postos_no_municipio(engine, item["municipio_esperado"])
            r["total_postos_esperado_municipio"] = total
            r["cobertura_municipio"] = len(set(cnpjs)) / total if total else None
        if item.get("deveria_ter_contexto") is False:
            r["observacoes"] = (f"pergunta fora do escopo: o retrieval devolveu contexto com "
                                f"similaridade maxima {max(sims):.3f} (falso positivo de contexto)")
    elif metodo != "vetorial" and metas:
        r["valor_obtido"] = metas[0].get("valor")

    if item.get("sql_verdade"):
        with engine.connect() as conn:
            esperado = conn.execute(text(item["sql_verdade"])).scalar()
        r["valor_esperado"] = None if esperado is None else float(esperado)
        r["acerto_valor"] = _iguais(r["valor_obtido"], r["valor_esperado"])
    return r


def _media(valores):
    valores = [v for v in valores if v is not None]
    return statistics.mean(valores) if valores else None


def _taxa(valores):
    valores = [v for v in valores if v is not None]
    return sum(1 for v in valores if v) / len(valores) if valores else None


def gravar(engine, tabela, run_id, campos, dados: dict):
    cols = ", ".join(["run_id"] + campos)
    vals = ", ".join([":run_id"] + [f":{c}" for c in campos])
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO {SCHEMA}.{tabela} ({cols}) VALUES ({vals})"),
                     {"run_id": str(run_id), **{c: dados.get(c) for c in campos}})


def _fmt(v, f="{:.3f}"):
    return "-" if v is None else f.format(v)


def imprimir_relatorio(resultados: list, resumo: dict):
    print("\n" + "=" * 72)
    print(f"📊 RELATÓRIO DE AVALIAÇÃO DO PIPELINE RAG ({VERSAO_PIPELINE})")
    print("=" * 72)
    for r in resultados:
        rota = ""
        if r["acerto_roteamento"] is not None:
            rota = f"  esperado={r['metodo_esperado']}  {'✅' if r['acerto_roteamento'] else '❌'}"
        print(f"\n[{r['tipo_consulta'].upper()}] {r['pergunta']}")
        print(f"  método obtido : {r['metodo_obtido']}{rota}")
        if r["metodo_obtido"] == "vetorial" and r["similaridade_media"] is not None:
            print(f"  similaridade média/min/max : {r['similaridade_media']:.3f} / {r['similaridade_min']:.3f} / {r['similaridade_max']:.3f}")
            print(f"  redundância (mesmo CNPJ)   : {_fmt(r['taxa_redundancia'], '{:.2%}')}")
            if r["total_postos_esperado_municipio"] is not None:
                print(f"  cobertura do município     : {_fmt(r['cobertura_municipio'], '{:.2%}')} de {r['total_postos_esperado_municipio']} postos")
        if r["precision_at_k"] is not None:
            print(f"  precision@{r['top_k']} (CNPJ esperado no contexto) : {r['precision_at_k']:.0%}")
        if r["acerto_valor"] is not None:
            print(f"  valor obtido={_fmt(r['valor_obtido'], '{:.2f}')}  esperado(SQL)={_fmt(r['valor_esperado'], '{:.2f}')}  {'✅' if r['acerto_valor'] else '❌'}")
        if r["observacoes"]:
            print(f"  obs: {r['observacoes']}")
        print(f"  latência ponta a ponta : {r['latencia_retrieval_ms']:.0f} ms")

    print("\n" + "-" * 72)
    print("RESUMO")
    print("-" * 72)
    print(f"  registros na camada de contexto        : {resumo['total_registros_contexto']}")
    print(f"  registros na gold (GO)                 : {resumo['registros_gold_go']}  -> paridade: {'OK' if resumo['paridade_contexto_gold'] else 'DIVERGENTE'}")
    print(f"  municípios distintos indexados         : {resumo['municipios_distintos_contexto']}")
    print(f"  acurácia de roteamento                 : {_fmt(resumo['acuracia_roteamento'], '{:.0%}')}")
    print(f"  acurácia de valor (agregações vs SQL)  : {_fmt(resumo['acuracia_valor'], '{:.0%}')}")
    print(f"  similaridade média (só vetorial)       : {_fmt(resumo['similaridade_media_geral'])}")
    print(f"  redundância média (só vetorial)        : {_fmt(resumo['taxa_redundancia_media'], '{:.2%}')}")
    print(f"  latência média ponta a ponta           : {_fmt(resumo['latencia_media_ms'], '{:.0f}')} ms")
    print(f"  plano usa índice vetorial              : {'SIM' if resumo['plano_execucao_usa_indice'] else 'NÃO (Seq Scan)'}")
    print("=" * 72)


# ==========================================
# PIPELINE
# ==========================================
def main():
    test_pg_connection()
    engine = get_sqla_engine()
    garantir_tabelas(engine)
    dims = carregar_dimensoes(engine)

    logger.info(f"carregando modelo de embeddings: {MODELO_NOME}")
    modelo_ia = SentenceTransformer(MODELO_NOME)
    conn_pg = psycopg2.connect(CONSTRING)

    run_id = uuid.uuid4()
    logger.info(f"iniciando avaliacao ({VERSAO_PIPELINE}). run_id={run_id}")

    resultados = []
    for item in GOLDEN_SET:
        logger.info(f"avaliando: '{item['pergunta']}'")
        resultados.append(avaliar_pergunta(engine, modelo_ia, dims, item, TOP_K))

    stats = estatisticas_contexto(engine)
    n_gold = registros_gold_go(engine)
    vetor_exemplo = modelo_ia.encode([f"query: {GOLDEN_SET[0]['pergunta']}"], normalize_embeddings=True)[0].tolist()
    usa_indice, plano = plano_execucao_usa_indice(conn_pg, vetor_exemplo, TOP_K)
    conn_pg.close()

    vetoriais = [r for r in resultados if r["metodo_obtido"] == "vetorial" and r["similaridade_media"] is not None]
    resumo = {
        "versao_pipeline": VERSAO_PIPELINE, "modelo_embedding": MODELO_NOME, "top_k": TOP_K,
        "total_perguntas": len(resultados),
        "total_registros_contexto": stats["total_registros"],
        "municipios_distintos_contexto": stats["municipios_distintos"],
        "bandeiras_distintas_contexto": bandeiras_distintas_gold(engine),
        "similaridade_media_geral": _media([r["similaridade_media"] for r in vetoriais]),
        "taxa_redundancia_media": _media([r["taxa_redundancia"] for r in vetoriais]),
        "latencia_media_ms": _media([r["latencia_retrieval_ms"] for r in resultados]),
        "plano_execucao_usa_indice": usa_indice, "plano_execucao_detalhe": plano,
        "acuracia_roteamento": _taxa([r["acerto_roteamento"] for r in resultados]),
        "acuracia_valor": _taxa([r["acerto_valor"] for r in resultados]),
        "registros_gold_go": n_gold,
        "paridade_contexto_gold": (n_gold == stats["total_registros"]),
    }

    for r in resultados:
        gravar(engine, TABELA_METRICAS, run_id, CAMPOS_METRICAS, r)
    gravar(engine, TABELA_RESUMO, run_id, CAMPOS_RESUMO, resumo)

    imprimir_relatorio(resultados, resumo)
    logger.info(f"avaliacao concluida e gravada em {SCHEMA}.{TABELA_METRICAS} / {SCHEMA}.{TABELA_RESUMO}")
    logger.info(f"run_id={run_id}")

    # ---------------------------------------------------------------- quality gate
    acc_rot, acc_val = resumo["acuracia_roteamento"], resumo["acuracia_valor"]
    current_step().set(eval_run_id=str(run_id), routing_accuracy=acc_rot, value_accuracy=acc_val,
                       latency_ms=resumo["latencia_media_ms"], index_used=usa_indice,
                       context_parity=resumo["paridade_contexto_gold"], rows_out=len(resultados))
    falhas = []
    if acc_rot is not None and acc_rot < MIN_ROUTING_ACC:
        falhas.append(f"routing accuracy {acc_rot:.0%} < {MIN_ROUTING_ACC:.0%}")
    if acc_val is not None and acc_val < MIN_VALUE_ACC:
        falhas.append(f"value accuracy {acc_val:.0%} < {MIN_VALUE_ACC:.0%}")
    log_event(logger, "rag_eval_gate", "FAILED: " + "; ".join(falhas) if falhas else "passed",
              level=40 if falhas else 20, run_id_eval=str(run_id), routing_accuracy=acc_rot,
              value_accuracy=acc_val, passed=not falhas)
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())