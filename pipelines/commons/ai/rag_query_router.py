"""
Query router for the ANP RAG: exact SQL on the gold layer when the question asks for an
identifier (CNPJ) or an aggregation (count/avg/min/max/list); vector search (pgvector) otherwise.
Every path returns the same shape: (chunk_text, metadata, similarity) -> similarity 1.0 = exact SQL.
"""
import os
import sys
import re
import calendar
import difflib
import unicodedata
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Optional
from warnings import filterwarnings

from sqlalchemy import text

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.logger import get_logger

logger = get_logger("rag_query_router")

# ==========================================
# CONFIG
# ==========================================
UF = os.getenv("RAG_UF", "GO").upper()  # scope of the MVP (one state); same env var used by rag01
if not re.fullmatch(r"[A-Z]{2}", UF):
    raise ValueError(f"RAG_UF must be a 2-letter UF, got {UF!r}")
LIMITE_LISTA = 15            # max de postos listados em perguntas "quais postos..."
LIMITE_DETALHE = 12          # max de registros em consulta por CNPJ
JANELA_RECENTE_DIAS = 7      # "mais barato", "atual" => ultimos 7 dias da base
FUZZY_CUTOFF = 0.85          # tolerancia a erro de digitacao (produto/municipio/bandeira)
DEDUP_VETORIAL = True        # over-fetch + dedup por (posto, produto) no caminho vetorial
VETORIAL_OVERFETCH = 6       # busca top_k * 6 candidatos e depois deduplica

_CNPJ_PATTERN = re.compile(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})")
_MESES = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
    "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}
_FROM = """
    FROM gold.ft_anp_combustiveis a
    JOIN gold.dm_postos b ON b.id_posto_sk = a.id_posto_sk
    JOIN gold.dm_produtos c ON c.id_produto_sk = a.id_produto_sk
"""


# ==========================================
# DIMENSOES (vocabulario vem da propria gold, nao de lista fixa no codigo)
# ==========================================
@dataclass
class Dimensoes:
    produtos: dict      # texto normalizado -> valor original da gold
    municipios: dict
    bandeiras: dict
    data_min: date
    data_max: date


def _norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^a-z0-9/\- ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _para_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def carregar_dimensoes(engine) -> Dimensoes:
    """Carrega uma vez (startup) os valores validos de produto/municipio/bandeira
    e o intervalo de datas. O roteador so 'reconhece' o que existe na gold."""
    def distintos(conn, sql):
        return [r[0] for r in conn.execute(text(sql)).fetchall() if r[0]]

    with engine.connect() as conn:
        produtos = distintos(conn, "SELECT DISTINCT produto FROM gold.dm_produtos")
        municipios = distintos(conn, f"SELECT DISTINCT municipio FROM gold.dm_postos WHERE estado_sigla = '{UF}'")
        bandeiras = distintos(conn, f"SELECT DISTINCT bandeira FROM gold.dm_postos WHERE estado_sigla = '{UF}'")
        dt = conn.execute(text(f"SELECT MIN(a.data_coleta), MAX(a.data_coleta) {_FROM} WHERE b.estado_sigla = '{UF}'")).fetchone()

    dims = Dimensoes(
        produtos={_norm(p): p for p in produtos},
        municipios={_norm(m): m for m in municipios},
        bandeiras={_norm(b): b for b in bandeiras},
        data_min=_para_date(dt[0]),
        data_max=_para_date(dt[1]),
    )
    logger.info(
        f"dimensoes carregadas: {len(produtos)} produtos, {len(municipios)} municipios, "
        f"{len(bandeiras)} bandeiras, periodo {dims.data_min} a {dims.data_max}"
    )
    return dims


# ==========================================
# INTERPRETACAO DA PERGUNTA
# ==========================================
@dataclass
class Intencao:
    cnpj: Optional[str] = None
    produto: Optional[str] = None
    municipio: Optional[str] = None
    bandeira: Optional[str] = None
    agregacao: Optional[str] = None      # COUNT | STATS | MIN | MAX | LISTA | None
    dt_ini: Optional[date] = None
    dt_fim: Optional[date] = None
    periodo_recente: bool = False


def extrair_cnpj(pergunta: str) -> Optional[str]:
    """CNPJ (14 digitos, com ou sem mascara). Embeddings nao tem nocao de
    identificador exato: pergunta por NOME funciona no vetorial, por CNPJ nao."""
    m = _CNPJ_PATTERN.search(pergunta)
    if not m:
        return None
    digitos = re.sub(r"\D", "", m.group(1))
    return digitos if len(digitos) == 14 else None


def _achar_entidade(texto: str, termos: dict) -> Optional[str]:
    # 1) match exato por palavra inteira, termo mais longo primeiro
    #    (ex.: 'gasolina aditivada' antes de 'gasolina', 'aparecida de goiania' antes de 'goiania')
    for termo in sorted(termos, key=len, reverse=True):
        if re.search(rf"(?<![a-z0-9]){re.escape(termo)}(?![a-z0-9])", texto):
            return termos[termo]
    # 2) fallback tolerante a erro de digitacao (janela com o mesmo numero de palavras)
    palavras = texto.split()
    melhor, melhor_score = None, FUZZY_CUTOFF
    for termo, original in termos.items():
        if len(termo) < 5:
            continue
        k = len(termo.split())
        for i in range(len(palavras) - k + 1):
            score = difflib.SequenceMatcher(None, " ".join(palavras[i:i + k]), termo).ratio()
            if score > melhor_score:
                melhor, melhor_score = original, score
    return melhor


def _detectar_agregacao(t: str) -> Optional[str]:
    if re.search(r"\b(quantos|quantas|quantidade|numero de)\b", t):
        return "COUNT"
    tem_min = re.search(r"\b(minimo|minima|mais barat[oa]|mais baix[oa]|menor|menores)\b", t)
    tem_max = re.search(r"\b(maximo|maxima|mais car[oa]|mais alt[oa]|maior|maiores)\b", t)
    tem_avg = re.search(r"\b(media|medio)\b", t)
    if tem_avg or (tem_min and tem_max):
        return "STATS"
    if tem_min:
        return "MIN"
    if tem_max:
        return "MAX"
    # LISTA so quando o objeto e 'postos/revendas' (evita "quais produtos o posto X vende")
    if re.search(r"\b(quais|liste|listar)\s+(os\s+)?(postos|revendas|revendedores)\b", t):
        return "LISTA"
    return None


def _intervalo_mes(ano: int, mes: int):
    return date(ano, mes, 1), date(ano, mes, calendar.monthrange(ano, mes)[1])


def _parse_periodo(t: str, dims: Dimensoes):
    try:
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
        if m:
            d = date(int(m[1]), int(m[2]), int(m[3]))
            return d, d
        m = re.search(r"\bmes\s+(\d{1,2})\s+de\s+(\d{4})\b", t) or re.search(r"\b(\d{1,2})/(\d{4})\b", t)
        if m:
            return _intervalo_mes(int(m[2]), int(m[1]))
        m = re.search(rf"\b({'|'.join(_MESES)})\b(?:\s+de)?\s*(\d{{4}})?", t)
        if m:
            ano = int(m[2]) if m[2] else dims.data_max.year
            return _intervalo_mes(ano, _MESES[m[1]])
        if re.search(r"\b(ultimo mes|mes passado)\b", t):
            return _intervalo_mes(dims.data_max.year, dims.data_max.month)
    except ValueError:
        pass
    return None, None


def interpretar(pergunta: str, dims: Dimensoes) -> Intencao:
    t = _norm(_CNPJ_PATTERN.sub(" ", pergunta))
    i = Intencao(
        cnpj=extrair_cnpj(pergunta),
        produto=_achar_entidade(t, dims.produtos),
        municipio=_achar_entidade(t, dims.municipios),
        bandeira=_achar_entidade(t, dims.bandeiras),
        agregacao=_detectar_agregacao(t),
    )
    i.dt_ini, i.dt_fim = _parse_periodo(t, dims)
    if not i.dt_ini:
        if re.search(r"\b(mais barat[oa]|mais baix[oa]|mais car[oa]|mais alt[oa]|atual|atualmente|hoje|recente|recentes)\b", t):
            i.periodo_recente = True
        elif i.cnpj and not i.agregacao and not i.produto:
            i.periodo_recente = True   # consulta por posto sem produto: foto mais recente dele
    return i


def usa_sql(i: Intencao) -> bool:
    if i.cnpj:
        return True
    return i.agregacao is not None and any([i.produto, i.municipio, i.bandeira])


# ==========================================
# EXECUCAO SQL (camada gold)
# ==========================================
def _filtros(i: Intencao):
    cond, p = [f"b.estado_sigla = '{UF}'"], {}
    for coluna, chave, valor in [
        ("b.cnpj", "cnpj", i.cnpj), ("c.produto", "produto", i.produto),
        ("b.municipio", "municipio", i.municipio), ("b.bandeira", "bandeira", i.bandeira),
    ]:
        if valor:
            cond.append(f"{coluna} = :{chave}")
            p[chave] = valor
    if i.dt_ini:
        cond.append("a.data_coleta >= :dt_ini")
        p["dt_ini"] = i.dt_ini
    if i.dt_fim:
        cond.append("a.data_coleta <= :dt_fim")
        p["dt_fim"] = i.dt_fim
    return " AND ".join(cond), p


def _descrever(i: Intencao) -> str:
    partes = []
    if i.produto:
        partes.append(f"produto {i.produto}")
    if i.municipio:
        partes.append(f"município {i.municipio}")
    if i.bandeira:
        partes.append(f"bandeira {i.bandeira}")
    if i.cnpj:
        partes.append(f"CNPJ {i.cnpj}")
    return ", ".join(partes) if partes else f"estado {UF}"


def _periodo_txt(i: Intencao, dims: Dimensoes) -> str:
    if i.dt_ini:
        return str(i.dt_ini) if i.dt_ini == i.dt_fim else f"{i.dt_ini} a {i.dt_fim}"
    return f"todo o período disponível ({dims.data_min} a {dims.data_max})"


def _resolver_periodo(conn, i: Intencao) -> Intencao:
    """'mais barato / atual' => janela dos ultimos N dias existentes nos dados filtrados."""
    if i.dt_ini or not i.periodo_recente:
        return i
    where, p = _filtros(i)
    dt_max = _para_date(conn.execute(text(f"SELECT MAX(a.data_coleta) {_FROM} WHERE {where}"), p).scalar())
    if not dt_max:
        return i
    return replace(i, dt_ini=dt_max - timedelta(days=JANELA_RECENTE_DIAS - 1), dt_fim=dt_max)


def _num(v) -> Optional[float]:
    return None if v is None else float(v)


def _sql_count(conn, i, dims):
    where, p = _filtros(i)
    r = conn.execute(text(
        f"SELECT COUNT(DISTINCT b.cnpj), COUNT(*), MIN(a.data_coleta), MAX(a.data_coleta) {_FROM} WHERE {where}"
    ), p).fetchone()
    n_postos, n_reg = int(r[0]), int(r[1])
    chunk = (f"Consulta SQL exata na camada gold ({_descrever(i)}): existem {n_postos} revendas distintas (CNPJs) "
             f"com registro de preço, em {n_reg} registros no período de {_para_date(r[2])} a {_para_date(r[3])}.")
    return [(chunk, {"tipo": "sql_count", "valor": n_postos, "n_registros": n_reg}, 1.0)]


def _sql_stats(conn, i, dims):
    where, p = _filtros(i)
    rows = conn.execute(text(
        f"""SELECT c.produto, COUNT(*), COUNT(DISTINCT b.cnpj), AVG(a.valor_venda), MIN(a.valor_venda),
                   MAX(a.valor_venda), MIN(a.data_coleta), MAX(a.data_coleta)
            {_FROM} WHERE {where} GROUP BY c.produto ORDER BY c.produto"""
    ), p).fetchall()
    out = []
    for r in rows:
        chunk = (f"Consulta SQL exata na camada gold ({_descrever(i)}), produto {r[0]}: preço médio R$ {float(r[3]):.2f}, "
                 f"mínimo R$ {float(r[4]):.2f}, máximo R$ {float(r[5]):.2f}, com base em {r[1]} registros de {r[2]} postos, "
                 f"período de {_para_date(r[6])} a {_para_date(r[7])}.")
        out.append((chunk, {"tipo": "sql_stats", "produto": r[0], "valor": _num(r[3]), "minimo": _num(r[4]),
                            "maximo": _num(r[5]), "n_registros": int(r[1])}, 1.0))
    return out


def _sql_ranking(conn, i, dims, top_k):
    fn, ordem, rotulo = ("MIN", "ASC", "Menor") if i.agregacao == "MIN" else ("MAX", "DESC", "Maior")
    where, p = _filtros(i)
    p["lim"] = top_k
    rows = conn.execute(text(
        f"""SELECT b.revenda, b.bandeira, b.bairro, b.municipio, b.cnpj, {fn}(a.valor_venda) AS valor
            {_FROM} WHERE {where}
            GROUP BY b.revenda, b.bandeira, b.bairro, b.municipio, b.cnpj
            ORDER BY valor {ordem}, b.revenda LIMIT :lim"""
    ), p).fetchall()
    per = _periodo_txt(i, dims)
    out = []
    for pos, r in enumerate(rows, 1):
        chunk = (f"Consulta SQL exata na camada gold ({_descrever(i)}), ranking #{pos}: {rotulo} preço R$ {float(r[5]):.2f} "
                 f"no posto '{r[0]}' (bandeira {r[1]}, bairro {r[2]}, {r[3]}, CNPJ {r[4]}). Período considerado: {per}.")
        out.append((chunk, {"tipo": "sql_ranking", "valor": _num(r[5]), "cnpj": r[4], "municipio": r[3]}, 1.0))
    return out


def _sql_lista(conn, i, dims):
    where, p = _filtros(i)
    total = int(conn.execute(text(f"SELECT COUNT(DISTINCT b.cnpj) {_FROM} WHERE {where}"), p).scalar() or 0)
    rows = conn.execute(text(
        f"""SELECT DISTINCT b.revenda, b.bandeira, b.bairro, b.municipio, b.cnpj
            {_FROM} WHERE {where} ORDER BY b.revenda LIMIT :lim"""
    ), {**p, "lim": LIMITE_LISTA}).fetchall()
    out = [(f"Consulta SQL exata na camada gold ({_descrever(i)}): {total} revendas encontradas "
            f"(período: {_periodo_txt(i, dims)}); listando {len(rows)}.",
            {"tipo": "sql_lista", "valor": total}, 1.0)]
    for r in rows:
        out.append((f"Posto '{r[0]}' (bandeira {r[1]}, bairro {r[2]}, {r[3]}, CNPJ {r[4]}).",
                    {"tipo": "sql_lista_item", "cnpj": r[4], "municipio": r[3]}, 1.0))
    return out


def _sql_detalhe(conn, i, dims):
    where, p = _filtros(i)
    rows = conn.execute(text(
        f"""SELECT b.revenda, b.bandeira, b.municipio, b.estado_sigla, b.bairro, b.cep, b.cnpj,
                   c.produto, a.valor_venda, a.data_coleta
            {_FROM} WHERE {where} ORDER BY a.data_coleta DESC, c.produto LIMIT :lim"""
    ), {**p, "lim": LIMITE_DETALHE}).fetchall()
    out = []
    for r in rows:
        chunk = (f"O posto '{r[0]}' da bandeira {r[1]}, localizado em {r[2]} - {r[3]} (Bairro: {r[4]}, CEP: {r[5]}, "
                 f"CNPJ: {r[6]}), vendeu o produto {r[7]} pelo valor de R$ {float(r[8]):.2f} na data {_para_date(r[9])}.")
        out.append((chunk, {"tipo": "sql_detalhe", "cnpj": r[6], "cep": r[5], "municipio": r[2], "estado_sigla": r[3]}, 1.0))
    if not out:
        logger.warning(f"consulta por posto sem registros na gold: {i}")
    return out


def executar_sql(engine, i: Intencao, dims: Dimensoes, top_k: int = 5):
    """Todas as respostas SQL voltam no mesmo formato do retriever vetorial:
    (chunk_text, metadata, similaridade). similaridade = 1.0 => resultado exato."""
    with engine.connect() as conn:
        i = _resolver_periodo(conn, i)
        if i.agregacao == "COUNT":
            return _sql_count(conn, i, dims)
        if i.agregacao == "STATS" or (i.agregacao in ("MIN", "MAX") and not i.produto):
            return _sql_stats(conn, i, dims)
        if i.agregacao in ("MIN", "MAX"):
            return _sql_ranking(conn, i, dims, top_k)
        if i.agregacao == "LISTA":
            return _sql_lista(conn, i, dims)
        return _sql_detalhe(conn, i, dims)


# ==========================================
# CAMINHO VETORIAL (fallback)
# ==========================================
def buscar_vetorial(pergunta: str, modelo_ia, top_k: int):
    from pipelines.rag.anp.rag02_retriever import buscar_contexto  # import tardio: so carrega quando precisa

    if not DEDUP_VETORIAL:
        return buscar_contexto(pergunta, modelo_ia, top_k)

    # cada chunk e uma observacao (posto, produto, data): os top-k por similaridade tendem a ser
    # o mesmo posto/produto em datas diferentes. Busca mais candidatos e mantem 1 por (posto, produto).
    candidatos = buscar_contexto(pergunta, modelo_ia, top_k * VETORIAL_OVERFETCH)
    vistos, unicos, repetidos = set(), [], []
    for c in candidatos:
        chave = c[0].split(" pelo valor de")[0]
        if chave in vistos:
            repetidos.append(c)
        else:
            vistos.add(chave)
            unicos.append(c)
    return (unicos + repetidos)[:top_k]


# ==========================================
# PONTO UNICO DE ROTEAMENTO
# ==========================================
def recuperar_contexto(pergunta: str, modelo_ia, engine, dims: Dimensoes, top_k: int = 5):
    """Retorna (resultados, metodo, intencao). metodo: sql_detalhe | sql_agregado | vetorial."""
    i = interpretar(pergunta, dims)
    if usa_sql(i):
        metodo = "sql_agregado" if i.agregacao else "sql_detalhe"
        logger.info(f"roteamento -> {metodo} | {i}")
        return executar_sql(engine, i, dims, top_k), metodo, i
    logger.info(f"roteamento -> vetorial | {i}")
    return buscar_vetorial(pergunta, modelo_ia, top_k), "vetorial", i
