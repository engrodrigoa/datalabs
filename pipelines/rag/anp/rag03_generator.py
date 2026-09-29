import os
import sys
import requests
from warnings import filterwarnings

from sentence_transformers import SentenceTransformer

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.env_loader import validate_env, CONSTRING
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger

# roteador: SQL exato/agregado na gold quando a pergunta pede, vetorial (pgvector) no resto
from pipelines.commons.ai.rag_query_router import carregar_dimensoes, recuperar_contexto

logger = get_logger("rag03_generator")

# ==========================================
# TARGETS & CONFIG
# ==========================================
MODELO_EMBEDDING = "intfloat/multilingual-e5-base"
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
TOP_K = 5

PROMPT_TEMPLATE = """Você é um assistente que responde perguntas sobre preços de combustíveis em Goiás,
com base EXCLUSIVAMENTE no contexto abaixo, extraído da base da ANP.

Regras:
- Linhas [SQL] são resultados exatos calculados sobre toda a base. Use-as como verdade e não recalcule.
- Linhas [BUSCA] são registros individuais recuperados por similaridade e podem não ser exaustivos.
- Se houver várias datas e a pergunta não especificar uma, use a mais recente e informe a data.
- Se a resposta não puder ser deduzida do contexto, diga claramente que não há dados suficientes.
- Não invente números ou postos que não estejam listados.

Contexto:
{contexto}

Pergunta: {pergunta}

Resposta:"""


# ==========================================
# HELPERS
# ==========================================
def montar_contexto(resultados) -> str:
    linhas = []
    for chunk, metadata, similaridade in resultados:
        if isinstance(metadata, dict) and str(metadata.get("tipo", "")).startswith("sql"):
            linhas.append(f"- [SQL] {chunk}")
        else:
            linhas.append(f"- [BUSCA] {chunk} (similaridade: {round(similaridade * 100, 1)}%)")
    return "\n".join(linhas)


def perguntar_llm(pergunta: str, contexto: str) -> str:
    prompt = PROMPT_TEMPLATE.format(contexto=contexto, pergunta=pergunta)
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
    resp = requests.post(OLLAMA_URL, json=payload, timeout=120)
    resp.raise_for_status()
    return resp.json()["response"].strip()


# ==========================================
# PIPELINE (INTERATIVO)
# ==========================================
def main():
    test_pg_connection()
    engine = get_sqla_engine()
    dims = carregar_dimensoes(engine)

    logger.info(f"Carregando modelo de embeddings: {MODELO_EMBEDDING}")
    modelo_ia = SentenceTransformer(MODELO_EMBEDDING)

    print("\n" + "=" * 60)
    print("🤖 ASSISTENTE ANP RAG + LLM (Digite 'sair' para encerrar)")
    print("=" * 60)

    while True:
        pergunta = input("\nFaça uma pergunta sobre os postos ou combustíveis: ")

        if pergunta.lower() in ["sair", "exit", "quit"]:
            logger.info("Encerrando o assistente.")
            break

        if not pergunta.strip():
            continue

        resultados, metodo, intencao = recuperar_contexto(pergunta, modelo_ia, engine, dims, TOP_K)


        if metodo.startswith("sql") and not resultados:
            print("\n💬 RESPOSTA:")
            print("-" * 60)
            print("Não encontrei registros na base para esses filtros.")
            print("-" * 60)
            continue

        contexto = montar_contexto(resultados)

        logger.info("Gerando resposta com Ollama...")
        try:
            resposta = perguntar_llm(pergunta, contexto)
        except requests.exceptions.RequestException as e:
            logger.error(f"Falha ao chamar o Ollama: {e}")
            print("\n Não consegui falar com o Ollama. Ele está rodando em localhost:11434?")
            continue

        print("\n💬 RESPOSTA:")
        print("-" * 60)
        print(resposta)
        print("-" * 60)
        print(f"\n🔀 Método de recuperação: {metodo}")
        print("\n📎 Fontes usadas:")
        print(contexto)


if __name__ == "__main__":
    main()