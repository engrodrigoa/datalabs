import os
import sys
from warnings import filterwarnings
import psycopg2

from sentence_transformers import SentenceTransformer

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.env_loader import CONSTRING
from pipelines.commons.dw_client import test_pg_connection
from pipelines.commons.logger import get_logger

logger = get_logger("rag02_retriever")

# ==========================================
# TARGETS & CONFIG
# ==========================================
SCHEMA = "ai"
TABELA = "rag_context_anp"
MODELO_NOME = "intfloat/multilingual-e5-base"
TOP_K = 3  

# ==========================================
# BUSCA SEMÂNTICA
# ==========================================
def buscar_contexto(pergunta: str, modelo_ia, top_k: int = 3):
    logger.info(f"Vetorizando a pergunta: '{pergunta}'")
    vetor_pergunta = modelo_ia.encode([f"query: {pergunta}"], normalize_embeddings=True)[0].tolist()
    
    # 2. Conecta ao banco
    conn = psycopg2.connect(CONSTRING)
    cursor = conn.cursor()

    query = f"""
        SELECT 
            chunk_text, 
            metadata,
            1 - (embedding <=> %s::vector) AS similaridade
        FROM {SCHEMA}.{TABELA}
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
    """
    
    cursor.execute(query, (vetor_pergunta, vetor_pergunta, top_k))
    resultados = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return resultados

# ==========================================
# PIPELINE (INTERATIVO)
# ==========================================
def main():
    test_pg_connection()
    
    logger.info(f"Carregando modelo AI: {MODELO_NOME}")
    modelo_ia = SentenceTransformer(MODELO_NOME)
    
    print("\n" + "="*60)
    print("🤖 ASSISTENTE ANP RAG - MODO RETRIEVER (Digite 'sair' para encerrar)")
    print("="*60)
    
    while True:
        pergunta = input("\nFaça uma pergunta sobre os postos ou combustíveis: ")
        
        if pergunta.lower() in ['sair', 'exit', 'quit']:
            logger.info("Encerrando o Retriever.")
            break
            
        if not pergunta.strip():
            continue
            
        resultados = buscar_contexto(pergunta, modelo_ia, TOP_K)
        
        print("\n🔎 TOP 3 RESULTADOS ENCONTRADOS PELO PGVECTOR:")
        print("-" * 60)
        for i, (chunk, metadata, similaridade) in enumerate(resultados, 1):
            # Formata a similaridade em percentual
            score = round(similaridade * 100, 2)
            print(f"[{i}] Score: {score}%")
            print(f"Metadados: {metadata}")
            print(f"Contexto: {chunk}")
            print("-" * 60)

if __name__ == "__main__":
    main()