from datetime import datetime, timezone
import polars as pl

def adicionar_colunas_auditoria(df: pl.DataFrame, nome_arq: str) -> pl.DataFrame:
    exprs = [
        pl.lit(nome_arq).alias("_source_file"),
        pl.lit(datetime.now(timezone.utc)).alias("_inserted_at")
    ]
    
    if "referencia_mes" in df.columns:
        exprs.append(
            pl.col("referencia_mes").str.replace("-", "").cast(pl.Int32)
        )
        
    return df.with_columns(exprs)

def validar_schema_basico(df: pl.DataFrame, colunas_esperadas: dict[str, pl.DataType]) -> None:
    for nome, tipo in colunas_esperadas.items():
        if nome not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {nome}")
        if df.schema[nome] != tipo:
            raise ValueError(f"Coluna {nome}: esperado {tipo}, obtido {df.schema[nome]}")