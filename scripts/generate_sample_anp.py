"""
Synthetic ANP price files with the real layout (offline demo + CI integration test).

    python scripts/generate_sample_anp.py datasource/anp [--today 2026-10-05] [--drifted]

Writes the weekly file (last 4 weeks) and two closed monthly files. --drifted also writes a
file with missing columns, to exercise the schema-drift detection of the landing step.
Real data: the dag_dbt_anp scrapers download it from gov.br/anp.
"""
import argparse
import datetime as dt
import random
from pathlib import Path

UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
       "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
PRODUCTS = {"GASOLINA": 6.2, "ETANOL": 4.3, "DIESEL S10": 6.0, "GNV": 4.9}
BRANDS = ["IPIRANGA", "RAIZEN", "VIBRA ENERGIA", "BRANCA"]
CITIES_GO = ["GOIANIA", "ANAPOLIS", "APARECIDA DE GOIANIA", "RIO VERDE"]
HEADER = ("Regiao - Sigla;Estado - Sigla;Municipio;Revenda;CNPJ da Revenda;Nome da Rua;Numero Rua;Complemento;"
          "Bairro;Cep;Produto;Data da Coleta;Valor de Venda;Valor de Compra;Unidade de Medida;Bandeira")


def stations(rng: random.Random):
    out = []
    for uf in UFS:
        for i in range(40 if uf == "GO" else 25):
            cnpj = f"{rng.randint(10, 99)}.{rng.randint(100, 999)}.{rng.randint(100, 999)}/0001-{rng.randint(10, 99)}"
            city = rng.choice(CITIES_GO) if uf == "GO" else rng.choice([f"CAPITAL {uf}", f"INTERIOR {uf}"])
            out.append((uf, city, f"POSTO {uf} {i} LTDA", cnpj, rng.choice(BRANDS)))
    # a fixed station used by the RAG golden set
    out.append(("GO", "ANAPOLIS", "POSTO SAO FRANCISCO LTDA", "25.080.920/0001-98", "IPIRANGA"))
    return out


def write(path: Path, start: dt.date, days: int, rng: random.Random, sts) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with path.open("w", encoding="utf-8") as f:
        f.write(HEADER + "\n")
        for d in range(days):
            day = start + dt.timedelta(days=d)
            if day.weekday() > 4:
                continue
            for uf, city, name, cnpj, brand in sts:
                for product, base in PRODUCTS.items():
                    if rng.random() < 0.35:
                        continue
                    price = f"{base * rng.uniform(0.9, 1.1):.2f}".replace(".", ",")
                    region = "CO" if uf in ("GO", "DF", "MT", "MS") else "NE"
                    f.write(f"{region};{uf};{city};{name};{cnpj};RUA A;100;;CENTRO;75000-000;{product};"
                            f"{day:%d/%m/%Y};{price};;R$ / litro;{brand}\n")
                    rows += 1
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="e.g. datasource/anp")
    ap.add_argument("--today", default=dt.date.today().isoformat())
    ap.add_argument("--drifted", action="store_true")
    args = ap.parse_args()

    rng = random.Random(7)
    sts = stations(rng)
    today = dt.date.fromisoformat(args.today)
    base = Path(args.target)
    first_of_month = today.replace(day=1)
    prev_month = (first_of_month - dt.timedelta(days=1)).replace(day=1)
    prev_prev = (prev_month - dt.timedelta(days=1)).replace(day=1)

    n = write(base / "ult4" / "ultimas-4-semanas-gasolina-etanol.csv", today - dt.timedelta(days=28), 28, rng, sts)
    for m in (prev_prev, prev_month):
        days = ((m.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - m).days
        n += write(base / "arquivos_fechados" / "combustivel" / "mes" / f"ca-{m:%Y-%m}.csv", m, days, rng, sts)
    if args.drifted:
        (base / "ult4" / "ultimas-4-semanas-glp.csv").write_text("Regiao - Sigla;Estado - Sigla;Produto\nCO;GO;GLP\n")
    print(f"{n:,} rows written under {base}")


if __name__ == "__main__":
    main()
