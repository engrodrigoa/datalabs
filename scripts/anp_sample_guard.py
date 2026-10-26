"""
Guard of `make sample-data` / `make demo-offline`: refuses to write synthetic ANP files on a database that
already holds REAL ANP loads, unless FORCE=1.

Why: the synthetic weekly file has the real file's name (ultimas-4-semanas-gasolina-etanol.csv) and the
landing replaces a load by its key (file name + weekly reference), so the synthetic rows REPLACE the real
weekly load in bronze. The incremental fact keeps the real keys that left silver, and the stations that only
existed in that load disappear from dm_postos: relationships_ft_anp_combustiveis_id_posto_sk fails and every
downstream gold model is skipped. Real loads are recognised by the scrapers' control tables
(ctrl.anp_metadata_*): the synthetic path never writes them.

    python scripts/anp_sample_guard.py          (exit 0 = safe, 1 = real data present)
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def decide(n_mensal: int, n_semanal: int, force: bool) -> tuple[bool, str]:
    if not (n_mensal or n_semanal):
        return True, "no real ANP load in ctrl.anp_metadata_*: synthetic sample is safe"
    found = f"{n_mensal} monthly / {n_semanal} weekly real ANP load(s) in ctrl.anp_metadata_*"
    if force:
        return True, f"FORCE=1: writing the synthetic sample anyway over {found}"
    return False, (f"{found}. The synthetic files would REPLACE the real weekly load in bronze and leave orphan "
                   "keys in the incremental fact. Use `make demo` / dag_dbt_anp for real data, or FORCE=1 on a "
                   "throwaway database.")


def main() -> int:
    from sqlalchemy import text

    from pipelines.commons.dw_client import get_sqla_engine

    with get_sqla_engine().connect() as conn:
        n_mensal = conn.execute(text("SELECT count(*) FROM ctrl.anp_metadata_mensal")).scalar() or 0
        n_semanal = conn.execute(text("SELECT count(*) FROM ctrl.anp_metadata_semanal")).scalar() or 0
    ok, msg = decide(n_mensal, n_semanal, os.getenv("ANP_SAMPLE_FORCE") == "1")
    print(("OK: " if ok else "REFUSED: ") + msg, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
