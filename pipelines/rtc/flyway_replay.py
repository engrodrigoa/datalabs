"""Rebuild the Receita Federal calculator database (SQLite) from its Flyway scripts, without Java.

The calculator (github.com/ReceitaFederal/calculadora-rtc-regime-regular, MIT) ships its reference tables as
Flyway migrations for SQLite: one baseline (B0001, ~2 MB of INSERTs) plus one V<n> script per change of the
official tables ("nova tabela cClassTrib 15/04/2026", "ajustes de vigência"...). The official build is
`mvnw flyway:migrate`; this module replays the same scripts with Python's sqlite3, following the two Flyway
behaviours the scripts depend on (measured on the 1.6.1 release, 2026-11-17: 61 scripts, 0.7 s, 0 FK violations):

    1. each V<n> runs in its own transaction: several scripts use `PRAGMA defer_foreign_keys = ON`, which only
       works inside a transaction (without it V0011 fails with "FOREIGN KEY constraint failed");
    2. a script that contains `PRAGMA foreign_keys` runs OUTSIDE a transaction (flyway.mixed=true): the pragma is a
       no-op inside one (without it V0044 fails with "Cannot add a REFERENCES column with non-NULL default").

beforeMigrate runs first, then B0001 (both outside a transaction, they toggle PRAGMA foreign_keys), the V<n>
scripts in numeric order (V10 after V9, not after V1), and afterMigrate last.

Pure functions only: no network, no lake. rtc01_fetch.py does the side effects.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path

_V_RE = re.compile(r"^V(\d+)__.+\.sql$", re.IGNORECASE)
_FK_PRAGMA_RE = re.compile(r"pragma\s+foreign_keys", re.IGNORECASE)

# What the pipeline needs from the calculator database, and the floor below which a release is not trusted.
# Floors are ~70% of what the 1.6.1 release has (measured 2026-11-17), so a legitimate cleanup passes and a
# truncated or emptied table does not.
CONTRACT_MIN_ROWS = {
    "SITUACAO_TRIBUTARIA": 15,           # 25
    "TRIBUTO": 4,                        # 4: IS, CBS, IBSUF, IBSMun
    "TRIBUTO_SITUACAO_TRIBUTARIA": 40,   # 61
    "CLASSIFICACAO_TRIBUTARIA": 130,     # 193
    "FUNDAMENTACAO_LEGAL": 130,          # 193
    "FUNDAMENTACAO_CLASSIFICACAO": 130,  # 193
    "ANEXO": 500,                        # 821
    "NCM_APLICAVEL": 900,                # 1,372
    "EXCECAO_NCM_APLICAVEL": 0,          # 76 (may legitimately be empty)
    "NBS_APLICAVEL": 0,                  # 118
    "EXCECAO_NBS_APLICAVEL": 0,          # 0
    "TIPO_DFE": 10,                      # 18
    "TIPO_DFE_CLASSIFICACAO": 250,       # 405
    "VERSAO_BASE_DADO": 1,               # 59
}
EXPORT_TABLES = tuple(CONTRACT_MIN_ROWS)


class ReplayError(Exception):
    """A script that failed, with its name (the event says which migration broke)."""

    def __init__(self, script: str, cause: Exception):
        super().__init__(f"{script}: {type(cause).__name__}: {cause}")
        self.script = script


def migration_number(name: str) -> int | None:
    m = _V_RE.match(name)
    return int(m.group(1)) if m else None


def ordered_scripts(names: Iterable[str]) -> list[str]:
    """beforeMigrate, B0001*, V<n> by number, afterMigrate. Anything else is ignored (README, .conf)."""
    names = list(names)
    before = [n for n in names if n.lower() == "beforemigrate.sql"]
    baseline = sorted(n for n in names if re.match(r"^B\d+__.+\.sql$", n, re.IGNORECASE))
    versioned = sorted((n for n in names if migration_number(n) is not None), key=migration_number)
    after = [n for n in names if n.lower() == "aftermigrate.sql"]
    return before + baseline + versioned + after


def runs_in_transaction(name: str, sql: str) -> bool:
    if migration_number(name) is None:          # callbacks and the baseline
        return False
    return not _FK_PRAGMA_RE.search(sql)


def replay(scripts: dict[str, str], db_path: str | Path) -> int:
    """Apply `scripts` ({file name: SQL}) to a NEW SQLite file. Returns how many scripts ran."""
    db_path = Path(db_path)
    db_path.unlink(missing_ok=True)
    con = sqlite3.connect(db_path)
    con.isolation_level = None                  # we decide where transactions start and end
    try:
        order = ordered_scripts(scripts)
        for name in order:
            sql = scripts[name]
            try:
                if runs_in_transaction(name, sql):
                    con.executescript("BEGIN;\n" + sql + "\nCOMMIT;")
                else:
                    con.executescript(sql)
            except sqlite3.Error as exc:
                if con.in_transaction:
                    con.execute("ROLLBACK")
                raise ReplayError(name, exc) from exc
        return len(order)
    finally:
        con.close()


def read_scripts(sql_dir: str | Path) -> dict[str, str]:
    """All *.sql under flyway/sql (criacao/ and manutencao/), keyed by file name (names are unique in the repo)."""
    out: dict[str, str] = {}
    for p in sorted(Path(sql_dir).rglob("*.sql")):
        if p.name in out:
            raise ValueError(f"duplicate script name {p.name!r}: {p}")
        out[p.name] = p.read_text(encoding="utf-8")
    return out


def table_counts(con: sqlite3.Connection, tables: Iterable[str] = EXPORT_TABLES) -> dict[str, int | None]:
    existing = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    return {t: (con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] if t in existing else None) for t in tables}


def contract_violations(con: sqlite3.Connection) -> list[str]:
    """Everything wrong with a rebuilt database, not only the first problem."""
    problems = []
    for table, n in table_counts(con, CONTRACT_MIN_ROWS).items():
        floor = CONTRACT_MIN_ROWS[table]
        if n is None:
            problems.append(f"table_missing: {table}")
        elif n < floor:
            problems.append(f"too_few_rows: {table} {n} < {floor}")
    fk = con.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        problems.append(f"foreign_key_violations: {len(fk)}")
    return problems


def overlapping_vigencias(con: sqlite3.Connection) -> list[tuple]:
    """cClassTrib rows that overlap in time for the same tax group (IBS/CBS vs IS) and code.

    The same code exists once for IBS/CBS and once for IS (e.g. 000001), so the natural key is
    (tax group, cClassTrib, start of validity), never the code alone. Overlaps inside one group would make a
    point-in-time join ambiguous: reported (event), not fatal, because the source is authoritative.
    """
    return con.execute("""
        WITH c AS (
            SELECT ct.CLTR_ID, ct.CLTR_CD, ct.CLTR_INICIO_VIGENCIA AS ini, COALESCE(ct.CLTR_FIM_VIGENCIA, '9999-12-31') AS fim,
                   CASE WHEN EXISTS (SELECT 1 FROM TRIBUTO_SITUACAO_TRIBUTARIA ts JOIN TRIBUTO t ON t.TBTO_ID = ts.TRST_TBTO_ID
                                     WHERE ts.TRST_SITR_ID = ct.CLTR_SITR_ID AND t.TBTO_SIGLA = 'IS')
                        THEN 'IS' ELSE 'IBSCBS' END AS grupo
            FROM CLASSIFICACAO_TRIBUTARIA ct)
        SELECT a.grupo, a.CLTR_CD, a.CLTR_ID, b.CLTR_ID
        FROM c a JOIN c b ON a.grupo = b.grupo AND a.CLTR_CD = b.CLTR_CD AND a.CLTR_ID < b.CLTR_ID
                         AND a.ini <= b.fim AND b.ini <= a.fim
        ORDER BY 1, 2""").fetchall()


def database_version(con: sqlite3.Connection) -> str | None:
    """Last migration recorded by the scripts themselves (e.g. 'V0059'), independent of the git sha."""
    row = con.execute("SELECT VRBD_VERSAO_BASE_DADO FROM VERSAO_BASE_DADO ORDER BY VRBD_ID DESC LIMIT 1").fetchone()
    return row[0] if row else None


def release_version(pom_xml: str) -> str | None:
    """The project's own <version> in pom.xml (the one after </parent>, not Spring Boot's)."""
    body = pom_xml.split("</parent>", 1)[-1]
    m = re.search(r"<version>\s*([^<\s]+)\s*</version>", body)
    return m.group(1) if m else None
