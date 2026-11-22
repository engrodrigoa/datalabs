"""cClassTrib legal reference -> LC 214 provisions (rtc03_lc214, ADR 0021): parse, resolve, compare the text."""
from pipelines.rtc import rtc03_lc214 as br

P = "lc214-2025:"
LAW = {
    P + "art-11": {"status": "vigente", "texto": "Art. 11. Considera-se local da operação com:"},
    P + "art-11:inc-VIII": {"status": "vigente",
                            "texto": "VIII - serviço de exploração de via, mediante cobrança de valor;"},
    P + "art-26": {"status": "vigente", "texto": "Art. 26. Não são contribuintes do IBS e da CBS:"},
    P + "art-26:inc-I": {"status": "vigente", "texto": "I - condomínio edilício;"},
    P + "art-26:inc-II": {"status": "revogado", "texto": "II - (Revogado)"},
    P + "art-57": {"status": "vigente", "texto": "Art. 57. Consideram-se de uso pessoal:"},
    P + "art-57:inc-I:ali-a": {"status": "vigente", "texto": "a) joias;"},
    P + "art-57:par-3:inc-IV:ali-a": {"status": "vigente", "texto": "a) outras;"},
}


def _f(fdlg_id, curto, texto=None):
    return {"fdlg_id": fdlg_id, "fdlg_texto_curto": curto, "fdlg_texto": texto}


def test_one_row_per_citation_with_status_and_text_check():
    rows = br.build_bridge([_f(2, "Art. 11, VIII", "Art. 11. Considera-se local da operação com:\\nVIII – serviço "
                                                   "de exploração de via, mediante cobrança de valor;")], LAW)
    assert [(r.outcome, r.dispositivo_id, r.status_lei, r.texto_check) for r in rows] == [
        ("resolved", P + "art-11:inc-VIII", "vigente", "match")]          # literal \n and the long dash: same words


def test_a_list_reference_resolves_every_item_and_flags_a_revoked_one():
    rows = br.build_bridge([_f(9, "Art. 26, I e II", "Art. 26. Não são contribuintes: I - condomínio edilício;")],
                           LAW)
    assert [(r.ordem, r.dispositivo_id, r.status_lei) for r in rows] == [
        (1, P + "art-26:inc-I", "vigente"), (2, P + "art-26:inc-II", "revogado")]
    assert br.summarize(rows)["cites_inactive"] == 1


def test_unparsed_ambiguous_not_found_and_foreign_are_rows_not_errors():
    rows = br.build_bridge([_f(1, "LC 214/2025", "Regra Geral"), _f(3, "alínea a do art. 57"), _f(4, "Art. 900"),
                            _f(5, "Art. 14 do Código Tributário Nacional"), _f(6, None)], LAW)
    got = {r.fdlg_id: (r.outcome, r.candidatos) for r in rows}
    assert got[1] == ("unparsed", None) and got[6] == ("unparsed", None)
    assert got[3] == ("ambiguous", [P + "art-57:inc-I:ali-a", P + "art-57:par-3:inc-IV:ali-a"])
    assert got[4] == ("not_found", None) and got[5][0] == "foreign"
    s = br.summarize(rows)
    assert (s["outcome_resolved"], s["outcome_ambiguous"], s["outcome_not_found"], s["resolved_pct"]) == (0, 1, 1, 0.0)


def test_the_text_check_separates_match_near_diverges_and_no_transcription():
    law = "I - condomínio edilício que exerça atividade econômica;"
    assert br.text_check(law, "Art. 26 ... I – condomínio edilício que exerça atividade econômica;") == ("match", 1.0)
    status, share = br.text_check(law, "I - condomínio edilício que atividade econômica exerça;")
    assert status == "near" and share >= br.NEAR                          # order changed, every word still there
    status, share = br.text_check(law, "I - associações sem fins lucrativos e entidades de classe;")
    assert status == "diverges" and share < br.NEAR                       # another text: amendment or slip
    assert br.text_check(law, "Regra Geral") == ("no_transcription", None)
    assert br.text_check(law, None) == ("no_transcription", None)


def test_words_ignore_accents_case_punctuation_and_both_kinds_of_newline():
    assert br.words("Art. 11.\\nVIII – Serviço\nde exploração") == ["art", "11", "viii", "servico", "de", "exploracao"]


class _Cur:
    """Just enough of a psycopg2 cursor for run(): answers by the first words of the query."""

    def __init__(self, db):
        self.db, self.rows, self.description, self.written = db, [], None, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self.rows = next((v for k, v in self.db.items() if k != "written" and flat.startswith(k)), [])
        self.description = [(c,) for c in ("fdlg_id", "fdlg_texto_curto", "fdlg_texto")]

    def executemany(self, sql, seq):
        self.written = list(seq)
        self.db["written"] = self.written

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Conn:
    def __init__(self, db):
        self.db, self.commits = db, 0

    def cursor(self):
        return _Cur(self.db)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


def _db(fingerprint=(), law_sha="a" * 64):
    return {
        "SELECT sha FROM ctrl.rtc_carga": [("s" * 40,)],
        "SELECT min(source_sha256), count(DISTINCT": [(law_sha, 1 if law_sha else 0)],
        "SELECT DISTINCT rtc_sha, law_sha256,": list(fingerprint),
        "SELECT fdlg_id, fdlg_texto_curto, fdlg_texto": [(2, "Art. 11, VIII", None)],
        "SELECT dispositivo_id, status, texto": [(k, v["status"], v["texto"]) for k, v in LAW.items()],
    }


def test_run_writes_the_bridge_then_skips_while_nothing_changes():
    conn = _Conn(_db())
    assert br.run(conn) == br.EXIT_OK and conn.commits == 1
    written = conn.db["written"]
    assert len(written) == 1 and written[0][5] == P + "art-11:inc-VIII" and written[0][-1] == br.BRIDGE_VERSION
    same = _Conn(_db(fingerprint=[("s" * 40, "a" * 64, br.BRIDGE_VERSION)]))
    assert br.run(same) == br.EXIT_SKIPPED and same.commits == 0
    assert br.run(same, force=True) == br.EXIT_OK


def test_a_new_law_version_or_bridge_version_rebuilds():
    old_law = _Conn(_db(fingerprint=[("s" * 40, "b" * 64, br.BRIDGE_VERSION)]))
    assert br.run(old_law) == br.EXIT_OK
    old_code = _Conn(_db(fingerprint=[("s" * 40, "a" * 64, "0")]))
    assert br.run(old_code) == br.EXIT_OK


def test_no_law_served_is_a_skip_not_a_failure():
    conn = _Conn(_db(law_sha=None))
    assert br.run(conn) == br.EXIT_SKIPPED and "written" not in conn.db
