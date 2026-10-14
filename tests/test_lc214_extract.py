"""LC 214 source step: structural contract, version detection by sha256, atomic manifest, retries."""
import json
import os

import pytest

from pipelines.rag.lc214 import lc01_extract as lc

GOOD_TEXT = "LEI COMPLEMENTAR Nº 214, DE 16 DE JANEIRO DE 2025\n" + "\f" * 259 + "Art. 544. Esta Lei...\f"


def _fake_extract(text):
    def extract(pdf_path, txt_path):
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
    return extract


def _pdf(tmp_path, name="lc.pdf", body=b"version 1"):
    p = tmp_path / name
    p.write_bytes(b"%PDF-1.4\n" + body)
    return str(p)


# --------------------------------------------------------------------------- contract
def test_contract_accepts_a_real_looking_law():
    assert lc.contract_violations(b"%PDF-1.4", 260, GOOD_TEXT) == []


def test_contract_reports_every_problem_at_once():
    problems = lc.contract_violations(b"<!DOCTYPE html>", 3, "Página não encontrada")
    assert problems[0] == "not_a_pdf"                       # HTML error page served with 200
    assert any(p.startswith("too_few_pages") for p in problems)
    assert sum(p.startswith("marker_missing") for p in problems) == 2


def test_page_count_comes_from_form_feeds():
    assert lc.count_pages(GOOD_TEXT) == 260


# --------------------------------------------------------------------------- versions
def test_same_file_twice_is_skipped(tmp_path):
    out, src = str(tmp_path / "out"), _pdf(tmp_path)
    assert lc.run(src, out, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_OK
    assert lc.run(src, out, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_SKIPPED
    manifest = lc.load_manifest(out)
    assert len(manifest["versions"]) == 1
    assert not os.path.exists(os.path.join(out, "_incoming.pdf"))   # no leftovers


def test_amended_law_is_kept_next_to_the_previous_version(tmp_path):
    out = str(tmp_path / "out")
    lc.run(_pdf(tmp_path, "v1.pdf", b"v1"), out, extract=_fake_extract(GOOD_TEXT))
    first = lc.load_manifest(out)["current"]
    assert lc.run(_pdf(tmp_path, "v2.pdf", b"v2"), out, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_OK
    manifest = lc.load_manifest(out)
    assert manifest["previous"] == first and manifest["current"] != first
    assert len(manifest["versions"]) == 2
    assert len(os.listdir(os.path.join(out, "raw"))) == 2            # history preserved, nothing overwritten


def test_rejected_file_never_touches_the_manifest(tmp_path):
    out = str(tmp_path / "out")
    lc.run(_pdf(tmp_path, "good.pdf", b"good"), out, extract=_fake_extract(GOOD_TEXT))
    before = lc.load_manifest(out)
    truncated = _fake_extract("LEI COMPLEMENTAR Nº 214\f\f")           # download cut half-way
    assert lc.run(_pdf(tmp_path, "bad.pdf", b"bad"), out, extract=truncated) == lc.EXIT_FAILED
    assert lc.load_manifest(out) == before
    assert len(os.listdir(os.path.join(out, "rejected"))) == 1        # kept for inspection


def test_html_error_page_is_rejected_before_extraction(tmp_path):
    """Found on the real run: pdftotext crashed on an HTML page before the contract was checked."""
    out = str(tmp_path / "out")
    page = tmp_path / "erro.pdf"
    page.write_bytes(b"<!DOCTYPE html><html>404</html>")

    def must_not_run(pdf, txt):
        raise AssertionError("extractor called on a file that is not a PDF")

    assert lc.run(str(page), out, extract=must_not_run) == lc.EXIT_FAILED
    assert not os.path.exists(os.path.join(out, "_incoming.pdf"))
    assert lc.load_manifest(out) == {"current": None, "versions": []}


def test_manifest_is_valid_json_with_relative_paths(tmp_path):
    out = str(tmp_path / "out")
    lc.run(_pdf(tmp_path), out, extract=_fake_extract(GOOD_TEXT))
    with open(os.path.join(out, "manifest.json"), encoding="utf-8") as f:
        v = json.load(f)["versions"][0]
    assert v["pdf"].startswith("raw/") and v["text"].startswith("text/")   # survives moving the folder


# --------------------------------------------------------------------------- download
class _Resp:
    def __init__(self, status, body=b""):
        self.status_code, self.body = status, body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, size):
        yield self.body


class _Session:
    def __init__(self, answers):
        self.answers, self.calls = list(answers), 0

    def get(self, url, **kw):
        self.calls += 1
        a = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(a, Exception):
            raise a
        return a


def test_download_retries_transient_errors(tmp_path):
    s = _Session([ConnectionError("reset"), _Resp(503), _Resp(200, b"%PDF-1.4 ok")])
    dest = str(tmp_path / "x.pdf")
    assert lc.download("u", dest, session=s, retries=4, sleep=lambda _: None) == len(b"%PDF-1.4 ok")
    assert s.calls == 3 and not os.path.exists(dest + ".part")


def test_download_gives_up_at_once_on_404(tmp_path):
    s = _Session([_Resp(404)])
    with pytest.raises(lc.SourceError) as e:
        lc.download("u", str(tmp_path / "x.pdf"), session=s, retries=4, sleep=lambda _: None)
    assert e.value.event == "source_unreachable" and s.calls == 1
