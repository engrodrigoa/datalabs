"""LC 214 source step: structural contract, version detection by sha256, atomic manifest, retries."""
import os

import pytest

from pipelines.rag.lc214 import lc01_extract as lc
from pipelines.rag.lc214.lake import LocalLake, k_manifest, k_raw, k_rejected, k_text

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
def _lake(tmp_path):
    return LocalLake(str(tmp_path / "lake"))


def _manifest(lake):
    return lake.get_json(k_manifest(), default=lc.empty_manifest())


def test_same_file_twice_is_skipped(tmp_path):
    lake, src = _lake(tmp_path), _pdf(tmp_path)
    assert lc.run(src, lake, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_OK
    assert lc.run(src, lake, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_SKIPPED
    assert len(_manifest(lake)["versions"]) == 1


def test_amended_law_is_kept_next_to_the_previous_version(tmp_path):
    lake = _lake(tmp_path)
    lc.run(_pdf(tmp_path, "v1.pdf", b"v1"), lake, extract=_fake_extract(GOOD_TEXT))
    first = _manifest(lake)["current"]
    assert lc.run(_pdf(tmp_path, "v2.pdf", b"v2"), lake, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_OK
    manifest = _manifest(lake)
    assert manifest["previous"] == first and manifest["current"] != first
    assert len(manifest["versions"]) == 2
    assert lake.exists(k_raw(first)) and lake.exists(k_raw(manifest["current"]))   # history kept


def test_landing_and_bronze_get_the_pdf_and_its_text(tmp_path):
    lake = _lake(tmp_path)
    lc.run(_pdf(tmp_path), lake, extract=_fake_extract(GOOD_TEXT))
    v = _manifest(lake)["versions"][0]
    assert v["pdf"] == k_raw(v["sha256"]) and v["pdf"].startswith("landing/lc214/raw/")
    assert v["text"] == k_text(v["sha256"]) and v["text"].startswith("bronze/lc214/text/")
    assert lake.get(v["text"]).decode("utf-8") == GOOD_TEXT
    assert lake.get(v["pdf"]).startswith(b"%PDF")


def test_rejected_file_never_touches_the_manifest(tmp_path):
    lake = _lake(tmp_path)
    lc.run(_pdf(tmp_path, "good.pdf", b"good"), lake, extract=_fake_extract(GOOD_TEXT))
    before = _manifest(lake)
    truncated = _fake_extract("LEI COMPLEMENTAR Nº 214\f\f")           # download cut half-way
    bad = _pdf(tmp_path, "bad.pdf", b"bad")
    assert lc.run(bad, lake, extract=truncated) == lc.EXIT_FAILED
    assert _manifest(lake) == before
    assert lake.exists(k_rejected(lc.sha256_file(bad)))                 # kept for inspection


def test_html_error_page_is_rejected_before_extraction(tmp_path):
    """Found on the real run: pdftotext crashed on an HTML page before the contract was checked."""
    lake = _lake(tmp_path)
    page = tmp_path / "erro.pdf"
    page.write_bytes(b"<!DOCTYPE html><html>404</html>")

    def must_not_run(pdf, txt):
        raise AssertionError("extractor called on a file that is not a PDF")

    assert lc.run(str(page), lake, extract=must_not_run) == lc.EXIT_FAILED
    assert not lake.exists(k_manifest())


def test_a_run_killed_before_the_manifest_leaves_no_dangling_current(tmp_path):
    """The manifest is the commit point: written after the data, so 'current' always points to real objects."""
    class DiesOnManifest(LocalLake):
        def put(self, key, data, content_type="application/octet-stream"):
            if key == k_manifest():
                raise KeyboardInterrupt("killed")
            super().put(key, data, content_type)

    lake = DiesOnManifest(str(tmp_path / "lake"))
    src = _pdf(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        lc.run(src, lake, extract=_fake_extract(GOOD_TEXT))
    assert not lake.exists(k_manifest())                                # nothing was committed
    healthy = LocalLake(lake.root)                                      # the next run completes the version
    assert lc.run(src, healthy, extract=_fake_extract(GOOD_TEXT)) == lc.EXIT_OK
    assert _manifest(healthy)["current"] == lc.sha256_file(src)


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
