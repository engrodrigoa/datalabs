"""RFB download against a flaky source: contract of 37 files, retries, resume, explicit causes."""
import io
import os
import zipfile

import pytest

from pipelines.rfb import rfb01_download_files as rfb


def _zip_bytes(name="x.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, "a;b\n1;2\n")
    return buf.getvalue()


ZIP = _zip_bytes()


class _Resp:
    def __init__(self, status, body=b"", ctype="application/zip", length=None):
        self.status_code = status
        self.body = body
        self.headers = {"Content-Type": ctype}
        if status == 200 and body:
            self.headers["Content-Length"] = str(len(body) if length is None else length)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, chunk_size):
        yield self.body


class FakeSession:
    """`script` maps a URL suffix to a list of answers consumed in order (the last one repeats)."""

    def __init__(self, script):
        self.script = script
        self.calls = []

    def _answer(self, method, url):
        self.calls.append((method, url))
        for suffix, answers in self.script.items():
            if url.endswith(suffix):
                return answers.pop(0) if len(answers) > 1 else answers[0]
        return _Resp(200, ZIP)

    def request(self, method, url, **kw):
        return self._answer(method, url)

    def get(self, url, **kw):
        return self._answer("GET", url)


def _fetcher(script, retries=4):
    return rfb.Fetcher("tok", session=FakeSession(script), retries=retries, pause_s=0, sleep=lambda s: None)


def test_contract_has_the_37_monthly_files():
    assert len(rfb.EXPECTED_FILES) == 37
    assert "Estabelecimentos9.zip" in rfb.EXPECTED_FILES and "Qualificacoes.zip" in rfb.EXPECTED_FILES


def test_share_token_precedence(monkeypatch):
    monkeypatch.delenv("RFB_SHARE_URL", raising=False)
    monkeypatch.delenv("RFB_SHARE_TOKEN", raising=False)
    assert rfb.share_token() == ("gn672Ad4CF8N6TK", "default link")
    monkeypatch.setenv("RFB_SHARE_TOKEN", "OLDTOKEN")
    assert rfb.share_token() == ("OLDTOKEN", "RFB_SHARE_TOKEN")
    monkeypatch.setenv("RFB_SHARE_URL", "https://arquivos.receitafederal.gov.br/index.php/s/NEW123?dir=/Dados")
    assert rfb.share_token() == ("NEW123", "RFB_SHARE_URL")   # the browser link wins over a stale token
    monkeypatch.setenv("RFB_SHARE_URL", "https://example.org/no-token")
    with pytest.raises(rfb.SourceError):
        rfb.share_token()


def test_flaky_404_then_200_is_downloaded(tmp_path):
    # the source answered 404 and then 200 for the same file within minutes
    f = _fetcher({"/2026-08": [_Resp(200)], "Empresas0.zip": [_Resp(404), _Resp(404), _Resp(200, ZIP)]})
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f) == 0
    assert len(os.listdir(tmp_path / "ref202608")) == 37


def test_file_that_never_comes_fails_with_its_status_and_rerun_resumes(tmp_path):
    f = _fetcher({"/2026-08": [_Resp(200)], "Socios3.zip": [_Resp(404)]})
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f) == 1
    assert "Socios3.zip" not in os.listdir(tmp_path / "ref202608")
    assert not [n for n in os.listdir(tmp_path / "ref202608") if n.endswith(".part")]

    f2 = _fetcher({"/2026-08": [_Resp(200)]})
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f2) == 0
    gets = [u for m, u in f2.session.calls if m == "GET"]
    assert len(gets) == 1 and gets[0].endswith("Socios3.zip")   # only the missing file is fetched again


def test_truncated_local_zip_is_downloaded_again(tmp_path):
    d = tmp_path / "ref202608"
    d.mkdir()
    for name in rfb.EXPECTED_FILES:
        (d / name).write_bytes(ZIP)
    (d / "Empresas5.zip").write_bytes(ZIP[:20])                    # killed mid-download by an old run
    f = _fetcher({"/2026-08": [_Resp(200)]})
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f) == 0
    assert [u for m, u in f.session.calls if m == "GET"][0].endswith("Empresas5.zip")
    assert rfb.is_valid_zip(str(d / "Empresas5.zip"))


def test_short_body_is_not_accepted(tmp_path):
    f = _fetcher({"Cnaes.zip": [_Resp(200, ZIP, length=len(ZIP) + 100)]}, retries=2)
    ok, status, reason = f.download("https://h/Cnaes.zip", str(tmp_path / "Cnaes.zip"))
    assert not ok and "truncated" in reason and not os.path.exists(tmp_path / "Cnaes.zip.part")


def test_html_answer_is_not_a_file(tmp_path):
    f = _fetcher({"Cnaes.zip": [_Resp(200, b"<html>login</html>", ctype="text/html")]}, retries=2)
    ok, _, reason = f.download("https://h/Cnaes.zip", str(tmp_path / "Cnaes.zip"))
    assert not ok and "html" in reason


def test_rejected_link_is_reported_as_auth_failure(tmp_path):
    f = _fetcher({"/2026-08": [_Resp(401)]})
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f) == 1
    assert not [m for m, _ in f.session.calls if m == "GET"]             # fails fast, no downloads attempted


def test_month_not_published_is_a_skip(tmp_path):
    f = _fetcher({"/2026-09": [_Resp(404)], "/CNPJ": [_Resp(200)]})
    assert rfb.download_month("tok", "2026-09", str(tmp_path), fetcher=f) == rfb.NOT_PUBLISHED


def test_wrong_share_dir_is_an_error(tmp_path):
    f = _fetcher({"/2026-08": [_Resp(404)], "/CNPJ": [_Resp(404)]})
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f) == 1


def test_complete_month_on_disk_does_not_touch_the_source(tmp_path):
    d = tmp_path / "ref202608"
    d.mkdir()
    for name in rfb.EXPECTED_FILES:
        (d / name).write_bytes(ZIP)
    f = _fetcher({"/2026-08": [_Resp(503)]})                            # source down
    assert rfb.download_month("tok", "2026-08", str(tmp_path), fetcher=f) == 0
    assert f.session.calls == []
