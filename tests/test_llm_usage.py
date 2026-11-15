"""LLM usage per call (ADR 0018): reported vs estimated tokens, prompt kept as hash only, best-effort write.

The database test writes one row to ai.llm_call and deletes it: pointing CHECKS_TEST_DSN at a lab database leaves
it as it was.
"""
import hashlib
import os
from pathlib import Path

import pytest

from pipelines.commons.ai import llm_usage as lu

DSN = os.getenv("CHECKS_TEST_DSN")
needs_db = pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres")
DDL = Path(__file__).resolve().parents[1] / "infra/postgres/init/sql/07_llm_usage.sql"

OLLAMA_RESP = {"model": "qwen2.5:3b", "response": "{}", "done": True, "done_reason": "stop",
               "total_duration": 9_000_000_000, "load_duration": 1_500_000_000,
               "prompt_eval_count": 1830, "prompt_eval_duration": 6_000_000_000,
               "eval_count": 120, "eval_duration": 1_400_000_000}


# --------------------------------------------------------------------------- no database
def test_reported_tokens_win_over_the_estimate():
    c = lu.from_ollama(OLLAMA_RESP, app="lc214_generate", model="qwen2.5:3b", prompt="uma pergunta " * 500,
                       system="regras", latency_s=9.2, num_ctx=6144, route="vector")
    assert c.token_method == "reported"
    assert (c.prompt_tokens, c.completion_tokens, c.total_tokens) == (1830, 120, 1950)
    assert c.prompt_tokens_estimate == lu.estimate_tokens("regras\n" + "uma pergunta " * 500)
    assert (c.latency_ms, c.load_ms, c.prompt_eval_ms, c.eval_ms) == (9200, 1500, 6000, 1400)
    assert (c.done_reason, c.num_ctx, c.route) == ("stop", 6144, "vector")


def test_a_response_without_usage_falls_back_to_the_estimate():
    c = lu.from_ollama({"response": "ok"}, app="anp_assistant", model="llama3.2:1b", prompt="a b c d")
    assert c.token_method == "estimated"
    assert c.prompt_tokens_reported is None and c.completion_tokens is None
    assert c.prompt_tokens == c.prompt_tokens_estimate == lu.estimate_tokens("a b c d") == 7   # 4 words x 1.7
    assert c.total_tokens == c.prompt_tokens and c.model == "llama3.2:1b"


@pytest.mark.parametrize("bad", [None, -1, True, "1830"])
def test_counts_that_are_not_counts_are_ignored(bad):
    c = lu.from_ollama({"prompt_eval_count": bad, "eval_count": bad}, app="x", model="m", prompt="p")
    assert c.token_method == "estimated" and c.completion_tokens is None


def test_the_prompt_is_kept_as_a_hash_and_a_size_never_as_text():
    secret = "CPF 000.000.000-00 da empresa X"
    row = lu.from_ollama(OLLAMA_RESP, app="x", model="m", prompt=secret, system="sys").row()
    assert secret not in str(row)
    assert row["prompt_sha256"] == hashlib.sha256(f"sys\n{secret}".encode()).hexdigest()
    assert (row["prompt_chars"], row["prompt_words"]) == (len(f"sys\n{secret}"), 6)
    assert row["token_method"] == "reported" and row["total_tokens"] == 1950


def test_the_same_prompt_has_the_same_hash():
    a = lu.prompt_facts("pergunta", "sys")
    assert a == lu.prompt_facts("pergunta", "sys") and a != lu.prompt_facts("pergunta", "outro sys")


def test_recording_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("LLM_USAGE_RECORD", "0")
    monkeypatch.setattr(lu, "_connect", lambda: pytest.fail("must not connect when recording is off"))
    assert lu.record_llm_call(lu.from_ollama(OLLAMA_RESP, app="x", model="m", prompt="p")) is False


def test_a_failed_write_never_raises(monkeypatch):
    monkeypatch.setenv("LLM_USAGE_RECORD", "1")
    events = []
    monkeypatch.setattr(lu, "log_event", lambda _l, event, msg, **kw: events.append(event))

    def boom():
        raise OSError("database down")

    monkeypatch.setattr(lu, "_connect", boom)
    assert lu.record_llm_call(lu.from_ollama(OLLAMA_RESP, app="x", model="m", prompt="p")) is False
    assert events == ["llm_usage_write_failed"]


def test_ad_hoc_calls_carry_no_step():
    assert lu._run_context() == {"pipeline": None, "step": None, "step_run_id": None, "dag_id": None,
                                 "dag_run_id": None}


# --------------------------------------------------------------------------- database
@needs_db
def test_a_call_lands_in_the_table_and_the_daily_view(monkeypatch):
    import psycopg2

    monkeypatch.setenv("LLM_USAGE_RECORD", "1")
    monkeypatch.setattr(lu, "_connect", lambda: psycopg2.connect(DSN))
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ai")
        cur.execute(DDL.read_text())
    call = lu.from_ollama(OLLAMA_RESP, app="test_llm_usage", model="qwen2.5:3b", prompt="p " * 10,
                          tags={"variant": "answer_default"})
    try:
        assert lu.record_llm_call(call) is True
        assert lu.record_llm_call(call) is True                        # the same call twice: one row
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), max(prompt_tokens), max(token_method), max(tags->>'variant') "
                        "FROM ai.llm_call WHERE call_id = %s", (call.call_id,))
            assert cur.fetchone() == (1, 1830, "reported", "answer_default")
            cur.execute("SELECT calls, total_tokens, reported_share FROM ai.vw_llm_usage_daily "
                        "WHERE app = 'test_llm_usage'")
            calls, total, share = cur.fetchone()
            assert (calls, total, float(share)) == (1, 1950, 1.0)
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ai.llm_call WHERE app = 'test_llm_usage'")
        conn.close()
