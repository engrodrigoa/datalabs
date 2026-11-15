"""ANP assistant (rag03): every Ollama call records its usage (ADR 0018); the answer never depends on the write."""
from pipelines.rag.anp import rag03_generator as rag03


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def test_the_answer_is_returned_and_its_tokens_recorded(monkeypatch):
    sent, recorded = [], []
    body = {"model": "llama3.2:1b", "response": " O menor preço é R$ 5,89. ", "done_reason": "stop",
            "prompt_eval_count": 512, "eval_count": 18}
    monkeypatch.setattr(rag03.requests, "post", lambda url, json, timeout: sent.append(json) or FakeResponse(body))
    monkeypatch.setattr(rag03, "record_llm_call", recorded.append)

    assert rag03.perguntar_llm("menor preço em Goiânia?", "- [SQL] ...", "sql_agregado") == "O menor preço é R$ 5,89."
    (c,) = recorded
    assert (c.app, c.route, c.token_method, c.prompt_tokens, c.completion_tokens) == \
        ("anp_assistant", "sql_agregado", "reported", 512, 18)
    assert sent[0]["prompt"] not in str(c.row())
