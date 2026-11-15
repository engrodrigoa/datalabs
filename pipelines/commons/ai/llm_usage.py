"""LLM usage per call -> ai.llm_call (ADR 0018).

One row per request a consumer sends to the local LLM, with the tokens it used. The number comes from the LLM's own
response when it carries it (Ollama: `prompt_eval_count` / `eval_count`, token_method `reported`); otherwise it is
the words x tokens-per-word estimate (token_method `estimated`). The estimate is stored on every row, so the
retriever's budget can be checked against the reported count.

Only the sha256 and the size of the prompt are kept, never its text or the reply: usage is metadata, not content.

Best-effort, like every audit write (ADR 0001): a failed insert logs `llm_usage_write_failed` and returns False; the
caller's answer is never lost because the metric could not be written. `LLM_USAGE_RECORD=0` turns it off (tests).

Usage in a consumer:

    from pipelines.commons.ai.llm_usage import from_ollama, record_llm_call
    t0 = time.monotonic()
    resp = requests.post(OLLAMA_URL, json=payload).json()
    record_llm_call(from_ollama(resp, app="anp_assistant", model=MODEL, prompt=prompt,
                                latency_s=time.monotonic() - t0))
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from pipelines.commons.logger import get_logger, log_event

logger = get_logger("llm_usage")

# The same default as the LC 214 retriever's budget (words x 1.7, ADR 0013); measured 1.79 (llama) / 1.83 (qwen).
TOKENS_PER_WORD = float(os.getenv("LLM_TOKENS_PER_WORD", os.getenv("LC214_LLM_TOKENS_PER_WORD", "1.7")))


def recording_enabled() -> bool:
    return os.getenv("LLM_USAGE_RECORD", "1").strip().lower() not in ("0", "false", "no", "off")


def estimate_tokens(text: str, per_word: float = TOKENS_PER_WORD) -> int:
    return math.ceil(len(text.split()) * per_word)


def _ms(value_ns) -> int | None:
    """Ollama durations are nanoseconds."""
    return round(value_ns / 1e6) if isinstance(value_ns, (int, float)) and not isinstance(value_ns, bool) else None


def _count(value) -> int | None:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0 else None


@dataclass
class LLMCall:
    app: str
    model: str
    prompt_sha256: str
    prompt_chars: int
    prompt_words: int
    prompt_tokens_estimate: int
    prompt_tokens_reported: int | None = None
    completion_tokens: int | None = None
    provider: str = "ollama"
    model_digest: str | None = None
    route: str | None = None
    num_ctx: int | None = None
    done_reason: str | None = None
    latency_ms: int | None = None
    load_ms: int | None = None
    prompt_eval_ms: int | None = None
    eval_ms: int | None = None
    tags: dict = field(default_factory=dict)
    call_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    called_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def token_method(self) -> str:
        return "reported" if self.prompt_tokens_reported is not None else "estimated"

    @property
    def prompt_tokens(self) -> int:
        return self.prompt_tokens_reported if self.prompt_tokens_reported is not None else self.prompt_tokens_estimate

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + (self.completion_tokens or 0)

    def row(self) -> dict:
        d = asdict(self)
        d.update(prompt_tokens=self.prompt_tokens, total_tokens=self.total_tokens, token_method=self.token_method,
                 tags=json.dumps(self.tags, default=str, ensure_ascii=False))
        return d


def prompt_facts(prompt: str, system: str | None = None) -> dict:
    """What is kept of the prompt: the text as sent (system + prompt), hashed and measured, never stored."""
    text = f"{system}\n{prompt}" if system else prompt
    return {"prompt_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), "prompt_chars": len(text),
            "prompt_words": len(text.split()), "prompt_tokens_estimate": estimate_tokens(text)}


def from_ollama(resp: dict, *, app: str, model: str, prompt: str, system: str | None = None,
                latency_s: float | None = None, model_digest: str | None = None, route: str | None = None,
                num_ctx: int | None = None, tags: dict | None = None) -> LLMCall:
    """An LLMCall from Ollama's /api/generate (or /api/chat) response. A response without the counts (an old server,
    a proxy that drops them) still makes a row, with token_method `estimated`."""
    resp = resp or {}
    return LLMCall(app=app, model=resp.get("model") or model, **prompt_facts(prompt, system),
                   prompt_tokens_reported=_count(resp.get("prompt_eval_count")),
                   completion_tokens=_count(resp.get("eval_count")), model_digest=model_digest, route=route,
                   num_ctx=num_ctx, done_reason=resp.get("done_reason"),
                   latency_ms=round(latency_s * 1000) if latency_s is not None else _ms(resp.get("total_duration")),
                   load_ms=_ms(resp.get("load_duration")), prompt_eval_ms=_ms(resp.get("prompt_eval_duration")),
                   eval_ms=_ms(resp.get("eval_duration")), tags=dict(tags or {}))


INSERT_SQL = """
    INSERT INTO ai.llm_call (
        call_id, called_at, app, pipeline, step, step_run_id, dag_id, dag_run_id, provider, model, model_digest,
        route, prompt_sha256, prompt_chars, prompt_words, prompt_tokens, prompt_tokens_reported,
        prompt_tokens_estimate, completion_tokens, total_tokens, token_method, num_ctx, done_reason,
        latency_ms, load_ms, prompt_eval_ms, eval_ms, tags)
    VALUES (
        %(call_id)s, %(called_at)s, %(app)s, %(pipeline)s, %(step)s, %(step_run_id)s, %(dag_id)s, %(dag_run_id)s,
        %(provider)s, %(model)s, %(model_digest)s, %(route)s, %(prompt_sha256)s, %(prompt_chars)s,
        %(prompt_words)s, %(prompt_tokens)s, %(prompt_tokens_reported)s, %(prompt_tokens_estimate)s,
        %(completion_tokens)s, %(total_tokens)s, %(token_method)s, %(num_ctx)s, %(done_reason)s,
        %(latency_ms)s, %(load_ms)s, %(prompt_eval_ms)s, %(eval_ms)s, %(tags)s::jsonb)
    ON CONFLICT (call_id) DO NOTHING
"""


def _run_context() -> dict:
    """The runner's step (pipeline, step, step_run_id, Airflow ids) when the call runs inside one; nulls ad hoc."""
    from pipelines.observability import current_step

    step = current_step()
    if not getattr(step, "persist", False):
        return {"pipeline": None, "step": None, "step_run_id": None, "dag_id": None, "dag_run_id": None}
    return {"pipeline": step.pipeline, "step": step.step, "step_run_id": step.step_run_id, "dag_id": step.dag_id,
            "dag_run_id": step.dag_run_id}


def _connect():
    import psycopg2
    from pipelines.commons.env_loader import CONSTRING

    return psycopg2.connect(CONSTRING, connect_timeout=5, application_name="datalabs-llm-usage")


def record_llm_call(call: LLMCall) -> bool:
    """Insert one call on a short connection of its own, so the caller's transaction is never touched (a failed
    insert inside it would abort the caller's work). Never raises."""
    if not recording_enabled():
        return False
    params = call.row() | _run_context()
    try:
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(INSERT_SQL, params)
        finally:
            conn.close()
        return True
    except Exception as exc:  # observability is best-effort (ADR 0001)
        log_event(logger, "llm_usage_write_failed", f"could not record LLM call {call.call_id}: {exc}",
                  level=logging.WARNING, app=call.app, model=call.model)
        return False
