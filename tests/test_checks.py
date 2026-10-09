from datetime import datetime, timedelta, timezone

import pytest

from pipelines.observability.checks import (
    DEFAULT_CONFIG, evaluate_freshness, evaluate_reconciliation, evaluate_threshold, load_checks,
)

NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
FRESH = {"severity": "error", "warn_after_hours": 24, "error_after_hours": 48}


@pytest.mark.parametrize("age_h,expected", [(1, "pass"), (30, "warn"), (60, "fail")])
def test_freshness_tiers(age_h, expected):
    status, observed, _, _ = evaluate_freshness(FRESH, NOW - timedelta(hours=age_h), now=NOW)
    assert status == expected
    assert observed == pytest.approx(age_h)


def test_freshness_empty_dataset_fails():
    assert evaluate_freshness(FRESH, None, now=NOW)[0] == "fail"


def test_freshness_with_warn_severity_never_fails():
    status, *_ = evaluate_freshness({**FRESH, "severity": "warn"}, NOW - timedelta(days=30), now=NOW)
    assert status == "warn"


def test_freshness_accepts_dates():
    status, observed, *_ = evaluate_freshness(FRESH, (NOW - timedelta(hours=12)).date(), now=NOW)
    assert status == "pass" and observed == 12


@pytest.mark.parametrize("value,expected", [(0, "pass"), (5, "fail"), (None, "fail")])
def test_threshold_max(value, expected):
    assert evaluate_threshold({"severity": "error", "max": 0}, value)[0] == expected


def test_threshold_min_warn():
    assert evaluate_threshold({"severity": "warn", "min": 27}, 26)[0] == "warn"


@pytest.mark.parametrize("left,right,tol,expected", [
    (100, 100, 0, "pass"), (100, 99, 0, "fail"), (100, 99, 1, "pass"), (0, 0, 0, "pass"),
])
def test_reconciliation(left, right, tol, expected):
    assert evaluate_reconciliation({"severity": "error", "tolerance_pct": tol}, left, right)[0] == expected


def test_config_is_valid():
    checks = load_checks(DEFAULT_CONFIG, "all")
    names = [c["name"] for c in checks]
    assert len(names) == len(set(names)), "duplicated check names"
    for c in checks:
        assert c["type"] in {"freshness", "threshold", "reconciliation"}, c["name"]
        assert c["severity"] in {"warn", "error"}, c["name"]
        assert c.get("query"), c["name"]
        if c["type"] == "reconciliation":
            assert c.get("compare_query"), c["name"]
        if c["type"] == "freshness":
            assert c.get("warn_after_hours") or c.get("error_after_hours"), c["name"]


def test_pipeline_filter():
    anp = load_checks(DEFAULT_CONFIG, "anp")
    assert anp and all(c["pipeline"] == "anp" for c in anp)
