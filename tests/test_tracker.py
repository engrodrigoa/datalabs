"""StepRun counters and metrics: what a step reports is what lands in audit.pipeline_step_runs."""
from pipelines.observability.tracker import StepRun


def _step() -> StepRun:
    return StepRun(pipeline="test", step="tracker", persist=False)


def test_numeric_metrics_accumulate_like_counters():
    # found in a real run: 37 files (GBs) reported m_bytes_downloaded=980, the size of the last file
    step = _step()
    for size in (552_734_286, 22_078, 980):
        step.add(bytes_downloaded=size)
    assert step.metrics["bytes_downloaded"] == 552_734_286 + 22_078 + 980


def test_non_numeric_metrics_are_overwritten():
    step = _step()
    step.add(profile={"rows": 1}).add(profile={"rows": 2}, source="a").add(source="b", flag=True).add(flag=False)
    assert step.metrics == {"profile": {"rows": 2}, "source": "b", "flag": False}   # bool is not summed


def test_number_after_text_overwrites_instead_of_failing():
    step = _step()
    step.set(size="unknown").add(size=10)
    assert step.metrics["size"] == 10


def test_label_keeps_every_distinct_value_in_order():
    # rfb11 loads 6 domain tables in one step: the run reported entity="motivos" (the last one)
    step = _step()
    for entity in ("cnaes", "municipios", "cnaes", "motivos"):
        step.label(entity=entity)
    assert step.metrics["entity"] == "cnaes,municipios,motivos"


def test_one_step_over_several_entities_totals_files():
    step = _step()
    for _ in range(6):                              # what processar_arquivos_landing does per domain table
        step.add(files_total=1).label(entity="x").add(files_ok=1, suspicious_rows=0)
    assert step.counters == {"files_total": 6, "files_ok": 6}
    assert step.metrics["suspicious_rows"] == 0
