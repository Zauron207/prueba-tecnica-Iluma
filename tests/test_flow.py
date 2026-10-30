"""El flow de Prefect: orden de las tareas y política de reintentos.

Los pasos reales se reemplazan por funciones falsas, así que no hace falta
Postgres. Se saltan si Prefect no está instalado (es una dependencia opcional).
"""

import psycopg
import pytest

pytest.importorskip("prefect")

from prefect.testing.utilities import prefect_test_harness  # noqa: E402

from jobs_pipeline import flow as jobs_flow  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def prefect_harness():
    # Levanta un Prefect temporal con su propia base, aislado de cualquier otro.
    with prefect_test_harness():
        yield


class FakeSteps:
    """Reemplaza la ejecución real de cada paso: registra el orden y puede fallar."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.failures: dict[str, list[Exception]] = {}

    def __call__(self, step, *args) -> None:
        self.calls.append(step.__name__)
        pending = self.failures.get(step.__name__)
        if pending:
            raise pending.pop(0)


@pytest.fixture
def steps(monkeypatch) -> FakeSteps:
    fake = FakeSteps()
    monkeypatch.setattr(jobs_flow, "_run_with_connection", fake)
    # Sin espera entre reintentos para que el test no tarde.
    for name in ("extract", "stage", "core", "quality_checks"):
        monkeypatch.setattr(
            jobs_flow, name, getattr(jobs_flow, name).with_options(retry_delay_seconds=0)
        )
    return fake


def test_tareas_en_orden(steps):
    jobs_flow.data_jobs_pipeline(csv_path="data_jobs.csv")
    assert steps.calls == ["extract_step", "stage_step", "core_step", "quality_step"]


def test_reintenta_si_se_cae_la_conexion(steps):
    steps.failures["stage_step"] = [psycopg.OperationalError("connection refused")]

    jobs_flow.data_jobs_pipeline(csv_path="data_jobs.csv")

    assert steps.calls.count("stage_step") == 2
    assert steps.calls[-1] == "quality_step"


def test_no_reintenta_errores_de_datos(steps):
    steps.failures["quality_step"] = [RuntimeError("Fallaron chequeos de calidad")]

    with pytest.raises(RuntimeError, match="chequeos de calidad"):
        jobs_flow.data_jobs_pipeline(csv_path="data_jobs.csv")

    assert steps.calls.count("quality_step") == 1


def test_no_sigue_si_falla_un_paso(steps):
    steps.failures["extract_step"] = [FileNotFoundError("No existe el CSV")]

    with pytest.raises(FileNotFoundError):
        jobs_flow.data_jobs_pipeline(csv_path="no_existe.csv")

    assert steps.calls == ["extract_step"]
