"""El mismo pipeline, orquestado con Prefect.

Cada paso de pipeline.py es una tarea. Las tareas no se pasan datos entre sí:
cada una lee de la base lo que dejó la anterior, así que se pueden reintentar o
correr por separado sin depender de lo que haya en memoria.

Uso:
    jobs-flow                          # una corrida
    jobs-flow --cron "0 6 * * *"       # queda corriendo y ejecuta según el cron
"""

import argparse
import logging
import os
from collections.abc import Callable
from pathlib import Path

# Prefect arma su configuración de logging al importarse, así que esto tiene que
# ir antes del import. Sin esto los logs del pipeline (conteos, chequeos) no
# aparecen en la consola ni en la UI de Prefect, solo los del propio Prefect.
os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "jobs_pipeline")

import psycopg  # noqa: E402
from prefect import flow, task  # noqa: E402
from prefect.client.schemas.objects import TaskRun  # noqa: E402
from prefect.states import State  # noqa: E402
from prefect.tasks import Task  # noqa: E402

from jobs_pipeline import pipeline  # noqa: E402
from jobs_pipeline.config import load_settings  # noqa: E402
from jobs_pipeline.db import connect  # noqa: E402

logging.getLogger("jobs_pipeline").setLevel(logging.INFO)


def is_transient_error(task: Task, task_run: TaskRun, state: State) -> bool:
    """Solo vale la pena reintentar si falló la conexión con la base.

    Un error de datos (un contrato de Pandera, un chequeo de calidad, una FK que
    no valida) va a fallar igual en el segundo intento, y reintentarlo solo
    demora el aviso.
    """
    try:
        state.result()
    except psycopg.OperationalError:
        return True
    except Exception:
        return False
    return False


# Reintentar es seguro porque cada paso es idempotente: recarga su capa completa
# dentro de una transacción, así que un intento a medias no deja basura.
DB_RETRIES = {"retries": 3, "retry_delay_seconds": 20, "retry_condition_fn": is_transient_error}


def _run_with_connection(step: Callable, *args: object) -> None:
    # Una conexión por tarea: las conexiones no se comparten entre tareas, que
    # pueden correr en otro hilo o en otro proceso.
    with connect(load_settings().conninfo) as conn:
        step(conn, *args)


@task(name="extract", **DB_RETRIES)
def extract(csv_path: Path) -> None:
    _run_with_connection(pipeline.extract_step, csv_path)


@task(name="stage", **DB_RETRIES)
def stage() -> None:
    _run_with_connection(pipeline.stage_step)


@task(name="core", **DB_RETRIES)
def core() -> None:
    _run_with_connection(pipeline.core_step)


@task(name="quality-checks", **DB_RETRIES)
def quality_checks() -> None:
    _run_with_connection(pipeline.quality_step)


@flow(name="data-jobs-pipeline")
def data_jobs_pipeline(csv_path: str | None = None) -> None:
    csv = Path(csv_path) if csv_path else load_settings().csv_path
    extract(csv)
    stage()
    core()
    quality_checks()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline data_jobs orquestado con Prefect")
    parser.add_argument("--csv", help="Ruta al CSV (por defecto CSV_PATH)")
    parser.add_argument(
        "--cron",
        help='Deja el flow programado en vez de correrlo una vez, ej. "0 6 * * *"',
    )
    args = parser.parse_args(argv)

    if args.cron:
        data_jobs_pipeline.serve(
            name="data-jobs-diario", cron=args.cron, parameters={"csv_path": args.csv}
        )
    else:
        data_jobs_pipeline(csv_path=args.csv)


if __name__ == "__main__":
    main()
