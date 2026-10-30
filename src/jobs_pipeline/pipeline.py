"""Punto de entrada del pipeline.

Uso:
    python -m jobs_pipeline.pipeline                 # todo
    python -m jobs_pipeline.pipeline --step stage    # un solo paso
"""

import argparse
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import psycopg

from jobs_pipeline.config import load_settings
from jobs_pipeline.db import connect
from jobs_pipeline.extract import load_raw, read_source_csv
from jobs_pipeline.load_core import load_core, run_quality_checks
from jobs_pipeline.stage import build_staging, load_staging, read_raw
from jobs_pipeline.validation import validate_raw, validate_staging

log = logging.getLogger("jobs_pipeline")

STEPS = ("extract", "stage", "core")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


@contextmanager
def timed(step: str) -> Iterator[None]:
    log.info("==> %s", step)
    start = time.perf_counter()
    yield
    log.info("<== %s (%.1f s)", step, time.perf_counter() - start)


# Un paso por función. Las usan tanto run() (CLI) como las tareas de Prefect en
# flow.py, así que la lógica existe en un solo lugar.


def extract_step(conn: psycopg.Connection, csv_path: Path) -> None:
    with timed("extract: CSV -> raw"):
        if not csv_path.exists():
            raise FileNotFoundError(f"No existe el CSV: {csv_path}")
        load_raw(conn, validate_raw(read_source_csv(csv_path)))


def stage_step(conn: psycopg.Connection) -> None:
    with timed("stage: raw -> staging"):
        staging, rejected = build_staging(read_raw(conn))
        load_staging(conn, validate_staging(staging), rejected)


def core_step(conn: psycopg.Connection) -> None:
    with timed("core: staging -> modelo 3FN"):
        load_core(conn)


def quality_step(conn: psycopg.Connection) -> None:
    with timed("chequeos de calidad"):
        run_quality_checks(conn)


def run(steps: tuple[str, ...], csv_path: Path | None = None) -> None:
    settings = load_settings()
    csv_path = csv_path or settings.csv_path

    with connect(settings.conninfo) as conn:
        if "extract" in steps:
            extract_step(conn, csv_path)
        if "stage" in steps:
            stage_step(conn)
        if "core" in steps:
            core_step(conn)
            quality_step(conn)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline data_jobs -> PostgreSQL 3FN")
    parser.add_argument("--step", choices=(*STEPS, "all"), default="all")
    parser.add_argument("--csv", type=Path, help="Ruta al CSV (por defecto CSV_PATH)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    steps = STEPS if args.step == "all" else (args.step,)
    try:
        with timed(f"pipeline ({', '.join(steps)})"):
            run(steps, args.csv)
    except Exception:
        log.exception("El pipeline falló")
        sys.exit(1)


if __name__ == "__main__":
    main()
