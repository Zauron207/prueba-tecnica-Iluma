"""Fase 1: leer el CSV, tiparlo y cargarlo tal cual en raw.job_postings."""

import ast
import logging
from collections.abc import Callable, Iterator
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb

from jobs_pipeline.config import SQL_DIR
from jobs_pipeline.db import copy_rows, run_sql_file

log = logging.getLogger(__name__)

SOURCE_COLUMNS = [
    "job_title_short",
    "job_title",
    "job_location",
    "job_via",
    "job_schedule_type",
    "job_work_from_home",
    "search_location",
    "job_posted_date",
    "job_no_degree_mention",
    "job_health_insurance",
    "job_country",
    "salary_rate",
    "salary_year_avg",
    "salary_hour_avg",
    "company_name",
    "job_skills",
    "job_type_skills",
]
BOOL_COLUMNS = ["job_work_from_home", "job_no_degree_mention", "job_health_insurance"]
NUMERIC_COLUMNS = ["salary_year_avg", "salary_hour_avg"]
RAW_COLUMNS = ["source_row_id", *SOURCE_COLUMNS, "source_file"]


def parse_bool(value: str) -> bool:
    """El CSV trae 'True'/'False'. Cualquier otra cosa es un error de origen."""
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"Valor booleano inesperado: {value!r}")


def _literal(value: str | None) -> object | None:
    # Los campos vienen como repr de Python (comillas simples), no como JSON,
    # así que json.loads no sirve. literal_eval solo acepta literales: no
    # ejecuta código, a diferencia de eval().
    if value is None or not value.strip():
        return None
    return ast.literal_eval(value)


def parse_skill_list(value: str | None) -> list[str] | None:
    parsed = _literal(value)
    if parsed is None:
        return None
    if not isinstance(parsed, list) or not all(isinstance(s, str) for s in parsed):
        raise ValueError(f"job_skills no es una lista de strings: {value!r}")
    return parsed


def parse_skill_dict(value: str | None) -> dict[str, list[str]] | None:
    parsed = _literal(value)
    if parsed is None:
        return None
    if not isinstance(parsed, dict) or not all(
        isinstance(k, str) and isinstance(v, list) and all(isinstance(s, str) for s in v)
        for k, v in parsed.items()
    ):
        raise ValueError(f"job_type_skills no es un dict[str, list[str]]: {value!r}")
    return parsed


def _parse_distinct(series: pd.Series, parser: Callable[[str], object]) -> pd.Series:
    # Hay ~177 mil textos distintos en ~668 mil filas con skills: parsear cada
    # texto una sola vez reduce bastante el tiempo de literal_eval.
    parsed = {value: parser(value) for value in series.dropna().unique()}
    return series.map(lambda value: parsed.get(value))


def read_source_csv(path: Path) -> pd.DataFrame:
    """Lee el CSV y devuelve un DataFrame con tipos reales.

    Todo se lee primero como texto para que pandas no adivine tipos (por
    ejemplo, convertir 'True' o fechas a su manera); luego se convierte cada
    columna explícitamente y cualquier valor inesperado hace fallar la lectura.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")

    missing = set(SOURCE_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Al CSV le faltan columnas: {sorted(missing)}")
    df = df[SOURCE_COLUMNS].astype(object)

    # Cadena vacía = dato ausente. (df.map convertiría los None en NaN.)
    df = df.where(df != "", None)

    for col in BOOL_COLUMNS:
        df[col] = df[col].map(parse_bool)
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise")
    df["job_posted_date"] = pd.to_datetime(
        df["job_posted_date"], format="%Y-%m-%d %H:%M:%S", errors="raise"
    )
    df["job_skills"] = _parse_distinct(df["job_skills"], parse_skill_list)
    df["job_type_skills"] = _parse_distinct(df["job_type_skills"], parse_skill_dict)

    # Número de fila de datos en el archivo (1 = primera fila después del header).
    df.insert(0, "source_row_id", range(1, len(df) + 1))
    df["source_file"] = path.name
    return df


def _to_python(value: object) -> object:
    """Convierte tipos de pandas/numpy a tipos que psycopg sabe adaptar."""
    if value is None:
        return None
    if isinstance(value, dict):
        return Jsonb(value)
    if isinstance(value, list):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # escalares numpy -> int/float/bool de Python
        return value.item()
    return value


def iter_copy_rows(df: pd.DataFrame, columns: list[str]) -> Iterator[tuple]:
    for row in df[columns].itertuples(index=False, name=None):
        yield tuple(_to_python(v) for v in row)


def load_raw(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    """Recarga completa de raw.job_postings en una transacción."""
    with conn.transaction():
        run_sql_file(conn, SQL_DIR / "raw.sql")
        conn.execute("TRUNCATE raw.job_postings")
        count = copy_rows(conn, "raw.job_postings", RAW_COLUMNS, iter_copy_rows(df, RAW_COLUMNS))
    log.info("raw.job_postings: %s filas cargadas", f"{count:,}")
    return count
