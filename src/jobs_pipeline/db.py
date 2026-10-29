"""Utilidades mínimas de base de datos sobre psycopg 3."""

import logging
from collections.abc import Iterable, Sequence
from pathlib import Path

import psycopg
from psycopg import sql

log = logging.getLogger(__name__)


def connect(conninfo: str) -> psycopg.Connection:
    # autocommit + bloques conn.transaction() explícitos: así cada paso deja
    # claro dónde empieza y termina su transacción.
    return psycopg.connect(conninfo, autocommit=True)


def run_sql_file(conn: psycopg.Connection, path: Path) -> None:
    """Ejecuta un archivo .sql completo. No hace commit: lo decide quien llama."""
    log.info("Ejecutando %s", path.name)
    conn.execute(path.read_text(encoding="utf-8"))


def copy_rows(
    conn: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence],
) -> int:
    """Carga filas con COPY, que es mucho más rápido que INSERTs fila a fila.

    `table` va como "schema.tabla"; se compone con sql.Identifier para no armar
    SQL concatenando strings.
    """
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(*table.split(".")),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )
    count = 0
    with conn.cursor() as cur, cur.copy(statement) as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    return count
