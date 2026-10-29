"""Fase 2b: staging -> core (modelo 3FN) y chequeos de calidad posteriores."""

import logging

import psycopg
from psycopg import sql

from jobs_pipeline.config import SQL_DIR
from jobs_pipeline.db import run_sql_file

log = logging.getLogger(__name__)

# Cada consulta devuelve la cantidad de filas que violan la regla; 0 = OK.
QUALITY_CHECKS: dict[str, str] = {
    # Todo lo que entró a raw está en core o en rechazados, sin huecos.
    "conciliacion_raw_vs_core": """
        SELECT abs(
            (SELECT count(*) FROM raw.job_postings)
          - (SELECT count(*) FROM core.jobs)
          - (SELECT count(*) FROM staging.rejected_postings)
        )
    """,
    # Cada skill de staging quedó en la tabla puente.
    "skills_completos": """
        SELECT abs(
            (SELECT coalesce(sum(cardinality(skills)), 0) FROM staging.job_postings)
          - (SELECT count(*) FROM core.job_skills)
        )
    """,
    # job_type_skills no se guarda en core: se reconstruye desde job_skills +
    # skill_type_assignments. Si la reconstrucción no coincide con el origen,
    # la normalización perdió o inventó información.
    "job_type_skills_reconstruible": """
        WITH origen AS (
            SELECT s.source_row_id, t.skill_type_name, e.skill_name
            FROM staging.job_postings s
            CROSS JOIN LATERAL jsonb_each(s.job_type_skills) AS t (skill_type_name, skill_list)
            CROSS JOIN LATERAL jsonb_array_elements_text(t.skill_list) AS e (skill_name)
        ),
        modelo AS (
            SELECT j.source_row_id, st.skill_type_name, sk.skill_name
            FROM core.job_skills js
            JOIN core.jobs j                    ON j.job_id = js.job_id
            JOIN core.skills sk                 ON sk.skill_id = js.skill_id
            JOIN core.skill_type_assignments sa ON sa.skill_id = js.skill_id
            JOIN core.skill_types st            ON st.skill_type_id = sa.skill_type_id
        )
        SELECT count(*) FROM (
            (SELECT * FROM origen EXCEPT SELECT * FROM modelo)
            UNION ALL
            (SELECT * FROM modelo EXCEPT SELECT * FROM origen)
        ) diferencias
    """,
    "salarios_completos": """
        SELECT abs(
            (SELECT count(*) FROM staging.job_postings WHERE salary_rate IS NOT NULL)
          - (SELECT count(*) FROM core.job_salaries)
        )
    """,
    # Una oferta sin ningún tipo de jornada es válida (1,6% viene vacío), pero
    # una con texto de jornada que no generó filas puente sería un error.
    "jornadas_completas": """
        SELECT abs(
            (SELECT coalesce(sum(cardinality(schedule_types)), 0) FROM staging.job_postings)
          - (SELECT count(*) FROM core.job_schedule_types)
        )
    """,
}


def load_core(conn: psycopg.Connection) -> None:
    # DDL + carga en una sola transacción: o queda todo nuevo o todo como estaba.
    with conn.transaction():
        run_sql_file(conn, SQL_DIR / "schema.sql")
        run_sql_file(conn, SQL_DIR / "load_core.sql")
    log_table_counts(conn)


def log_table_counts(conn: psycopg.Connection) -> None:
    tables = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'core' ORDER BY tablename"
    ).fetchall()
    for (table,) in tables:
        query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("core", table))
        count = conn.execute(query).fetchone()[0]
        log.info("core.%-24s %12s filas", table, f"{count:,}")


def run_quality_checks(conn: psycopg.Connection) -> None:
    failed = []
    for name, query in QUALITY_CHECKS.items():
        violations = conn.execute(query).fetchone()[0]
        if violations:
            log.error("Chequeo %s: %s violaciones", name, f"{violations:,}")
            failed.append(name)
        else:
            log.info("Chequeo %s: OK", name)
    if failed:
        raise RuntimeError(f"Fallaron chequeos de calidad: {', '.join(failed)}")
