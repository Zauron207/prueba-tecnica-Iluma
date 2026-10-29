"""Pipeline completo contra PostgreSQL usando el CSV de ejemplo.

Usa una base aparte (<POSTGRES_DB>_test) para no tocar los datos reales.
Si no hay Postgres disponible, los tests se saltan.
"""

import psycopg
import pytest
from psycopg import sql

from jobs_pipeline.config import load_settings
from jobs_pipeline.pipeline import STEPS, run

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def test_db():
    try:
        settings = load_settings()
        admin = psycopg.connect(settings.conninfo, autocommit=True, connect_timeout=3)
    except (RuntimeError, psycopg.OperationalError) as exc:
        pytest.skip(f"PostgreSQL no disponible: {exc}")

    test_name = f"{settings.db_name}_test"
    with admin:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (test_name,)
        ).fetchone()
        if not exists:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(test_name)))
    return settings, test_name


@pytest.fixture(scope="module")
def loaded_db(test_db, request):
    settings, test_name = test_db
    mp = pytest.MonkeyPatch()
    mp.setenv("POSTGRES_DB", test_name)
    request.addfinalizer(mp.undo)

    csv_path = request.path.parent / "fixtures" / "sample_jobs.csv"
    run(STEPS, csv_path)
    # Correrlo dos veces comprueba que el pipeline es idempotente.
    run(STEPS, csv_path)

    conninfo = settings.conninfo.replace(f"dbname={settings.db_name}", f"dbname={test_name}")
    with psycopg.connect(conninfo) as conn:
        yield conn


def count(conn, table):
    query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(*table.split(".")))
    return conn.execute(query).fetchone()[0]


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("raw.job_postings", 8),
        ("staging.rejected_postings", 2),
        ("core.jobs", 6),
        ("core.companies", 2),
        ("core.countries", 3),
        ("core.search_locations", 4),
        ("core.locations", 4),
        ("core.platforms", 3),
        ("core.job_roles", 4),
        ("core.job_titles", 5),
        ("core.schedule_types", 4),
        ("core.skills", 6),
        ("core.skill_types", 4),
        ("core.skill_type_assignments", 8),
        ("core.job_skills", 10),
        ("core.job_schedule_types", 9),
        ("core.job_salaries", 4),
    ],
)
def test_conteos(loaded_db, table, expected):
    assert count(loaded_db, table) == expected


def test_claves_foraneas_quedan_activas(loaded_db):
    # load_core las quita para cargar rápido; deben volver todas.
    fks = loaded_db.execute(
        """
        SELECT count(*) FROM pg_constraint
        WHERE contype = 'f' AND connamespace = 'core'::regnamespace AND convalidated
        """
    ).fetchone()[0]
    assert fks == 14


def test_tipos_semiestructurados_en_raw(loaded_db):
    skills, types = loaded_db.execute(
        "SELECT job_skills, job_type_skills FROM raw.job_postings WHERE source_row_id = 1"
    ).fetchone()
    assert skills == ["python", "sql", "sas", "sas"]
    assert types == {"analyst_tools": ["sas"], "programming": ["python", "sql", "sas"]}


def test_skill_con_dos_tipos(loaded_db):
    types = loaded_db.execute(
        """
        SELECT array_agg(st.skill_type_name ORDER BY st.skill_type_name)
        FROM core.skills s
        JOIN core.skill_type_assignments a USING (skill_id)
        JOIN core.skill_types st USING (skill_type_id)
        WHERE s.skill_name = 'sas'
        """
    ).fetchone()[0]
    assert types == ["analyst_tools", "programming"]


def test_pais_se_obtiene_desde_la_busqueda(loaded_db):
    country = loaded_db.execute(
        """
        SELECT c.country_name
        FROM core.jobs j
        JOIN core.search_locations sl USING (search_location_id)
        JOIN core.countries c USING (country_id)
        WHERE j.source_row_id = 4
        """
    ).fetchone()[0]
    assert country == "Sierra Leone"


def test_salario_mensual_sin_promedio(loaded_db):
    row = loaded_db.execute(
        """
        SELECT s.salary_rate, s.salary_year_avg, s.salary_hour_avg
        FROM core.job_salaries s JOIN core.jobs j USING (job_id)
        WHERE j.source_row_id = 5
        """
    ).fetchone()
    assert row == ("month", None, None)
