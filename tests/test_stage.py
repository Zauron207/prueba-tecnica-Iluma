from decimal import Decimal

import pytest

from jobs_pipeline.extract import read_source_csv
from jobs_pipeline.stage import build_staging
from jobs_pipeline.validation import validate_staging


@pytest.fixture
def staged(sample_csv):
    staging, rejected = build_staging(read_source_csv(sample_csv))
    return staging.set_index("source_row_id", drop=False), rejected


def test_rechaza_duplicado_exacto_y_fila_sin_titulo(staged):
    _, rejected = staged
    assert rejected.to_dict("records") == [
        {"source_row_id": 2, "reason": "duplicado exacto de la fila 1"},
        {"source_row_id": 6, "reason": "sin job_title"},
    ]


def test_republicacion_no_es_duplicado(staged):
    # Las filas 1 y 3 solo difieren en la fecha: son dos publicaciones.
    staging, _ = staged
    assert {1, 3} <= set(staging.index)


def test_titulo_se_conserva_literal(staged):
    staging, _ = staged
    assert staging.loc[7, "job_title"] == "Data  Engineer SR"
    assert staging.loc[7, "role_name"] == "Senior Data Engineer"
    assert staging.loc[8, "job_title"] == "Data Engineer SR"
    assert staging.loc[8, "role_name"] == "Data Engineer"


def test_limpieza_aplicada(staged):
    staging, _ = staged
    fila = staging.loc[4]

    assert fila["platform_name"] == "LinkedIn"
    assert fila["schedule_types"] == ["Full-time", "Part-time"]
    assert fila["country_name"] == "Sierra Leone"
    assert staging.loc[5, "location_name"] == "Colorado Springs, CO"
    assert staging.loc[7, "schedule_types"] == ["Full-time"]


def test_empresas_unificadas(staged):
    staging, _ = staged
    assert staging.loc[4, "company_key"] == staging.loc[1, "company_key"] == "bnp paribas"
    assert set(staging["company_name"]) == {"BNP Paribas", "Acme Corp"}


def test_skills_sin_repetidos(staged):
    staging, _ = staged
    assert staging.loc[1, "skills"] == ["python", "sql", "sas"]
    assert staging.loc[7, "job_type_skills"]["programming"] == ["mongodb"]
    assert staging.loc[5, "skills"] == []


def test_salario_redondeado_a_centavos(staged):
    staging, _ = staged
    assert staging.loc[4, "salary_hour_avg"] == pytest.approx(25.5)
    assert staging.loc[5, "salary_rate"] == "month"


def test_staging_cumple_contrato(staged):
    staging, _ = staged
    validate_staging(staging.reset_index(drop=True))


def test_redondeo_conserva_decimal():
    # Desde Postgres los NUMERIC llegan como Decimal; el redondeo no debe pasarlos a float.
    from jobs_pipeline.stage import _round_money

    assert _round_money(Decimal("61.159996")) == Decimal("61.16")
    assert _round_money(None) is None
