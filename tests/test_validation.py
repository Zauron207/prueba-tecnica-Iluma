import pandas as pd
import pandera.errors
import pytest

from jobs_pipeline.extract import read_source_csv
from jobs_pipeline.stage import build_staging
from jobs_pipeline.validation import (
    functional_dependency_violations,
    validate_raw,
    validate_staging,
)


def test_raw_valido_pasa(sample_csv):
    validate_raw(read_source_csv(sample_csv))


def test_raw_detecta_skills_inconsistentes(sample_csv):
    df = read_source_csv(sample_csv)
    df.at[0, "job_skills"] = ["python"]  # el dict sigue diciendo python, sql, sas

    with pytest.raises(pandera.errors.SchemaErrors, match="skills_list_matches_type_dict"):
        validate_raw(df)


def test_raw_detecta_salario_anual_con_tarifa_horaria(sample_csv):
    df = read_source_csv(sample_csv)
    df.at[0, "salary_rate"] = "hour"

    with pytest.raises(pandera.errors.SchemaErrors, match="year_avg_only_with_year_rate"):
        validate_raw(df)


def test_raw_detecta_tarifa_desconocida(sample_csv):
    df = read_source_csv(sample_csv)
    df.at[0, "salary_rate"] = "decade"

    with pytest.raises(pandera.errors.SchemaErrors):
        validate_raw(df)


def test_functional_dependency_violations():
    df = pd.DataFrame(
        {
            "title": ["MIS Analyst", "MIS Analyst", "Data Engineer", "Data Engineer"],
            "role": ["Data Scientist", "Software Engineer", "Data Engineer", "Data Engineer"],
        }
    )
    broken = functional_dependency_violations(df, "title", "role")
    assert broken.index.tolist() == ["MIS Analyst"]


def test_staging_falla_si_un_titulo_tiene_dos_roles(sample_csv):
    staging, _ = build_staging(read_source_csv(sample_csv))
    staging.loc[staging["job_title"] == "Data Engineer", "role_name"] = ["Data Engineer", "Otro"]

    with pytest.raises(ValueError, match="job_title -> role_name"):
        validate_staging(staging)
