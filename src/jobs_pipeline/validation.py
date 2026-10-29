"""Contratos de datos (Pandera) que se validan antes de cada carga."""

import logging

import pandas as pd
import pandera.pandas as pa

log = logging.getLogger(__name__)

SALARY_RATES = {"year", "month", "week", "day", "hour"}


def _is_list_or_null(value: object) -> bool:
    return value is None or isinstance(value, list)


def _is_dict_or_null(value: object) -> bool:
    return value is None or isinstance(value, dict)


def _flatten(types: dict[str, list[str]] | None) -> set[str]:
    return {s for skills in (types or {}).values() for s in skills}


def _skills_match_types(df: pd.DataFrame) -> pd.Series:
    """job_skills y job_type_skills deben describir exactamente los mismos skills."""
    return pd.Series(
        [
            set(skills or []) == _flatten(types)
            for skills, types in zip(df["job_skills"], df["job_type_skills"], strict=True)
        ],
        index=df.index,
    )


def _salary_matches_rate(df: pd.DataFrame, avg_col: str, rate: str) -> pd.Series:
    return df[avg_col].isna() | (df["salary_rate"] == rate)


def _is_datetime(series: pd.Series) -> bool:
    return pd.api.types.is_datetime64_any_dtype(series)


raw_schema = pa.DataFrameSchema(
    {
        "source_row_id": pa.Column(int, pa.Check.ge(1), unique=True),
        "job_title_short": pa.Column(str, nullable=False),
        "job_title": pa.Column(str, nullable=True),
        "search_location": pa.Column(str, nullable=False),
        "job_posted_date": pa.Column(checks=pa.Check(_is_datetime), nullable=False),
        "job_work_from_home": pa.Column(bool),
        "job_no_degree_mention": pa.Column(bool),
        "job_health_insurance": pa.Column(bool),
        "salary_rate": pa.Column(str, pa.Check.isin(SALARY_RATES), nullable=True),
        "salary_year_avg": pa.Column(float, pa.Check.gt(0), nullable=True),
        "salary_hour_avg": pa.Column(float, pa.Check.gt(0), nullable=True),
        "job_skills": pa.Column(
            checks=pa.Check(_is_list_or_null, element_wise=True), nullable=True
        ),
        "job_type_skills": pa.Column(
            checks=pa.Check(_is_dict_or_null, element_wise=True), nullable=True
        ),
    },
    checks=[
        pa.Check(_skills_match_types, name="skills_list_matches_type_dict"),
        pa.Check(
            lambda df: _salary_matches_rate(df, "salary_year_avg", "year"),
            name="year_avg_only_with_year_rate",
        ),
        pa.Check(
            lambda df: _salary_matches_rate(df, "salary_hour_avg", "hour"),
            name="hour_avg_only_with_hour_rate",
        ),
    ],
    strict=False,
)


staging_schema = pa.DataFrameSchema(
    {
        "source_row_id": pa.Column(int, unique=True),
        "job_title": pa.Column(str, nullable=False),
        "role_name": pa.Column(str, nullable=False),
        "search_location_name": pa.Column(str, nullable=False),
        "country_name": pa.Column(str, nullable=False),
        "posted_at": pa.Column(checks=pa.Check(_is_datetime), nullable=False),
        "schedule_types": pa.Column(
            checks=pa.Check(lambda v: isinstance(v, list), element_wise=True)
        ),
        "skills": pa.Column(checks=pa.Check(lambda v: isinstance(v, list), element_wise=True)),
        "job_type_skills": pa.Column(
            checks=pa.Check(lambda v: isinstance(v, dict), element_wise=True)
        ),
        "salary_rate": pa.Column(str, pa.Check.isin(SALARY_RATES), nullable=True),
    },
    checks=[
        pa.Check(
            lambda df: df["company_key"].isna() == df["company_name"].isna(),
            name="company_key_and_name_together",
        ),
    ],
    strict=False,
)


def functional_dependency_violations(
    df: pd.DataFrame, determinant: str, dependent: str
) -> pd.Series:
    """Valores de `determinant` que se asocian a más de un `dependent`.

    El modelo 3FN asume ciertas dependencias funcionales (por ejemplo,
    job_title -> role). Si los datos las rompen, el modelo dejaría de ser
    correcto, así que se verifican explícitamente antes de cargar.
    """
    pairs = df[[determinant, dependent]].dropna().drop_duplicates()
    counts = pairs.groupby(determinant)[dependent].nunique()
    return counts[counts > 1]


STAGING_DEPENDENCIES = [
    ("job_title", "role_name"),
    ("search_location_name", "country_name"),
    ("company_key", "company_name"),
]


def validate_raw(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Validando contrato de datos raw (%s filas)", f"{len(df):,}")
    return raw_schema.validate(df, lazy=True)


def validate_staging(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Validando contrato de datos staging (%s filas)", f"{len(df):,}")
    validated = staging_schema.validate(df, lazy=True)
    for determinant, dependent in STAGING_DEPENDENCIES:
        broken = functional_dependency_violations(df, determinant, dependent)
        if not broken.empty:
            sample = ", ".join(map(repr, broken.index[:5]))
            raise ValueError(
                f"Se rompe la dependencia {determinant} -> {dependent} en "
                f"{len(broken)} valores (ej.: {sample})"
            )
    return validated
