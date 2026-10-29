"""Fase 2a: raw.job_postings -> staging.job_postings (limpio, una fila por oferta)."""

import logging

import pandas as pd
import psycopg
from psycopg import sql

from jobs_pipeline import cleaning
from jobs_pipeline.config import SQL_DIR
from jobs_pipeline.db import copy_rows, run_sql_file
from jobs_pipeline.extract import SOURCE_COLUMNS, iter_copy_rows

log = logging.getLogger(__name__)

STAGING_COLUMNS = [
    "source_row_id",
    "job_title",
    "role_name",
    "location_name",
    "platform_name",
    "schedule_types",
    "work_from_home",
    "search_location_name",
    "country_name",
    "posted_at",
    "no_degree_mention",
    "health_insurance",
    "salary_rate",
    "salary_year_avg",
    "salary_hour_avg",
    "company_key",
    "company_name",
    "skills",
    "job_type_skills",
]


def read_raw(conn: psycopg.Connection) -> pd.DataFrame:
    columns = ["source_row_id", *SOURCE_COLUMNS]
    query = sql.SQL("SELECT {} FROM raw.job_postings ORDER BY source_row_id").format(
        sql.SQL(", ").join(map(sql.Identifier, columns))
    )
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    log.info("raw.job_postings: %s filas leídas", f"{len(rows):,}")
    return pd.DataFrame(rows, columns=columns)


def find_rejections(raw: pd.DataFrame) -> pd.DataFrame:
    """Filas que no deben llegar al modelo, con el motivo.

    - Duplicados exactos (las 17 columnas iguales, incluida la fecha al
      segundo): es la misma oferta scrapeada dos veces. Se conserva la primera.
      Ojo: filas iguales salvo la fecha NO son duplicados, son republicaciones.
    - Ofertas sin título: sin título no hay rol ni nada que analizar.
    """
    # Las listas y dicts no son hasheables; su repr sí, y es estable.
    fingerprint = pd.util.hash_pandas_object(raw[SOURCE_COLUMNS].astype(str), index=False)
    first_seen = raw.groupby(fingerprint)["source_row_id"].transform("min")
    is_duplicate = raw["source_row_id"] != first_seen

    no_title = raw["job_title"].map(cleaning.normalize_text).isna() & ~is_duplicate

    reasons = pd.concat(
        [
            "duplicado exacto de la fila " + first_seen[is_duplicate].astype(str),
            pd.Series("sin job_title", index=raw.index[no_title]),
        ]
    )
    return pd.DataFrame(
        {"source_row_id": raw.loc[reasons.index, "source_row_id"], "reason": reasons}
    ).sort_values("source_row_id")


def build_staging(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rejected = find_rejections(raw)
    df = raw[~raw["source_row_id"].isin(rejected["source_row_id"])]

    company_keys = df["company_name"].map(cleaning.company_key)
    company_names = df["company_name"].map(cleaning.normalize_text)

    staging = pd.DataFrame(
        {
            "source_row_id": df["source_row_id"],
            # El título va tal cual: el rol (job_title_short) se calculó sobre el
            # texto exacto y normalizar espacios rompe job_title -> rol en 26
            # títulos ("MIS Analyst" vs "MIS  Analyst" tienen roles distintos).
            "job_title": df["job_title"],
            "role_name": df["job_title_short"].map(cleaning.normalize_text),
            "location_name": df["job_location"].map(cleaning.clean_location),
            "platform_name": df["job_via"].map(cleaning.clean_platform),
            "schedule_types": df["job_schedule_type"].map(cleaning.split_schedule_types),
            "work_from_home": df["job_work_from_home"],
            "search_location_name": df["search_location"].map(cleaning.normalize_text),
            "country_name": [
                cleaning.resolve_country(c, s)
                for c, s in zip(df["job_country"], df["search_location"], strict=True)
            ],
            "posted_at": df["job_posted_date"],
            "no_degree_mention": df["job_no_degree_mention"],
            "health_insurance": df["job_health_insurance"],
            "salary_rate": df["salary_rate"].map(cleaning.normalize_text),
            # Ruido de punto flotante del origen (61.159996): se redondea a centavos.
            "salary_year_avg": df["salary_year_avg"].map(_round_money),
            "salary_hour_avg": df["salary_hour_avg"].map(_round_money),
            "company_key": company_keys,
            "company_name": cleaning.canonical_names(company_keys, company_names),
            "skills": df["job_skills"].map(cleaning.normalize_skills),
            "job_type_skills": df["job_type_skills"].map(cleaning.normalize_skill_types),
        }
    ).reset_index(drop=True)

    log.info(
        "staging: %s ofertas válidas, %s rechazadas", f"{len(staging):,}", f"{len(rejected):,}"
    )
    return staging, rejected


def _round_money(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    return round(value, 2)


def load_staging(conn: psycopg.Connection, staging: pd.DataFrame, rejected: pd.DataFrame) -> None:
    with conn.transaction():
        run_sql_file(conn, SQL_DIR / "staging.sql")
        copy_rows(
            conn, "staging.job_postings", STAGING_COLUMNS, iter_copy_rows(staging, STAGING_COLUMNS)
        )
        copy_rows(
            conn,
            "staging.rejected_postings",
            ["source_row_id", "reason"],
            iter_copy_rows(rejected, ["source_row_id", "reason"]),
        )
    log.info("staging.job_postings y staging.rejected_postings cargadas")
