-- Capa STAGING: una fila por oferta ya limpia, todavía desnormalizada.
-- Es transitoria (se recrea en cada corrida), por eso UNLOGGED: no genera WAL
-- y si se pierde en un crash basta con volver a correr el paso.

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.job_postings;
DROP TABLE IF EXISTS staging.rejected_postings;

CREATE UNLOGGED TABLE staging.job_postings (
    source_row_id        BIGINT PRIMARY KEY,
    job_title            TEXT    NOT NULL,
    role_name            TEXT    NOT NULL,
    location_name        TEXT,
    platform_name        TEXT,
    schedule_types       TEXT[]  NOT NULL,
    work_from_home       BOOLEAN NOT NULL,
    search_location_name TEXT    NOT NULL,
    country_name         TEXT    NOT NULL,
    posted_at            TIMESTAMP NOT NULL,
    no_degree_mention    BOOLEAN NOT NULL,
    health_insurance     BOOLEAN NOT NULL,
    salary_rate          TEXT,
    salary_year_avg      NUMERIC(12, 2),
    salary_hour_avg      NUMERIC(10, 2),
    company_key          TEXT,
    company_name         TEXT,
    skills               TEXT[]  NOT NULL,
    job_type_skills      JSONB   NOT NULL
);

-- Filas que no pasan al modelo, con el motivo. Nada se descarta en silencio.
CREATE UNLOGGED TABLE staging.rejected_postings (
    source_row_id BIGINT PRIMARY KEY,
    reason        TEXT NOT NULL
);
