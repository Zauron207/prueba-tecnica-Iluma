-- Capa RAW: copia fiel del CSV, solo con tipos correctos.
-- No se limpia nada aquí; si la lógica de limpieza cambia, se reprocesa desde
-- esta tabla sin volver a leer el archivo.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.job_postings (
    source_row_id         BIGINT PRIMARY KEY,  -- número de fila en el CSV (linaje)
    job_title_short       TEXT,
    job_title             TEXT,
    job_location          TEXT,
    job_via               TEXT,
    job_schedule_type     TEXT,
    job_work_from_home    BOOLEAN,
    search_location       TEXT,
    job_posted_date       TIMESTAMP,           -- el origen no trae zona horaria
    job_no_degree_mention BOOLEAN,
    job_health_insurance  BOOLEAN,
    job_country           TEXT,
    salary_rate           TEXT,
    salary_year_avg       NUMERIC,             -- sin escala: se guarda el valor tal cual
    salary_hour_avg       NUMERIC,
    company_name          TEXT,
    job_skills            TEXT[],
    job_type_skills       JSONB,
    source_file           TEXT        NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
