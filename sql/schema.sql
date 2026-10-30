-- Modelo relacional en 3FN (schema core).
-- Cada hecho se guarda en un solo lugar. Los comentarios de cada tabla dicen
-- por qué existe; el razonamiento completo, con los números, está en el README.

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- Geografía
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.countries (
    country_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

-- job_country depende de la búsqueda que hizo el bot, no de job_location
-- (search_location -> job_country se cumple en el 100% de las filas;
-- job_location -> job_country falla en 1.166 ubicaciones). Por eso el país
-- cuelga de aquí y no de core.locations.
CREATE TABLE IF NOT EXISTS core.search_locations (
    search_location_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    search_location_name TEXT     NOT NULL UNIQUE,
    country_id           SMALLINT NOT NULL REFERENCES core.countries (country_id)
);

-- Texto de ubicación que muestra la oferta ("Bogotá, Colombia", "Anywhere").
CREATE TABLE IF NOT EXISTS core.locations (
    location_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_name TEXT NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------
-- Empresas y plataformas
-- ---------------------------------------------------------------------------

-- company_key es el nombre normalizado (minúsculas, espacios colapsados) y es
-- lo que define la identidad de la empresa. company_name es la variante más
-- frecuente en el origen, para mostrar.
CREATE TABLE IF NOT EXISTS core.companies (
    company_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_key  TEXT NOT NULL UNIQUE,
    company_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.platforms (
    platform_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    platform_name TEXT NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------
-- Títulos y roles
-- ---------------------------------------------------------------------------

-- Equivale a job_title_short (Data Engineer, Data Analyst, ...).
CREATE TABLE IF NOT EXISTS core.job_roles (
    role_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    role_name TEXT NOT NULL UNIQUE
);

-- job_title_short se calcula a partir de job_title (diccionario de datos) y en
-- los datos cada título tiene exactamente un rol. Si el rol quedara en jobs
-- habría una dependencia transitiva job -> job_title -> rol.
CREATE TABLE IF NOT EXISTS core.job_titles (
    job_title_id INTEGER  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_title    TEXT     NOT NULL UNIQUE,
    role_id      SMALLINT NOT NULL REFERENCES core.job_roles (role_id)
);

-- ---------------------------------------------------------------------------
-- Catálogos de jornada y skills
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.schedule_types (
    schedule_type_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_type_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.skill_types (
    skill_type_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    skill_type_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.skills (
    skill_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    skill_name TEXT NOT NULL UNIQUE
);

-- La clasificación de un skill es propiedad del skill, no de la oferta: sas,
-- mongodb, ruby y firebase aparecen siempre en dos tipos a la vez. Por eso es
-- una relación N:M propia y no una columna skill_type_id en core.skills.
CREATE TABLE IF NOT EXISTS core.skill_type_assignments (
    skill_id      INTEGER  NOT NULL REFERENCES core.skills (skill_id),
    skill_type_id SMALLINT NOT NULL REFERENCES core.skill_types (skill_type_id),
    PRIMARY KEY (skill_id, skill_type_id)
);

-- ---------------------------------------------------------------------------
-- Ofertas
-- ---------------------------------------------------------------------------

-- El origen no trae un id de oferta y hay ~36 mil filas que solo difieren en la
-- fecha (republicaciones), así que no existe una clave natural confiable.
-- job_id es sustituta; source_row_id mantiene el linaje con raw.
CREATE TABLE IF NOT EXISTS core.jobs (
    job_id             BIGINT    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id      BIGINT    NOT NULL UNIQUE,
    job_title_id       INTEGER   NOT NULL REFERENCES core.job_titles (job_title_id),
    company_id         INTEGER            REFERENCES core.companies (company_id),
    location_id        INTEGER            REFERENCES core.locations (location_id),
    search_location_id SMALLINT  NOT NULL REFERENCES core.search_locations (search_location_id),
    platform_id        SMALLINT           REFERENCES core.platforms (platform_id),
    posted_at          TIMESTAMP NOT NULL,
    work_from_home     BOOLEAN   NOT NULL,
    no_degree_mention  BOOLEAN   NOT NULL,
    health_insurance   BOOLEAN   NOT NULL
);

-- Postgres no indexa las FK automáticamente; sin estos índices los joins y los
-- borrados en cascada recorren toda la tabla.
CREATE INDEX IF NOT EXISTS ix_jobs_job_title       ON core.jobs (job_title_id);
CREATE INDEX IF NOT EXISTS ix_jobs_company         ON core.jobs (company_id);
CREATE INDEX IF NOT EXISTS ix_jobs_location        ON core.jobs (location_id);
CREATE INDEX IF NOT EXISTS ix_jobs_search_location ON core.jobs (search_location_id);
CREATE INDEX IF NOT EXISTS ix_jobs_platform        ON core.jobs (platform_id);
CREATE INDEX IF NOT EXISTS ix_jobs_posted_at       ON core.jobs (posted_at);

-- job_schedule_type venía como texto compuesto ("Full-time and Part-time"),
-- lo cual rompe 1FN. Se resuelve como N:M.
CREATE TABLE IF NOT EXISTS core.job_schedule_types (
    job_id           BIGINT   NOT NULL REFERENCES core.jobs (job_id) ON DELETE CASCADE,
    schedule_type_id SMALLINT NOT NULL REFERENCES core.schedule_types (schedule_type_id),
    PRIMARY KEY (job_id, schedule_type_id)
);
CREATE INDEX IF NOT EXISTS ix_job_schedule_types_type ON core.job_schedule_types (schedule_type_id);

-- Relación N:M ofertas <-> skills. job_type_skills no se guarda: se reconstruye
-- con esta tabla + core.skill_type_assignments.
CREATE TABLE IF NOT EXISTS core.job_skills (
    job_id   BIGINT  NOT NULL REFERENCES core.jobs (job_id) ON DELETE CASCADE,
    skill_id INTEGER NOT NULL REFERENCES core.skills (skill_id),
    PRIMARY KEY (job_id, skill_id)
);
CREATE INDEX IF NOT EXISTS ix_job_skills_skill ON core.job_skills (skill_id);

-- Solo ~4% de las ofertas publica salario. En una tabla 1:0..1 evitamos tres
-- columnas casi siempre nulas en core.jobs. Los CHECK fijan la regla del origen:
-- el promedio anual solo existe con tarifa anual y el horario con tarifa horaria.
CREATE TABLE IF NOT EXISTS core.job_salaries (
    job_id          BIGINT PRIMARY KEY REFERENCES core.jobs (job_id) ON DELETE CASCADE,
    salary_rate     TEXT   NOT NULL
        CHECK (salary_rate IN ('year', 'month', 'week', 'day', 'hour')),
    salary_year_avg NUMERIC(12, 2) CHECK (salary_year_avg > 0),
    salary_hour_avg NUMERIC(10, 2) CHECK (salary_hour_avg > 0),
    CHECK (salary_year_avg IS NULL OR salary_rate = 'year'),
    CHECK (salary_hour_avg IS NULL OR salary_rate = 'hour')
);
