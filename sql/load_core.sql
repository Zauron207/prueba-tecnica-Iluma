-- Carga staging -> core (full refresh).
-- Se ejecuta dentro de una sola transacción desde Python: si algo falla, core
-- queda exactamente como estaba antes de la corrida. Python quita las FK antes
-- de este script y las recrea (validándolas) después; ver load_core.py.
--
-- Orden: primero catálogos, luego jobs, luego tablas puente. Las claves se
-- resuelven por la clave natural de cada catálogo (nombre), así que los UNIQUE
-- del modelo también actúan como validación: si un título apareciera con dos
-- roles, el INSERT en core.job_titles falla en vez de cargar datos ambiguos.

TRUNCATE
    core.job_salaries,
    core.job_skills,
    core.job_schedule_types,
    core.jobs,
    core.skill_type_assignments,
    core.skills,
    core.skill_types,
    core.schedule_types,
    core.job_titles,
    core.job_roles,
    core.platforms,
    core.companies,
    core.locations,
    core.search_locations,
    core.countries
RESTART IDENTITY;

-- Catálogos ------------------------------------------------------------------

INSERT INTO core.countries (country_name)
SELECT DISTINCT country_name
FROM staging.job_postings
ORDER BY country_name;

INSERT INTO core.search_locations (search_location_name, country_id)
SELECT DISTINCT s.search_location_name, c.country_id
FROM staging.job_postings s
JOIN core.countries c ON c.country_name = s.country_name
ORDER BY s.search_location_name;

INSERT INTO core.locations (location_name)
SELECT DISTINCT location_name
FROM staging.job_postings
WHERE location_name IS NOT NULL
ORDER BY location_name;

INSERT INTO core.companies (company_key, company_name)
SELECT DISTINCT company_key, company_name
FROM staging.job_postings
WHERE company_key IS NOT NULL
ORDER BY company_key;

INSERT INTO core.platforms (platform_name)
SELECT DISTINCT platform_name
FROM staging.job_postings
WHERE platform_name IS NOT NULL
ORDER BY platform_name;

INSERT INTO core.job_roles (role_name)
SELECT DISTINCT role_name
FROM staging.job_postings
ORDER BY role_name;

INSERT INTO core.job_titles (job_title, role_id)
SELECT DISTINCT s.job_title, r.role_id
FROM staging.job_postings s
JOIN core.job_roles r ON r.role_name = s.role_name
ORDER BY s.job_title;

INSERT INTO core.schedule_types (schedule_type_name)
SELECT DISTINCT u.schedule_type_name
FROM staging.job_postings s
CROSS JOIN LATERAL unnest(s.schedule_types) AS u (schedule_type_name)
ORDER BY u.schedule_type_name;

INSERT INTO core.skill_types (skill_type_name)
SELECT DISTINCT t.skill_type_name
FROM staging.job_postings s
CROSS JOIN LATERAL jsonb_object_keys(s.job_type_skills) AS t (skill_type_name)
ORDER BY t.skill_type_name;

INSERT INTO core.skills (skill_name)
SELECT DISTINCT u.skill_name
FROM staging.job_postings s
CROSS JOIN LATERAL unnest(s.skills) AS u (skill_name)
ORDER BY u.skill_name;

INSERT INTO core.skill_type_assignments (skill_id, skill_type_id)
SELECT DISTINCT sk.skill_id, st.skill_type_id
FROM staging.job_postings s
CROSS JOIN LATERAL jsonb_each(s.job_type_skills) AS t (skill_type_name, skill_list)
CROSS JOIN LATERAL jsonb_array_elements_text(t.skill_list) AS e (skill_name)
JOIN core.skills sk      ON sk.skill_name = e.skill_name
JOIN core.skill_types st ON st.skill_type_name = t.skill_type_name;

-- Las tablas recién cargadas no tienen estadísticas; sin ellas el planner arma
-- los joins siguientes a ciegas.
ANALYZE core.job_titles, core.companies, core.locations, core.platforms,
        core.search_locations, core.skills, core.schedule_types;

-- Ofertas --------------------------------------------------------------------

INSERT INTO core.jobs (
    source_row_id, job_title_id, company_id, location_id, search_location_id,
    platform_id, posted_at, work_from_home, no_degree_mention, health_insurance
)
SELECT
    s.source_row_id,
    jt.job_title_id,
    co.company_id,
    l.location_id,
    sl.search_location_id,
    p.platform_id,
    s.posted_at,
    s.work_from_home,
    s.no_degree_mention,
    s.health_insurance
FROM staging.job_postings s
JOIN core.job_titles jt            ON jt.job_title = s.job_title
JOIN core.search_locations sl      ON sl.search_location_name = s.search_location_name
LEFT JOIN core.companies co        ON co.company_key = s.company_key
LEFT JOIN core.locations l         ON l.location_name = s.location_name
LEFT JOIN core.platforms p         ON p.platform_name = s.platform_name
ORDER BY s.source_row_id;

ANALYZE core.jobs;

-- Tablas puente y salario ------------------------------------------------------
-- Sin DISTINCT a propósito: staging ya viene deduplicado y si no lo estuviera
-- la PK compuesta lo detecta.

INSERT INTO core.job_schedule_types (job_id, schedule_type_id)
SELECT j.job_id, st.schedule_type_id
FROM staging.job_postings s
JOIN core.jobs j ON j.source_row_id = s.source_row_id
CROSS JOIN LATERAL unnest(s.schedule_types) AS u (schedule_type_name)
JOIN core.schedule_types st ON st.schedule_type_name = u.schedule_type_name;

INSERT INTO core.job_skills (job_id, skill_id)
SELECT j.job_id, sk.skill_id
FROM staging.job_postings s
JOIN core.jobs j ON j.source_row_id = s.source_row_id
CROSS JOIN LATERAL unnest(s.skills) AS u (skill_name)
JOIN core.skills sk ON sk.skill_name = u.skill_name;

INSERT INTO core.job_salaries (job_id, salary_rate, salary_year_avg, salary_hour_avg)
SELECT j.job_id, s.salary_rate, s.salary_year_avg, s.salary_hour_avg
FROM staging.job_postings s
JOIN core.jobs j ON j.source_row_id = s.source_row_id
WHERE s.salary_rate IS NOT NULL;
