"""Reglas de limpieza y estandarización.

Todas son funciones puras (entrada -> salida, sin tocar la base) para poder
probarlas aisladas. Cada regla salió de algo que se vio al perfilar los datos;
el README documenta los números.
"""

import re

import pandas as pd

_WHITESPACE = re.compile(r"\s+")
# "Colorado Springs, CO (+3 others)": la oferta lista varias ciudades y el
# scraper solo guarda la primera más un contador.
_OTHERS_SUFFIX = re.compile(r"\s*\(\+\d+ others?\)$")
# "via LinkedIn" y, en ofertas de Indonesia, "melalui LinkedIn".
_PLATFORM_PREFIX = re.compile(r"^(via|melalui)(\s+|$)", re.IGNORECASE)
# "Full-time, Part-time, and Internship" / "Full-time and Part-time"
_SCHEDULE_SEPARATOR = re.compile(r",\s*and\s+|\s+and\s+|,\s*")

SCHEDULE_TYPES = {
    "full-time": "Full-time",
    "part-time": "Part-time",
    "contractor": "Contractor",
    "temp work": "Temp work",
    "internship": "Internship",
    "per diem": "Per diem",
    "volunteer": "Volunteer",
    # Etiquetas en indonesio que Google Jobs usa para las mismas categorías.
    "pekerjaan tetap": "Full-time",
    "kontraktor": "Contractor",
}


def normalize_text(value: str | None) -> str | None:
    """Recorta y colapsa espacios. Un texto vacío se trata como ausente."""
    if value is None or pd.isna(value):
        return None
    cleaned = _WHITESPACE.sub(" ", str(value)).strip()
    return cleaned or None


def clean_platform(job_via: str | None) -> str | None:
    text = normalize_text(job_via)
    if text is None:
        return None
    return _PLATFORM_PREFIX.sub("", text) or None


def clean_location(job_location: str | None) -> str | None:
    text = normalize_text(job_location)
    if text is None:
        return None
    return _OTHERS_SUFFIX.sub("", text) or None


def split_schedule_types(value: str | None) -> list[str]:
    """Separa el texto compuesto en tipos de jornada canónicos, sin repetir.

    Un valor desconocido se conserva tal cual en vez de descartarse: preferimos
    que aparezca como categoría nueva a perder el dato.
    """
    text = normalize_text(value)
    if text is None:
        return []
    result: list[str] = []
    for part in _SCHEDULE_SEPARATOR.split(text):
        part = part.strip()
        if not part:
            continue
        canonical = SCHEDULE_TYPES.get(part.lower(), part)
        if canonical not in result:
            result.append(canonical)
    return result


def company_key(name: str | None) -> str | None:
    """Clave de identidad de una empresa: 'BNP PARIBAS' y 'Bnp Paribas' son la misma.

    Solo unifica mayúsculas y espacios. No se quitan sufijos legales (Inc, SA,
    LLC) ni se hace fuzzy matching: el riesgo de fusionar empresas distintas es
    mayor que el beneficio, y un falso merge en la fuente de verdad es difícil
    de detectar después.
    """
    text = normalize_text(name)
    return text.casefold() if text else None


def canonical_names(keys: pd.Series, names: pd.Series) -> pd.Series:
    """Para cada clave devuelve el nombre más frecuente entre sus variantes.

    Empates: orden alfabético, para que el resultado sea determinístico entre
    corridas.
    """
    pairs = pd.DataFrame({"key": keys, "name": names}).dropna()
    counts = pairs.value_counts().reset_index(name="n")
    best = counts.sort_values(["key", "n", "name"], ascending=[True, False, True]).drop_duplicates(
        "key"
    )
    mapping = dict(zip(best["key"], best["name"], strict=True))
    return keys.map(mapping)


def normalize_skills(skills: list[str] | None) -> list[str]:
    """Minúsculas, sin espacios sobrantes y sin repetidos (conserva el orden).

    Los repetidos son reales en el origen: 'sas' o 'mongodb' aparecen dos veces
    en la lista porque el extractor los clasifica en dos tipos a la vez.
    """
    if not skills:
        return []
    result: list[str] = []
    for skill in skills:
        name = normalize_text(skill)
        if name is None:
            continue
        name = name.lower()
        if name not in result:
            result.append(name)
    return result


def normalize_skill_types(types: dict[str, list[str]] | None) -> dict[str, list[str]]:
    if not types:
        return {}
    result: dict[str, list[str]] = {}
    for skill_type, skills in types.items():
        key = normalize_text(skill_type)
        if key is None:
            continue
        key = key.lower()
        merged = normalize_skills(result.get(key, []) + list(skills or []))
        if merged:
            result[key] = merged
    return result


def resolve_country(job_country: str | None, search_location: str | None) -> str | None:
    """País de la oferta; si viene vacío se toma de la búsqueda que la originó.

    Es seguro porque search_location -> job_country se cumple en todas las
    filas con país, y las 49 filas sin país vienen de búsquedas a nivel país
    ("Gabon", "Sierra Leone"), donde el último segmento es el país.
    """
    country = normalize_text(job_country)
    if country:
        return country
    search = normalize_text(search_location)
    if search is None:
        return None
    return search.split(",")[-1].strip() or None
