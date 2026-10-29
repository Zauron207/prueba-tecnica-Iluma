import pandas as pd
import pytest

from jobs_pipeline import cleaning


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("  Data   Engineer ", "Data Engineer"),
        ("", None),
        ("   ", None),
        (None, None),
        (float("nan"), None),
    ],
)
def test_normalize_text(value, expected):
    assert cleaning.normalize_text(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("via LinkedIn", "LinkedIn"),
        ("melalui LinkedIn", "LinkedIn"),
        ("Via  Indeed", "Indeed"),
        ("Trabajo.org - Vacantes De Empleo, Trabajo", "Trabajo.org - Vacantes De Empleo, Trabajo"),
        ("via ", None),
        (None, None),
    ],
)
def test_clean_platform(value, expected):
    assert cleaning.clean_platform(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Colorado Springs, CO   (+3 others)", "Colorado Springs, CO"),
        ("Egypt  (+1 other)", "Egypt"),
        ("Anywhere", "Anywhere"),
        ("New York, NY", "New York, NY"),
        (None, None),
    ],
)
def test_clean_location(value, expected):
    assert cleaning.clean_location(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Full-time", ["Full-time"]),
        ("Full-time and Part-time", ["Full-time", "Part-time"]),
        ("Full-time, Part-time, and Internship", ["Full-time", "Part-time", "Internship"]),
        ("Temp work and Internship", ["Temp work", "Internship"]),
        ("Pekerjaan tetap", ["Full-time"]),
        ("Kontraktor", ["Contractor"]),
        ("Part-time and Full-time", ["Part-time", "Full-time"]),
        ("Full-time and Pekerjaan tetap", ["Full-time"]),
        ("Algo Nuevo", ["Algo Nuevo"]),
        ("", []),
        (None, []),
    ],
)
def test_split_schedule_types(value, expected):
    assert cleaning.split_schedule_types(value) == expected


def test_company_key_unifica_mayusculas_y_espacios():
    variantes = ["BNP Paribas", "BNP PARIBAS", "bnp  paribas", " Bnp Paribas "]
    assert {cleaning.company_key(v) for v in variantes} == {"bnp paribas"}


def test_company_key_no_fusiona_razones_sociales_distintas():
    assert cleaning.company_key("1 Point System") != cleaning.company_key("1 Point System LLC")


def test_canonical_names_elige_la_variante_mas_frecuente():
    names = pd.Series(["Capgemini", "Capgemini", "CAPGEMINI", "capgemini", "Acme", None])
    keys = names.map(cleaning.company_key)

    result = cleaning.canonical_names(keys, names)

    assert result.tolist()[:5] == ["Capgemini"] * 4 + ["Acme"]
    assert pd.isna(result.iloc[5])


def test_canonical_names_desempata_de_forma_deterministica():
    names = pd.Series(["Emids", "eMids"])
    keys = names.map(cleaning.company_key)
    assert cleaning.canonical_names(keys, names).unique().tolist() == ["Emids"]


class TestSkills:
    def test_quita_repetidos_y_conserva_orden(self):
        result = cleaning.normalize_skills(["python", "sas", "SQL ", "sas"])
        assert result == ["python", "sas", "sql"]

    def test_vacio(self):
        assert cleaning.normalize_skills(None) == []
        assert cleaning.normalize_skills([]) == []

    def test_normaliza_dict_de_tipos(self):
        result = cleaning.normalize_skill_types(
            {"Programming": ["Python", "python"], "cloud": [], " programming ": ["sql"]}
        )
        assert result == {"programming": ["python", "sql"]}


@pytest.mark.parametrize(
    ("job_country", "search_location", "expected"),
    [
        ("Colombia", "Colombia", "Colombia"),
        # Georgia la búsqueda, Estados Unidos el país: se respeta el dato de origen.
        ("United States", "Georgia", "United States"),
        (None, "Sierra Leone", "Sierra Leone"),
        ("", "Texas, United States", "United States"),
        (None, None, None),
    ],
)
def test_resolve_country(job_country, search_location, expected):
    assert cleaning.resolve_country(job_country, search_location) == expected
