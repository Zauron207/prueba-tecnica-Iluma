import pandas as pd
import pytest

from jobs_pipeline.extract import (
    parse_bool,
    parse_skill_dict,
    parse_skill_list,
    read_source_csv,
)


class TestParseBool:
    def test_valores_validos(self):
        assert parse_bool("True") is True
        assert parse_bool("False") is False

    @pytest.mark.parametrize("value", ["true", "1", "", "yes"])
    def test_rechaza_lo_que_no_viene_del_origen(self, value):
        with pytest.raises(ValueError):
            parse_bool(value)


class TestParseSkills:
    def test_lista_con_comillas_simples(self):
        assert parse_skill_list("['python', 'sql']") == ["python", "sql"]

    def test_skill_con_caracteres_especiales(self):
        assert parse_skill_list("['c#', 'c++', 'power bi']") == ["c#", "c++", "power bi"]

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_vacio_es_none(self, value):
        assert parse_skill_list(value) is None

    def test_rechaza_algo_que_no_es_lista(self):
        with pytest.raises(ValueError):
            parse_skill_list("{'a': 1}")

    def test_no_ejecuta_codigo(self):
        # literal_eval solo evalúa literales; una llamada a función es un error.
        with pytest.raises(ValueError):
            parse_skill_list("__import__('os').getcwd()")

    def test_dict_de_tipos(self):
        value = "{'cloud': ['aws'], 'programming': ['python', 'sql']}"
        assert parse_skill_dict(value) == {"cloud": ["aws"], "programming": ["python", "sql"]}

    def test_rechaza_dict_mal_formado(self):
        with pytest.raises(ValueError):
            parse_skill_dict("{'cloud': 'aws'}")


class TestReadSourceCsv:
    def test_tipos_de_columnas(self, sample_csv):
        df = read_source_csv(sample_csv)

        assert len(df) == 8
        assert df["source_row_id"].tolist() == list(range(1, 9))
        assert pd.api.types.is_datetime64_any_dtype(df["job_posted_date"])
        assert df["job_work_from_home"].tolist()[3] is True
        assert df.loc[0, "job_skills"] == ["python", "sql", "sas", "sas"]
        assert df.loc[0, "job_type_skills"]["analyst_tools"] == ["sas"]
        assert df["source_file"].unique().tolist() == ["sample_jobs.csv"]

    def test_vacios_quedan_como_nulos(self, sample_csv):
        df = read_source_csv(sample_csv)
        sin_skills = df.loc[4]

        assert sin_skills["job_skills"] is None
        assert sin_skills["job_type_skills"] is None
        assert df.loc[3, "job_country"] is None
        assert pd.isna(df.loc[0, "salary_hour_avg"])

    def test_falla_si_falta_una_columna(self, tmp_path):
        path = tmp_path / "incompleto.csv"
        path.write_text("job_title,job_location\nx,y\n", encoding="utf-8")
        with pytest.raises(ValueError, match="faltan columnas"):
            read_source_csv(path)
