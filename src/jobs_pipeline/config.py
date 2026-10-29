"""Configuración leída de variables de entorno (o de un archivo .env en local)."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    csv_path: Path

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno {name} (ver .env.example)")
    return value


def load_settings() -> Settings:
    # override=False: si la variable ya existe en el entorno (Docker, CI), gana esa.
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    return Settings(
        db_host=os.getenv("POSTGRES_HOST", "localhost"),
        db_port=int(os.getenv("POSTGRES_PORT", "5432")),
        db_name=_required("POSTGRES_DB"),
        db_user=_required("POSTGRES_USER"),
        db_password=_required("POSTGRES_PASSWORD"),
        csv_path=Path(os.getenv("CSV_PATH", PROJECT_ROOT / "data" / "data_jobs.csv")),
    )
