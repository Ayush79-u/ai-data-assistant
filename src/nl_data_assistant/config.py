"""
config.py — centralised settings loaded from .env.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

# Load .env
load_dotenv()

_REQUIRED_MYSQL = ["MYSQL_HOST", "MYSQL_USER"]


def validate_config() -> None:
    """Raise EnvironmentError at startup if critical keys are missing."""
    missing = [k for k in _REQUIRED_MYSQL if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required .env variables: {missing}\n"
            "Copy .env.example to .env and fill in your MySQL credentials."
        )


@dataclass(frozen=True)
class Settings:
    # 🔥 Use 127.0.0.1 instead of localhost (more reliable)
    mysql_host: str = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_user: str = os.getenv("MYSQL_USER", "root")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "")
    mysql_database: str = os.getenv("MYSQL_DATABASE", "ai_data_assistant")

    default_target: str = os.getenv("DEFAULT_TARGET", "mysql")
    default_workbook: Path = Path(
        os.getenv("DEFAULT_WORKBOOK", "data/workbook.xlsx")
    )

    @property
    def default_database(self) -> str:
        return self.mysql_database.strip()

    @property
    def mysql_server_url(self) -> str:
        """
        Build safe MySQL connection URL.
        Handles special characters in password (like @, :, /, etc.)
        """
        encoded_password = quote_plus(self.mysql_password)  # 🔥 CRITICAL FIX

        return (
            f"mysql+pymysql://{self.mysql_user}:{encoded_password}"
            f"@{self.mysql_host}:{self.mysql_port}"
            "?charset=utf8mb4"
        )

    def mysql_url_for(self, database: str | None = None) -> str:
        target_database = (database if database is not None else self.default_database).strip()
        base = self.mysql_server_url.removesuffix("?charset=utf8mb4")
        if target_database:
            return f"{base}/{target_database}?charset=utf8mb4"
        return self.mysql_server_url

    @property
    def mysql_url(self) -> str:
        return self.mysql_url_for()


# Singleton settings object
settings = Settings()
