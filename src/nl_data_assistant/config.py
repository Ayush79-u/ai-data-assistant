from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class AppConfig:
    base_dir: Path
    data_dir: Path
    upload_dir: Path
    output_dir: Path
    default_workbook: Path
    default_target: str
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    openai_api_key: str
    openai_model: str

    @classmethod
    def from_env(cls, base_dir: str | Path | None = None) -> "AppConfig":
        resolved_base = Path(base_dir or Path.cwd()).resolve()
        data_dir = resolved_base / "data"
        upload_dir = data_dir / "uploads"
        output_dir = resolved_base / "outputs"
        default_workbook = resolved_base / os.getenv("DEFAULT_WORKBOOK", "data/workbook.xlsx")

        config = cls(
            base_dir=resolved_base,
            data_dir=data_dir,
            upload_dir=upload_dir,
            output_dir=output_dir,
            default_workbook=default_workbook,
            default_target=os.getenv("DEFAULT_TARGET", "mysql").strip().lower() or "mysql",
            mysql_host=os.getenv("MYSQL_HOST", "localhost").strip(),
            mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
            mysql_user=os.getenv("MYSQL_USER", "").strip(),
            mysql_password=os.getenv("MYSQL_PASSWORD", ""),
            mysql_database=os.getenv("MYSQL_DATABASE", "").strip(),
            openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            openai_model=os.getenv("OPENAI_MODEL", "").strip(),
        )
        config.ensure_directories()
        return config

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.default_workbook.parent.mkdir(parents=True, exist_ok=True)

    @property
    def mysql_url(self) -> str | None:
        if not (self.mysql_user and self.mysql_database):
            return None
        password = quote_plus(self.mysql_password)
        return (
            f"mysql+pymysql://{self.mysql_user}:{password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )

    @property
    def llm_enabled(self) -> bool:
        return bool(self.openai_api_key and self.openai_model)

