from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class OkttoSettings:
    base_url: str = os.getenv("OKTTO_API_BASE_URL", "https://api.oktto.com.br/v1")
    token: str = os.getenv("OKTTO_API_TOKEN", "")
    timeout_seconds: int = int(os.getenv("OKTTO_TIMEOUT_SECONDS", "30"))
    max_retries: int = int(os.getenv("OKTTO_MAX_RETRIES", "3"))
    backoff_factor: float = float(os.getenv("OKTTO_BACKOFF_FACTOR", "0.5"))
    page_size: int = int(os.getenv("OKTTO_PAGE_SIZE", "100"))


@dataclass(frozen=True)
class SheetsSettings:
    spreadsheet_id: str = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")
    credentials_json: str = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "")


@dataclass(frozen=True)
class AppSettings:
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    oktto: OkttoSettings = OkttoSettings()
    sheets: SheetsSettings = SheetsSettings()


def get_settings() -> AppSettings:
    return AppSettings()
