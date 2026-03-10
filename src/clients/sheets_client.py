from __future__ import annotations

from typing import Any, List

import pandas as pd

try:
    import gspread
except ImportError:
    gspread = None

try:
    from google.oauth2.credentials import Credentials as UserCredentials
    from google.oauth2.service_account import Credentials
except ImportError:
    UserCredentials = Any
    Credentials = None

from src.config import SheetsSettings
from src.utils.logger import get_logger


def _ensure_google_dependencies() -> None:
    if gspread is None or Credentials is None:
        raise RuntimeError(
            "Dependencias Google indisponiveis no ambiente. "
            "Verifique a instalacao de gspread e google-auth no deploy."
        )


class SheetsClient:
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(self, settings: SheetsSettings) -> None:
        self.logger = get_logger(self.__class__.__name__)
        _ensure_google_dependencies()
        credentials = Credentials.from_service_account_file(
            settings.credentials_json,
            scopes=self.SCOPES,
        )
        self.gc = gspread.authorize(credentials)
        self.spreadsheet = self.gc.open_by_key(settings.spreadsheet_id)

    def upsert_dataframe(self, worksheet_name: str, dataframe: pd.DataFrame) -> None:
        worksheet = self._get_or_create_worksheet(worksheet_name)
        rows: List[List[str]] = [dataframe.columns.tolist()] + dataframe.fillna("").astype(str).values.tolist()
        worksheet.clear()
        worksheet.update(rows)
        self.logger.info("Worksheet %s updated with %d rows", worksheet_name, len(dataframe))

    def _get_or_create_worksheet(self, worksheet_name: str):
        try:
            return self.spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            self.logger.info("Worksheet %s not found. Creating new one.", worksheet_name)
            return self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)


class SheetsClientOAuth:
    def __init__(self, spreadsheet_id: str, credentials: UserCredentials) -> None:
        self.logger = get_logger(self.__class__.__name__)
        _ensure_google_dependencies()
        self.gc = gspread.authorize(credentials)
        self.spreadsheet = self.gc.open_by_key(spreadsheet_id)

    def upsert_dataframe(self, worksheet_name: str, dataframe: pd.DataFrame) -> None:
        worksheet = self._get_or_create_worksheet(worksheet_name)
        rows: List[List[str]] = [dataframe.columns.tolist()] + dataframe.fillna("").astype(str).values.tolist()
        worksheet.clear()
        worksheet.update(rows)
        self.logger.info("Worksheet %s updated with %d rows", worksheet_name, len(dataframe))

    def _get_or_create_worksheet(self, worksheet_name: str):
        try:
            return self.spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            self.logger.info("Worksheet %s not found. Creating new one.", worksheet_name)
            return self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
