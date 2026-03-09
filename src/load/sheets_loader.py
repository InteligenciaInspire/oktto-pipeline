from __future__ import annotations

import pandas as pd

from src.clients.sheets_client import SheetsClient


def load_dataframe_to_sheet(client: SheetsClient, worksheet_name: str, dataframe: pd.DataFrame) -> None:
    client.upsert_dataframe(worksheet_name, dataframe)
