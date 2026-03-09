from __future__ import annotations

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClient
from src.extract.sales import fetch_sales
from src.load.sheets_loader import load_dataframe_to_sheet
from src.transform.normalize_sales import normalize_sales
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run(oktto_client: OkttoClient, sheets_client: SheetsClient) -> None:
    logger.info("Sync sales started")
    sales = fetch_sales(oktto_client)
    sales_df = normalize_sales(sales)

    load_dataframe_to_sheet(sheets_client, "raw_sales", sales_df)
    load_dataframe_to_sheet(sheets_client, "sales_tratadas", sales_df)

    logger.info("Sync sales finished")
