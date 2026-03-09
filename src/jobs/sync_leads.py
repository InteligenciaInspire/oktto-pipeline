from __future__ import annotations

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClient
from src.extract.leads import fetch_leads
from src.load.sheets_loader import load_dataframe_to_sheet
from src.transform.normalize_leads import normalize_leads
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run(oktto_client: OkttoClient, sheets_client: SheetsClient) -> None:
    logger.info("Sync leads started")
    leads = fetch_leads(oktto_client)
    leads_df = normalize_leads(leads)

    load_dataframe_to_sheet(sheets_client, "raw_leads", leads_df)
    load_dataframe_to_sheet(sheets_client, "leads_tratados", leads_df)

    logger.info("Sync leads finished")
